// src/routes/v1/drive/contacts/handlers.ts
import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import {
  Contact,
  ContactFE,
  ApiResponse,
  IRequestCreateContact,
  IRequestDeleteContact,
  IRequestGetContact,
  IRequestListContacts,
  IRequestRedeemContact,
  IRequestUpdateContact,
  IResponseDeleteContact,
  IResponseListContacts,
  IResponseRedeemContact,
  UserID,
  IDPrefixEnum,
  ApiKeyValue,
  SortDirection,
  ContactGroupPreview,
  SystemPermissionType,
  ApiKeyID,
  ApiKey,
  SystemResourceID,
  SystemTableValueEnum,
  GranteeID,
  GroupID, // Added GroupID import
  GroupInviteID, // Added GroupInviteID import
  GroupRole, // Added GroupRole import
  GroupInviteeID, // Added GroupInviteeID import
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import {
  authenticateRequest,
  generateApiKey,
  seed_phrase_to_wallet_addresses,
} from "../../../../services/auth";
import {
  validateEmail,
  validateEvmAddress,
  validateExternalId,
  validateExternalPayload,
  validateIdString,
  validateUrl,
  validateUserId,
} from "../../../../services/validation";
import { createApiResponse, getDriveOwnerId, OrgIdParams } from "../../types";
import {
  checkSystemPermissions,
  checkPermissionsTableAccess,
  mapDbRowToSystemPermission,
} from "../../../../services/permissions/system";
import { PUBLIC_GRANTEE_ID_STRING } from "../../../../services/permissions/directory";
import {
  getGroupById,
  isUserOnLocalGroup, // Added for local group membership check
  getGroupInviteById, // Added for fetching group invite details
} from "../../../../services/groups"; // Import group services

// Type definitions for route params
interface GetContactParams extends OrgIdParams {
  contact_id: UserID;
}

// --- Utility Functions (Mimicking Rust's `Contact::redacted` and permission checks) ---

async function redactContact(
  contact: Contact,
  requesterUserId: UserID,
  orgId: string
): Promise<ContactFE> {
  const redactedContact = { ...contact } as ContactFE;

  const ownerId = await getDriveOwnerId(orgId);
  const isOwner = requesterUserId === ownerId;

  // Derive permission_previews based on system permissions for this specific contact record
  // and for the overall 'contacts' table.
  const plainContactId = contact.id;
  const recordResourceId: SystemResourceID =
    `${plainContactId}` as SystemResourceID;
  const tableResourceId: SystemResourceID =
    `TABLE_${SystemTableValueEnum.CONTACTS}` as SystemResourceID;

  // Retrieve permissions for the specific contact record
  const recordPermissions = await checkSystemPermissions(
    recordResourceId,
    requesterUserId, // Check permissions for the requester
    orgId
  );
  // Retrieve permissions for the 'CONTACTS' table
  const tablePermissions = await checkSystemPermissions(
    tableResourceId,
    requesterUserId, // Check permissions for the requester
    orgId
  );

  const permissionPreviews = Array.from(
    new Set([...recordPermissions, ...tablePermissions])
  );
  redactedContact.permission_previews = permissionPreviews;

  redactedContact.labels = contact.labels;

  // Fetch real group data and filter based on permissions.
  // We need to query the `` join table to get group IDs associated with this contact.
  const plainContactIdForGroups = contact.id;
  const contactGroupsQuery = `
    SELECT group_id FROM group_invites WHERE invitee_id = ? AND invitee_type = 'USER';
  `;
  const contactGroupRows: { group_id: string }[] = await db.queryDrive(
    orgId,
    contactGroupsQuery,
    [plainContactIdForGroups]
  );

  const groupPreviews: ContactGroupPreview[] = [];
  for (const row of contactGroupRows) {
    const groupId: GroupID = `${row.group_id}` as GroupID;
    const group = await getGroupById(groupId, orgId);

    if (group) {
      // Find the specific invite for this user within this group
      const memberInviteQuery = `
        SELECT id, role FROM group_invites
        WHERE group_id = ? AND invitee_type = 'USER' AND invitee_id = ?;
      `;
      const memberInviteRows: { id: string; role: string }[] =
        await db.queryDrive(orgId, memberInviteQuery, [
          group.id,
          plainContactIdForGroups,
        ]);

      let is_admin = false;
      let invite_id: GroupInviteID | undefined;

      if (memberInviteRows.length > 0) {
        const inviteRow = memberInviteRows[0];
        is_admin = inviteRow.role === GroupRole.ADMIN;
        invite_id = `${inviteRow.id}` as GroupInviteID;
      } else {
        // If no direct invite found, check if the user is the owner of the group
        if (group.owner === contact.id) {
          is_admin = true;
          // For owner, there might not be an explicit invite record, generate a placeholder invite ID
          invite_id = `${IDPrefixEnum.GroupInvite}OWNER_DEFAULT`; // Placeholder
        }
      }

      groupPreviews.push({
        group_id: group.id,
        invite_id: invite_id || ("" as GroupInviteID), // Provide a default empty string or handle undefined in FE
        is_admin,
        group_name: group.name,
        group_avatar: group.avatar,
      });
    }
  }
  redactedContact.group_previews = groupPreviews;

  // 2nd most sensitive: redeem_code, private_note
  // Access to these is granted if the requester is the owner OR has EDIT permission on the record.
  const hasEditPermissionOnContact = permissionPreviews.includes(
    SystemPermissionType.EDIT
  );

  if (!isOwner && !hasEditPermissionOnContact) {
    redactedContact.redeem_code = undefined;
    redactedContact.private_note = undefined;
  }

  // 3rd most sensitive: notifications_url, from_placeholder_user_id
  // Access to these is granted if the requester is the owner OR has EDIT permission on the record OR is the contact itself.
  const isOwned = requesterUserId === contact.id;
  if (!isOwner && !hasEditPermissionOnContact && !isOwned) {
    redactedContact.notifications_url = "";
    redactedContact.from_placeholder_user_id = "";
  }

  redactedContact.labels = [];

  return redactedContact;
}

// --- Handlers ---

export async function getContactHandler(
  request: FastifyRequest<{ Params: GetContactParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id, contact_id } = request.params;

    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const plainContactId = contact_id;
    const contacts = await db.queryDrive(
      org_id,
      "SELECT * FROM contacts WHERE id = ?",
      [plainContactId]
    );

    if (!contacts || contacts.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Contact not found",
        })
      );
    }

    const contact = contacts[0] as Contact;
    contact.id = `${contact.id}` as UserID;

    const ownerId = await getDriveOwnerId(org_id);
    const isOwner = requesterApiKey.user_id === ownerId;

    const recordResourceId: SystemResourceID =
      `${plainContactId}` as SystemResourceID;
    const contactRecordPermissions = await checkSystemPermissions(
      recordResourceId,
      requesterApiKey.user_id,
      org_id
    );

    const contactsTableResourceId: SystemResourceID =
      `TABLE_${SystemTableValueEnum.CONTACTS}` as SystemResourceID;
    const contactsTablePermissions = await checkSystemPermissions(
      contactsTableResourceId,
      requesterApiKey.user_id,
      org_id
    );

    const hasViewPermission =
      isOwner ||
      contactRecordPermissions.includes(SystemPermissionType.VIEW) ||
      contactsTablePermissions.includes(SystemPermissionType.VIEW);

    if (!hasViewPermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const castFeContact = await redactContact(
      contact,
      requesterApiKey.user_id,
      org_id
    );

    return reply.status(200).send(createApiResponse(castFeContact));
  } catch (error) {
    request.log.error("Error in getContactHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function listContactsHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestListContacts }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;

    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const requestBody = request.body;

    if (requestBody.filters && requestBody.filters.length > 256) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: filters - Filters must be 256 characters or less",
        })
      );
    }
    const pageSize = requestBody.page_size || 50;
    if (pageSize === 0 || pageSize > 1000) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: page_size - Page size must be between 1 and 1000",
        })
      );
    }
    if (requestBody.cursor && requestBody.cursor.length > 256) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: cursor - Cursor must be 256 characters or less",
        })
      );
    }

    const direction = requestBody.direction || SortDirection.DESC;
    const filters = requestBody.filters || "";

    const ownerId = await getDriveOwnerId(org_id);
    const isOwner = requesterApiKey.user_id === ownerId;

    const hasViewTablePermission = await checkPermissionsTableAccess(
      requesterApiKey.user_id,
      SystemPermissionType.VIEW,
      org_id
    );

    if (!hasViewTablePermission && !isOwner) {
      return reply.status(200).send(
        createApiResponse<IResponseListContacts["ok"]["data"]>({
          items: [],
          page_size: 0,
          total: 0,
          direction: direction,
          cursor: null,
        })
      );
    }

    let totalCountResult;
    try {
      totalCountResult = await db.queryDrive(
        org_id,
        `SELECT COUNT(*) AS count FROM contacts ${filters ? `WHERE name LIKE '%${filters}%' OR email LIKE '%${filters}%' OR icp_principal LIKE '%${filters}%'` : ""}`
      );
    } catch (e) {
      request.log.error("Error querying total contacts count:", e);
      totalCountResult = [{ count: 0 }];
    }
    const totalCount = totalCountResult[0].count;

    if (totalCount === 0) {
      return reply.status(200).send(
        createApiResponse<IResponseListContacts["ok"]["data"]>({
          items: [],
          page_size: 0,
          total: 0,
          direction: direction,
          cursor: null,
        })
      );
    }

    let query = `SELECT * FROM contacts`;
    const queryParams: any[] = [];
    let whereClauses: string[] = [];

    if (filters) {
      whereClauses.push(
        `(name LIKE ? OR email LIKE ? OR icp_principal LIKE ?)`
      );
      queryParams.push(`%${filters}%`, `%${filters}%`, `%${filters}%`);
    }

    if (whereClauses.length > 0) {
      query += ` WHERE ${whereClauses.join(" AND ")}`;
    }

    query += ` ORDER BY created_at ${direction}`;

    let offset = 0;
    if (requestBody.cursor) {
      offset = parseInt(requestBody.cursor, 10);
      if (isNaN(offset)) {
        return reply.status(400).send(
          createApiResponse(undefined, {
            code: 400,
            message: "Invalid cursor format",
          })
        );
      }
    }
    query += ` LIMIT ? OFFSET ?`;
    queryParams.push(pageSize, offset);

    const rawContacts = await db.queryDrive(org_id, query, queryParams);

    const processedContacts: ContactFE[] = [];
    if (hasViewTablePermission || isOwner) {
      for (const contact of rawContacts) {
        contact.id = `${contact.id}` as UserID;
        processedContacts.push(
          await redactContact(
            contact as Contact,
            requesterApiKey.user_id,
            org_id
          )
        );
      }
    } else {
      for (const contact of rawContacts) {
        contact.id = `${contact.id}` as UserID;

        const contactRecordResourceId: SystemResourceID =
          `${contact.id}` as SystemResourceID;
        const contactRecordPermissions = await checkSystemPermissions(
          contactRecordResourceId,
          requesterApiKey.user_id,
          org_id
        );

        if (contactRecordPermissions.includes(SystemPermissionType.VIEW)) {
          processedContacts.push(
            await redactContact(
              contact as Contact,
              requesterApiKey.user_id,
              org_id
            )
          );
        }
      }
    }

    const nextCursor =
      processedContacts.length < pageSize
        ? null
        : (offset + pageSize).toString();

    const totalCountToReturn = processedContacts.length;

    return reply.status(200).send(
      createApiResponse<IResponseListContacts["ok"]["data"]>({
        items: processedContacts,
        page_size: processedContacts.length,
        total: totalCountToReturn,
        direction: direction,
        cursor: nextCursor,
      })
    );
  } catch (error) {
    request.log.error("Error in listContactsHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function createContactHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestCreateContact }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;

    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const createReq = request.body;

    if (createReq.id && !createReq.id.startsWith(IDPrefixEnum.User)) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Validation error: id - Invalid UserID format",
        })
      );
    }
    if (!createReq.icp_principal) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: icp_principal - ICP principal cannot be empty",
        })
      );
    }
    if (!validateIdString(createReq.name)) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: name - Name must be 256 characters or less",
        })
      );
    }
    if (createReq.email && !validateEmail(createReq.email)) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Validation error: email - Invalid email format",
        })
      );
    }
    if (createReq.avatar && !validateUrl(createReq.avatar)) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Validation error: avatar - Invalid avatar URL",
        })
      );
    }
    if (
      createReq.notifications_url &&
      !validateUrl(createReq.notifications_url)
    ) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: notifications_url - Invalid notifications URL",
        })
      );
    }
    if (
      createReq.evm_public_address &&
      !validateEvmAddress(createReq.evm_public_address)
    ) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: evm_public_address - Invalid EVM public address",
        })
      );
    }

    if (createReq.public_note && createReq.public_note.length > 8192) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: public_note - Public note must be 8,192 characters or less",
        })
      );
    }
    if (createReq.private_note && createReq.private_note.length > 8192) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: private_note - Private note must be 8,192 characters or less",
        })
      );
    }
    if (createReq.external_id && !validateExternalId(createReq.external_id)) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Validation error: external_id - Invalid external ID",
        })
      );
    }
    if (
      createReq.external_payload &&
      !validateExternalPayload(createReq.external_payload)
    ) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: external_payload - Invalid external payload",
        })
      );
    }

    if (createReq.seed_phrase) {
      try {
        const derivedAddresses = await seed_phrase_to_wallet_addresses(
          createReq.seed_phrase
        );
        if (derivedAddresses.icp_principal !== createReq.icp_principal) {
          return reply.status(400).send(
            createApiResponse(undefined, {
              code: 400,
              message: `Validation error: seed_phrase - Seed phrase generates ICP principal '${derivedAddresses.icp_principal}' which doesn't match the provided principal '${createReq.icp_principal}'`,
            })
          );
        }
        if (
          createReq.evm_public_address &&
          derivedAddresses.evm_public_address !== createReq.evm_public_address
        ) {
          return reply.status(400).send(
            createApiResponse(undefined, {
              code: 400,
              message: `Validation error: seed_phrase - Seed phrase generates EVM address '${derivedAddresses.evm_public_address}' which doesn't match the provided address '${createReq.evm_public_address}'`,
            })
          );
        }
      } catch (e: any) {
        return reply.status(400).send(
          createApiResponse(undefined, {
            code: 400,
            message: `Validation error: seed_phrase - ${e.message}`,
          })
        );
      }
    }

    const ownerId = await getDriveOwnerId(org_id);
    const isOwner = requesterApiKey.user_id === ownerId;

    const hasCreatePermission = await checkPermissionsTableAccess(
      requesterApiKey.user_id,
      SystemPermissionType.CREATE,
      org_id
    );

    if (!hasCreatePermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    // Generate contactId based on ICP principal if not provided
    const contactId: UserID =
      (createReq.id as UserID) ||
      (`${IDPrefixEnum.User}${createReq.icp_principal.replace(/[^a-zA-Z0-9]/g, "_")}` as UserID);
    const plainContactId = contactId;

    // Ensure the ID is unique if it's client-provided
    const existingContact = await db.queryDrive(
      org_id,
      "SELECT id FROM contacts WHERE id = ?",
      [plainContactId]
    );
    if (existingContact.length > 0) {
      return reply.status(409).send(
        createApiResponse(undefined, {
          code: 409,
          message: "Contact with this ID already exists.",
        })
      );
    }

    const newContact: Contact = {
      id: contactId,
      name: createReq.name,
      avatar: createReq.avatar || "",
      email: createReq.email || "",
      notifications_url: createReq.notifications_url || "",
      public_note: createReq.public_note || "",
      private_note: createReq.private_note || "",
      evm_public_address: createReq.evm_public_address || "",
      icp_principal: createReq.icp_principal,
      seed_phrase: createReq.seed_phrase || "",
      labels: [],
      from_placeholder_user_id: createReq.is_placeholder
        ? contactId
        : undefined,
      redeem_code: createReq.is_placeholder ? uuidv4() : undefined,
      created_at: Date.now(),
      last_online_ms: 0,
      external_id: createReq.external_id || undefined,
      external_payload: createReq.external_payload || undefined,
    };

    await dbHelpers.transaction("drive", org_id, (database) => {
      const stmt = database.prepare(
        `INSERT INTO contacts (id, name, avatar, email, notifications_url, public_note, private_note, evm_public_address, icp_principal, seed_phrase, from_placeholder_user_id, redeem_code, created_at, last_online_ms, external_id, external_payload)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );
      stmt.run(
        plainContactId,
        newContact.name,
        newContact.avatar,
        newContact.email,
        newContact.notifications_url,
        newContact.public_note,
        newContact.private_note,
        newContact.evm_public_address,
        newContact.icp_principal,
        newContact.seed_phrase,
        newContact.from_placeholder_user_id
          ? newContact.from_placeholder_user_id
          : null,
        newContact.redeem_code,
        newContact.created_at,
        newContact.last_online_ms,
        newContact.external_id,
        newContact.external_payload
      );

      // Add the contact to the default "Everyone" group if it exists
      interface DefaultGroupQueryResult {
        default_everyone_group_id?: string; // Change 'value' to the new column name
        id?: string; // Keep this for the fallback search
      }

      // Query the specific new column 'default_everyone_group_id'
      const defaultEveryoneGroupResult = database
        .prepare(
          `SELECT default_everyone_group_id FROM about_drive LIMIT 1` // Assuming about_drive is effectively a singleton for the current drive
        )
        .get() as DefaultGroupQueryResult | undefined;

      let defaultGroupId: GroupID | null = null;
      if (defaultEveryoneGroupResult?.default_everyone_group_id) {
        // Use the new column name
        defaultGroupId =
          defaultEveryoneGroupResult.default_everyone_group_id as GroupID;
      }

      if (defaultGroupId) {
        const plainDefaultGroupId = defaultGroupId;
        const newGroupInviteId: GroupInviteID =
          `${IDPrefixEnum.GroupInvite}${uuidv4()}` as GroupInviteID;
        const plainNewGroupInviteId = newGroupInviteId;

        // Check if a member invite for this user already exists in this group
        const existingInvite = database
          .prepare(
            `
          SELECT id FROM group_invites
          WHERE group_id = ? AND invitee_id = ?;
        `
          )
          .get(plainDefaultGroupId, plainContactId);

        if (!existingInvite) {
          // Insert into group_invites table
          database
            .prepare(
              `
            INSERT INTO group_invites (id, group_id, inviter_id, invitee_type, invitee_id, role, note, active_from, expires_at, created_at, last_modified_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
          `
            )
            .run(
              plainNewGroupInviteId,
              plainDefaultGroupId,
              requesterApiKey.user_id,
              "USER",
              plainContactId,
              GroupRole.MEMBER, // Default role for "Everyone" group
              `Auto-invited to default 'Default Everyone' upon contact creation.`,
              Date.now(),
              -1, // Never expires
              Date.now(),
              Date.now()
            );
        }
      }
    });

    const castFeContact = await redactContact(
      newContact,
      requesterApiKey.user_id,
      org_id
    );

    return reply.status(200).send(createApiResponse(castFeContact));
  } catch (error) {
    request.log.error("Error in createContactHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function updateContactHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestUpdateContact }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;

    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const updateReq = request.body;

    if (!validateUserId(updateReq.id)) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Validation error: id - Invalid UserID format",
        })
      );
    }
    if (updateReq.name && !validateIdString(updateReq.name)) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: name - Name must be 256 characters or less",
        })
      );
    }
    if (updateReq.email && !validateEmail(updateReq.email)) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Validation error: email - Invalid email format",
        })
      );
    }
    if (updateReq.avatar && !validateUrl(updateReq.avatar)) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Validation error: avatar - Invalid avatar URL",
        })
      );
    }
    if (
      updateReq.notifications_url &&
      !validateUrl(updateReq.notifications_url)
    ) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: notifications_url - Invalid notifications URL",
        })
      );
    }
    if (
      updateReq.evm_public_address &&
      !validateEvmAddress(updateReq.evm_public_address)
    ) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: evm_public_address - Invalid EVM public address",
        })
      );
    }

    if (updateReq.public_note && updateReq.public_note.length > 8192) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: public_note - Public note must be 8,192 characters or less",
        })
      );
    }
    if (updateReq.private_note && updateReq.private_note.length > 8192) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: private_note - Private note must be 8,192 characters or less",
        })
      );
    }
    if (updateReq.external_id && !validateExternalId(updateReq.external_id)) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Validation error: external_id - Invalid external ID",
        })
      );
    }
    if (
      updateReq.external_payload &&
      !validateExternalPayload(updateReq.external_payload)
    ) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: external_payload - Invalid external payload",
        })
      );
    }

    const plainContactId = updateReq.id;
    const contacts = await db.queryDrive(
      org_id,
      "SELECT * FROM contacts WHERE id = ?",
      [plainContactId]
    );

    if (!contacts || contacts.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Contact not found",
        })
      );
    }

    const existingContact = contacts[0] as Contact;
    existingContact.id = `${existingContact.id}` as UserID;

    const ownerId = await getDriveOwnerId(org_id);
    const isOwner = requesterApiKey.user_id === ownerId;

    const recordResourceId: SystemResourceID =
      `${plainContactId}` as SystemResourceID;
    const contactRecordPermissions = await checkSystemPermissions(
      recordResourceId,
      requesterApiKey.user_id,
      org_id
    );

    const contactsTableResourceId: SystemResourceID =
      `TABLE_${SystemTableValueEnum.CONTACTS}` as SystemResourceID;
    const contactsTablePermissions = await checkSystemPermissions(
      contactsTableResourceId,
      requesterApiKey.user_id,
      org_id
    );

    const hasEditPermission =
      isOwner ||
      contactRecordPermissions.includes(SystemPermissionType.EDIT) ||
      contactsTablePermissions.includes(SystemPermissionType.EDIT);

    if (!hasEditPermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const updates: string[] = [];
    const values: any[] = [];

    if (updateReq.name !== undefined) {
      updates.push("name = ?");
      values.push(updateReq.name);
    }
    if (updateReq.avatar !== undefined) {
      updates.push("avatar = ?");
      values.push(updateReq.avatar);
    }
    if (updateReq.email !== undefined) {
      updates.push("email = ?");
      values.push(updateReq.email);
    }
    if (updateReq.notifications_url !== undefined) {
      updates.push("notifications_url = ?");
      values.push(updateReq.notifications_url);
    }
    if (updateReq.public_note !== undefined) {
      updates.push("public_note = ?");
      values.push(updateReq.public_note);
    }
    if (updateReq.private_note !== undefined) {
      // Allow private_note update if hasEditPermission is true (or isOwner)
      updates.push("private_note = ?");
      values.push(updateReq.private_note);
    }
    if (updateReq.evm_public_address !== undefined) {
      updates.push("evm_public_address = ?");
      values.push(updateReq.evm_public_address);
    }
    if (updateReq.external_id !== undefined) {
      updates.push("external_id = ?");
      values.push(updateReq.external_id);
    }
    if (updateReq.external_payload !== undefined) {
      updates.push("external_payload = ?");
      values.push(updateReq.external_payload);
    }

    if (updates.length === 0) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "No fields to update",
        })
      );
    }

    values.push(plainContactId);

    await dbHelpers.transaction("drive", org_id, (database) => {
      const stmt = database.prepare(
        `UPDATE contacts SET ${updates.join(", ")} WHERE id = ?`
      );
      stmt.run(...values);
    });

    const updatedContacts = await db.queryDrive(
      org_id,
      "SELECT * FROM contacts WHERE id = ?",
      [plainContactId]
    );

    const updatedContact = updatedContacts[0] as Contact;
    updatedContact.id = `${updatedContact.id}` as UserID;

    const castFeContact = await redactContact(
      updatedContact,
      requesterApiKey.user_id,
      org_id
    );

    return reply.status(200).send(createApiResponse(castFeContact));
  } catch (error) {
    request.log.error("Error in updateContactHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function deleteContactHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestDeleteContact }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;

    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const deleteReq = request.body;

    if (!validateUserId(deleteReq.id)) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Validation error: id - Invalid UserID format",
        })
      );
    }

    const plainContactId = deleteReq.id;
    const contacts = await db.queryDrive(
      org_id,
      "SELECT * FROM contacts WHERE id = ?",
      [plainContactId]
    );

    if (!contacts || contacts.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Contact not found",
        })
      );
    }

    const existingContact = contacts[0] as Contact;
    existingContact.id = `${existingContact.id}` as UserID;

    const ownerId = await getDriveOwnerId(org_id);
    const isOwner = requesterApiKey.user_id === ownerId;

    const recordResourceId: SystemResourceID =
      `${plainContactId}` as SystemResourceID;
    const contactRecordPermissions = await checkSystemPermissions(
      recordResourceId,
      requesterApiKey.user_id,
      org_id
    );

    const contactsTableResourceId: SystemResourceID =
      `TABLE_${SystemTableValueEnum.CONTACTS}` as SystemResourceID;
    const contactsTablePermissions = await checkSystemPermissions(
      contactsTableResourceId,
      requesterApiKey.user_id,
      org_id
    );

    const hasDeletePermission =
      isOwner ||
      contactRecordPermissions.includes(SystemPermissionType.DELETE) ||
      contactsTablePermissions.includes(SystemPermissionType.DELETE);

    if (!hasDeletePermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    await dbHelpers.transaction("drive", org_id, (database) => {
      // Delete from contacts table
      database.prepare("DELETE FROM contacts WHERE id = ?").run(plainContactId);

      // Clean up related entries in `group_invites` where this user is the invitee
      database
        .prepare(
          "DELETE FROM group_invites WHERE invitee_id = ? AND invitee_type = 'USER'"
        )
        .run(plainContactId);

      // Clean up api_keys associated with this user
      database
        .prepare("DELETE FROM api_keys WHERE user_id = ?")
        .run(plainContactId);

      // Clean up permissions_directory where this user is grantee or granter
      database
        .prepare("DELETE FROM permissions_directory WHERE granted_by = ?")
        .run(plainContactId);
      database
        .prepare(
          "DELETE FROM permissions_directory WHERE grantee_id = ? AND grantee_type = 'User'"
        )
        .run(plainContactId);

      // Clean up permissions_system where this user is grantee or granter
      database
        .prepare("DELETE FROM permissions_system WHERE granted_by = ?")
        .run(plainContactId);
      database
        .prepare(
          "DELETE FROM permissions_system WHERE grantee_id = ? AND grantee_type = 'User'"
        )
        .run(plainContactId);
    });

    const deletedData: IResponseDeleteContact["ok"]["data"] = {
      id: deleteReq.id,
      deleted: true,
    };

    return reply.status(200).send(createApiResponse(deletedData));
  } catch (error) {
    request.log.error("Error in deleteContactHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function redeemContactHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestRedeemContact }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;

    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const redeemReq = request.body;

    if (!validateUserId(redeemReq.current_user_id)) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Validation error: current_user_id - Invalid UserID format",
        })
      );
    }
    if (!validateUserId(redeemReq.new_user_id)) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Validation error: new_user_id - Invalid UserID format",
        })
      );
    }
    if (!redeemReq.redeem_code) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Validation error: redeem_code - Redeem code is required",
        })
      );
    }

    const currentPlainUserId = redeemReq.current_user_id;
    const newPlainUserId = redeemReq.new_user_id;

    const currentContacts = await db.queryDrive(
      org_id,
      "SELECT * FROM contacts WHERE id = ?",
      [currentPlainUserId]
    );

    if (!currentContacts || currentContacts.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Current contact not found",
        })
      );
    }

    const currentContact = currentContacts[0] as Contact;
    currentContact.id = `${currentContact.id}` as UserID;

    if (currentContact.redeem_code !== redeemReq.redeem_code) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Redeem token does not match",
        })
      );
    }

    const ownerId = await getDriveOwnerId(org_id);
    const isOwner = requesterApiKey.user_id === ownerId;

    const hasInviteTablePermission = await checkPermissionsTableAccess(
      requesterApiKey.user_id,
      SystemPermissionType.INVITE,
      org_id
    );

    const currentContactRecordResourceId: SystemResourceID =
      `${currentPlainUserId}` as SystemResourceID;
    const currentContactRecordPermissions = await checkSystemPermissions(
      currentContactRecordResourceId,
      requesterApiKey.user_id,
      org_id
    );
    const canEditCurrentContact = currentContactRecordPermissions.includes(
      SystemPermissionType.EDIT
    );

    if (!isOwner && !hasInviteTablePermission && !canEditCurrentContact) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Forbidden: Insufficient permissions to redeem contact.",
        })
      );
    }

    let updateCount = 0;
    try {
      await dbHelpers.transaction("drive", org_id, (database) => {
        // 1. Update `contacts` table for the placeholder
        database
          .prepare(
            "UPDATE contacts SET id = ?, from_placeholder_user_id = NULL, redeem_code = NULL WHERE id = ?"
          )
          .run(newPlainUserId, currentPlainUserId);

        // 2. Add old ID to `contact_past_ids` for the new contact
        database
          .prepare(
            "INSERT INTO contact_past_ids (user_id, past_user_id) VALUES (?, ?)"
          )
          .run(newPlainUserId, currentPlainUserId);

        // 3. Update `api_keys`
        database
          .prepare("UPDATE api_keys SET user_id = ? WHERE user_id = ?")
          .run(newPlainUserId, currentPlainUserId);

        // 4. Update `folders`
        database
          .prepare("UPDATE folders SET created_by = ? WHERE created_by = ?")
          .run(newPlainUserId, currentPlainUserId);
        database
          .prepare(
            "UPDATE folders SET last_updated_by = ? WHERE last_updated_by = ?"
          )
          .run(newPlainUserId, currentPlainUserId);

        // 5. Update `files`
        database
          .prepare("UPDATE files SET created_by = ? WHERE created_by = ?")
          .run(newPlainUserId, currentPlainUserId);
        database
          .prepare(
            "UPDATE files SET last_updated_by = ? WHERE last_updated_by = ?"
          )
          .run(newPlainUserId, currentPlainUserId);

        // 6. Update `groups`
        database
          .prepare("UPDATE groups SET owner = ? WHERE owner = ?")
          .run(newPlainUserId, currentPlainUserId);

        // 7. Update `group_invites`
        database
          .prepare(
            "UPDATE group_invites SET inviter_id = ? WHERE inviter_id = ?"
          )
          .run(newPlainUserId, currentPlainUserId);
        database
          .prepare(
            "UPDATE group_invites SET invitee_id = ? WHERE invitee_id = ? AND invitee_type = 'USER'"
          )
          .run(newPlainUserId, currentPlainUserId);

        // 8. Update `labels` (created_by)
        database
          .prepare("UPDATE labels SET created_by = ? WHERE created_by = ?")
          .run(newPlainUserId, currentPlainUserId);

        // 9. Update `permissions_directory`
        database
          .prepare(
            "UPDATE permissions_directory SET granted_by = ? WHERE granted_by = ?"
          )
          .run(newPlainUserId, currentPlainUserId);
        database
          .prepare(
            "UPDATE permissions_directory SET grantee_id = ? WHERE grantee_id = ? AND grantee_type = 'User'"
          )
          .run(newPlainUserId, currentPlainUserId);

        // 10. Update `permissions_system`
        database
          .prepare(
            "UPDATE permissions_system SET granted_by = ? WHERE granted_by = ?"
          )
          .run(newPlainUserId, currentPlainUserId);
        database
          .prepare(
            "UPDATE permissions_system SET grantee_id = ? WHERE grantee_id = ? AND grantee_type = 'User'"
          )
          .run(newPlainUserId, currentPlainUserId);

        // 11. Update `contact_id_superswap_history`
        database
          .prepare(
            "INSERT INTO contact_id_superswap_history (old_user_id, new_user_id, swapped_at) VALUES (?, ?, ?)"
          )
          .run(currentPlainUserId, newPlainUserId, Date.now());

        updateCount = 1;
      });
    } catch (e: any) {
      request.log.error("Error during superswap:", e);
      return reply.status(500).send(
        createApiResponse(undefined, {
          code: 500,
          message: `Failed to redeem contact: ${e.message || "Unknown error during superswap"}`,
        })
      );
    }

    const updatedContactResult = await db.queryDrive(
      org_id,
      "SELECT * FROM contacts WHERE id = ?",
      [newPlainUserId]
    );

    let updatedContact: Contact;
    if (updatedContactResult.length > 0) {
      updatedContact = updatedContactResult[0] as Contact;
      updatedContact.id = `${updatedContact.id}` as UserID;
      if (redeemReq.note) {
        const newPublicNote = updatedContact.public_note
          ? `Note from User: ${redeemReq.note}, Prior Original Note: ${updatedContact.public_note}`
          : `Note from User: ${redeemReq.note}`;
        await db.queryDrive(
          org_id,
          "UPDATE contacts SET public_note = ? WHERE id = ?",
          [newPublicNote, updatedContact.id]
        );
        updatedContact.public_note = newPublicNote;
      }
    } else {
      throw new Error("Redeemed contact not found after superswap.");
    }

    // TODO: SUPERSWAP Fire a webhook for superswap user

    const newApiKeyId = `${IDPrefixEnum.ApiKey}${uuidv4()}` as ApiKeyID;
    const newApiKeyValue = await generateApiKey();
    const generatedApiKey: ApiKey = {
      id: newApiKeyId,
      value: newApiKeyValue as ApiKeyValue,
      user_id: redeemReq.new_user_id,
      name: "Superswap User API Key",
      private_note: "Automatically generated API key for superswapped user",
      created_at: Date.now(),
      begins_at: 0,
      expires_at: -1,
      is_revoked: false,
      labels: [],
    };

    await dbHelpers.transaction("drive", org_id, (database) => {
      database
        .prepare(
          `INSERT INTO api_keys (id, value, user_id, name, private_note, created_at, begins_at, expires_at, is_revoked, external_id, external_payload)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
        )
        .run(
          generatedApiKey.id, // Corrected: ApiKeyID prefix handled by
          generatedApiKey.value,
          generatedApiKey.user_id,
          generatedApiKey.name,
          generatedApiKey.private_note,
          generatedApiKey.created_at,
          generatedApiKey.begins_at,
          generatedApiKey.expires_at,
          generatedApiKey.is_revoked ? 1 : 0,
          generatedApiKey.external_id,
          generatedApiKey.external_payload
        );
    });

    const redeemedContactFe = await redactContact(
      updatedContact,
      requesterApiKey.user_id,
      org_id
    );

    const responseData: IResponseRedeemContact["ok"]["data"] = {
      contact: redeemedContactFe,
      api_key: generatedApiKey.value,
    };

    return reply.status(200).send(createApiResponse(responseData));
  } catch (error) {
    request.log.error("Error in redeemContactHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}
