// src/routes/v1/drive/contacts/handlers.ts

// src/routes/v1/drive/contacts/handlers.ts
import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import {
  Contact,
  ContactFE,
  FactoryApiResponse,
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
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import {
  authenticateRequest,
  generateApiKey,
  seed_phrase_to_wallet_addresses,
} from "../../../../services/auth"; // Assuming seed_phrase_to_wallet_addresses is in auth service
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

// Type definitions for route params
interface GetContactParams extends OrgIdParams {
  contact_id: UserID;
}

// --- Utility Functions (Mimicking Rust's `Contact::redacted` and permission checks) ---

// This should ideally be a method on the Contact object if we were building a full ORM.
// For now, it's a standalone function mimicking the Rust `redacted` behavior.
// TODO: REDACT Refactor into a class method or a separate service for Contact if possible within TS structure.
async function redactContact(
  contact: Contact,
  requesterUserId: UserID,
  orgId: string
): Promise<ContactFE> {
  const redactedContact = { ...contact } as ContactFE;

  const ownerId = await getDriveOwnerId(orgId);
  const isOwner = requesterUserId === ownerId;
  const isOwned = requesterUserId === contact.id;

  // Simulate `check_system_permissions`
  // TODO: PERMIT Implement actual permission checks based on your SQLite schema and data.
  // For now, return a mock set of permissions.
  const permissionPreviews: SystemPermissionType[] = [];
  if (isOwner || isOwned) {
    permissionPreviews.push(
      SystemPermissionType.EDIT,
      SystemPermissionType.VIEW
    );
  } else {
    permissionPreviews.push(SystemPermissionType.VIEW); // Default view for non-owners/non-self
  }
  redactedContact.permission_previews = permissionPreviews;

  // Filter labels (mocking redact_label behavior)
  // TODO: REDACT Implement actual label redaction logic based on permissions/ownership.
  redactedContact.labels = contact.labels;

  // Filter group previews (mocking redact_group_previews behavior)
  // TODO: GROUP Fetch real group data and filter based on permissions.
  redactedContact.group_previews = []; // Sensible placeholder

  // 2nd most sensitive: redeem_code, private_note
  if (!isOwner && !permissionPreviews.includes(SystemPermissionType.EDIT)) {
    redactedContact.redeem_code = undefined;
    redactedContact.private_note = undefined;
  }

  // 3rd most sensitive: notifications_url, from_placeholder_user_id
  if (
    !isOwner &&
    !permissionPreviews.includes(SystemPermissionType.EDIT) &&
    !isOwned
  ) {
    redactedContact.notifications_url = "";
    redactedContact.from_placeholder_user_id = "";
  }

  return redactedContact;
}
// --- Handlers ---

export async function getContactHandler(
  request: FastifyRequest<{ Params: GetContactParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id, contact_id } = request.params;

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    // Get the contact from the specific drive DB
    const contacts = await db.queryDrive(
      org_id,
      "SELECT * FROM contacts WHERE id = ?",
      [contact_id]
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
    const ownerId = await getDriveOwnerId(org_id);
    const isOwner = requesterApiKey.user_id === ownerId;

    // TODO:  PERMIT Implement actual permission checks based on `permissions_system` table.
    // For now, simulate the Rust logic: if not owner, check for 'View' permission.
    let hasPermission = isOwner;
    if (!isOwner) {
      // Simulate check_system_permissions logic for a specific contact or the entire contacts table
      // In a real scenario, this would involve complex DB queries joining permissions_system and contacts.
      // For now, we'll assume a simplified check.
      const canViewRecord = true; // TODO: PERMIT Query permissions_system table for SystemPermissionType.View on this specific contact_id
      const canViewTable = true; // TODO: PERMIT Query permissions_system table for SystemPermissionType.View on 'CONTACTS' table
      hasPermission = canViewRecord || canViewTable;
    }

    if (!hasPermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    // Redact sensitive fields based on requester's permissions and ownership
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
        message: "Internal server error",
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

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const requestBody = request.body;

    // Validate request body (similar to Rust's `validate_body` on `ListContactsRequestBody`)
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
    const filters = requestBody.filters || ""; // Use an empty string if no filters

    // Query for total count (might be restricted by permissions)
    let totalCountResult;
    try {
      totalCountResult = await db.queryDrive(
        org_id,
        `SELECT COUNT(*) AS count FROM contacts ${filters ? `WHERE name LIKE '%${filters}%' OR email LIKE '%${filters}%' OR icp_principal LIKE '%${filters}%'` : ""}`
      );
    } catch (e) {
      request.log.error("Error querying total contacts count:", e);
      // Fallback to 0 or re-throw based on error handling policy
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

    // Add filtering
    if (filters) {
      whereClauses.push(
        `(name LIKE ? OR email LIKE ? OR icp_principal LIKE ?)`
      );
      queryParams.push(`%${filters}%`, `%${filters}%`, `%${filters}%`);
    }

    if (whereClauses.length > 0) {
      query += ` WHERE ${whereClauses.join(" AND ")}`;
    }

    // Add ordering
    query += ` ORDER BY created_at ${direction || SortDirection.ASC}`;

    // Add pagination (LIMIT and OFFSET based on cursor)
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

    // Apply redaction and permission checks to each contact
    const processedContacts: ContactFE[] = [];
    for (const contact of rawContacts) {
      // TODO: PERMIT In a real scenario, implement `check_system_permissions` to determine if `requesterApiKey.user_id`
      // has VIEW permission for this specific contact record OR the overall 'contacts' table.
      // For now, we'll assume everyone can view if they pass the initial auth.
      const canView = true; // Placeholder for actual permission check.

      if (canView) {
        processedContacts.push(
          await redactContact(
            contact as Contact,
            requesterApiKey.user_id,
            org_id
          )
        );
      }
    }

    // Determine next cursor
    const nextCursor =
      processedContacts.length < pageSize
        ? null
        : (offset + pageSize).toString();

    // Determine total count to return (considering permissions, similar to Rust's logic)
    // For simplicity, we'll return the actual total count if the user is authenticated.
    // In a more complex scenario, this would depend on the `has_table_permission` logic.
    let totalCountToReturn = totalCount;

    return reply.status(200).send(
      createApiResponse<IResponseListContacts["ok"]["data"]>({
        items: processedContacts,
        page_size: processedContacts.length, // Actual items returned
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
        message: "Internal server error",
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

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const createReq = request.body;

    // Validate request body (mimicking Rust's `validate_body`)
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
    // TODO: VALIDATE  Add more robust ICP principal validation (e.g., using @dfinity/principal)
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

    // Seed phrase validation and address derivation
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

    // Check create permission if not owner
    if (!isOwner) {
      // TODO: PERMIT Implement actual permission check for SystemTableEnum.Contacts with SystemPermissionType.Create
      const hasCreatePermission = true; // Placeholder for actual permission check
      if (!hasCreatePermission) {
        return reply
          .status(403)
          .send(
            createApiResponse(undefined, { code: 403, message: "Forbidden" })
          );
      }
    }

    const contactId: UserID =
      (createReq.id as UserID) ||
      `${IDPrefixEnum.User}${createReq.icp_principal.replace(/[^a-zA-Z0-9]/g, "_")}`;

    // Ensure the ID is unique if it's client-provided
    const existingContact = await db.queryDrive(
      org_id,
      "SELECT id FROM contacts WHERE id = ?",
      [contactId]
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
      private_note: createReq.private_note || "", // Allow null if not provided
      evm_public_address: createReq.evm_public_address || "",
      icp_principal: createReq.icp_principal,
      seed_phrase: createReq.seed_phrase || "",
      labels: [],
      from_placeholder_user_id: createReq.is_placeholder
        ? contactId
        : undefined,
      redeem_code: createReq.is_placeholder ? uuidv4() : undefined, // Generate if placeholder
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
        newContact.id,
        newContact.name,
        newContact.avatar,
        newContact.email,
        newContact.notifications_url,
        newContact.public_note,
        newContact.private_note,
        newContact.evm_public_address,
        newContact.icp_principal,
        newContact.seed_phrase,
        newContact.from_placeholder_user_id,
        newContact.redeem_code,
        newContact.created_at,
        newContact.last_online_ms,
        newContact.external_id,
        newContact.external_payload
      );

      // TODO: GROUP Add the contact to the default "Everyone" group if it exists (requires a service to handle group memberships)
      // This would involve inserting into the `contact_groups` table and potentially updating `groups` and `group_invites` tables.
    });

    // Redact sensitive fields before sending response
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
        message: "Internal server error",
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

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const updateReq = request.body;

    // Validate request body
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

    // Get existing contact
    const contacts = await db.queryDrive(
      org_id,
      "SELECT * FROM contacts WHERE id = ?",
      [updateReq.id]
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
    const ownerId = await getDriveOwnerId(org_id);
    const isOwner = requesterApiKey.user_id === ownerId;

    // Check update permission if not owner
    if (!isOwner) {
      // TODO: PERMIT Implement actual permission checks for SystemTableEnum.Contacts or specific contact record with SystemPermissionType.Edit
      const hasEditPermission = true; // Placeholder for actual permission check
      if (!hasEditPermission) {
        return reply
          .status(403)
          .send(
            createApiResponse(undefined, { code: 403, message: "Forbidden" })
          );
      }
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
    // Only owner can update private_note
    if (updateReq.private_note !== undefined && isOwner) {
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

    values.push(updateReq.id); // WHERE clause parameter

    await dbHelpers.transaction("drive", org_id, (database) => {
      const stmt = database.prepare(
        `UPDATE contacts SET ${updates.join(", ")} WHERE id = ?`
      );
      stmt.run(...values);
    });

    // Fetch the updated contact to return
    const updatedContacts = await db.queryDrive(
      org_id,
      "SELECT * FROM contacts WHERE id = ?",
      [updateReq.id]
    );

    const updatedContact = updatedContacts[0] as Contact;

    // Redact sensitive fields before sending response
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
        message: "Internal server error",
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

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const deleteReq = request.body;

    // Validate request body
    if (!validateUserId(deleteReq.id)) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Validation error: id - Invalid UserID format",
        })
      );
    }

    // Get existing contact
    const contacts = await db.queryDrive(
      org_id,
      "SELECT * FROM contacts WHERE id = ?",
      [deleteReq.id]
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
    const ownerId = await getDriveOwnerId(org_id);
    const isOwner = requesterApiKey.user_id === ownerId;

    // Check delete permission if not owner
    if (!isOwner) {
      // TODO: PERMIT Implement actual permission checks for SystemTableEnum.Contacts or specific contact record with SystemPermissionType.Delete
      const hasDeletePermission = true; // Placeholder for actual permission check
      if (!hasDeletePermission) {
        return reply
          .status(403)
          .send(
            createApiResponse(undefined, { code: 403, message: "Forbidden" })
          );
      }
    }

    await dbHelpers.transaction("drive", org_id, (database) => {
      // Delete from contacts table
      const stmt = database.prepare("DELETE FROM contacts WHERE id = ?");
      stmt.run(deleteReq.id);

      // TODO: GROUP Clean up related entries in `contact_groups` and `group_invites` tables.
      // This would involve more complex SQL or a dedicated service to manage these relationships.
      // Example:
      // database.prepare("DELETE FROM contact_groups WHERE user_id = ?").run(deleteReq.id);
      // database.prepare("DELETE FROM group_invites WHERE invitee_id = ?").run(deleteReq.id);
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
        message: "Internal server error",
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

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const redeemReq = request.body;

    // Validate request body
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

    // Check for existence of current user contact and redeem token match
    const currentContacts = await db.queryDrive(
      org_id,
      "SELECT * FROM contacts WHERE id = ?",
      [redeemReq.current_user_id]
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

    if (currentContact.redeem_code !== redeemReq.redeem_code) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Redeem token does not match",
        })
      );
    }

    // Perform superswap operation
    // This is a complex operation that needs to update all references to `current_user_id` to `new_user_id`
    // across all relevant tables (contacts.past_user_ids, api_keys.user_id, folders.created_by_user_id,
    // files.created_by_user_id, groups.owner_user_id, group_invites.inviter_user_id, etc.).
    // This cannot be done with a simple single SQL query and requires a dedicated service.
    // TODO: PERMIT Implement the `superswapUserId` function that updates all relevant tables.
    let updateCount = 0; // Placeholder for actual records updated
    try {
      // Example of what `superswapUserId` would do (simplified):
      await dbHelpers.transaction("drive", org_id, (database) => {
        // 1. Update `contacts` table for the placeholder
        database
          .prepare(
            "UPDATE contacts SET id = ?, from_placeholder_user_id = NULL, redeem_code = NULL WHERE id = ?"
          )
          .run(redeemReq.new_user_id, redeemReq.current_user_id);

        // 2. Add old ID to `past_user_ids` for the new contact
        // This is more complex for SQLite, might involve fetching the contact, updating its array/JSON field, then saving.
        // For now, we'll directly update the `contacts` table (simplistic, needs refinement for `past_user_ids` list)
        const updatePastIdsStmt = database.prepare(
          "UPDATE contacts SET past_user_ids = json_insert(coalesce(past_user_ids, '[]'), '$[#]', ?) WHERE id = ?"
        );
        updatePastIdsStmt.run(redeemReq.current_user_id, redeemReq.new_user_id);

        // 3. Update `api_keys`
        database
          .prepare("UPDATE api_keys SET user_id = ? WHERE user_id = ?")
          .run(redeemReq.new_user_id, redeemReq.current_user_id);

        // 4. Update `folders`
        database
          .prepare(
            "UPDATE folders SET created_by_user_id = ? WHERE created_by_user_id = ?"
          )
          .run(redeemReq.new_user_id, redeemReq.current_user_id);
        database
          .prepare(
            "UPDATE folders SET last_updated_by_user_id = ? WHERE last_updated_by_user_id = ?"
          )
          .run(redeemReq.new_user_id, redeemReq.current_user_id);

        // 5. Update `files`
        database
          .prepare(
            "UPDATE files SET created_by_user_id = ? WHERE created_by_user_id = ?"
          )
          .run(redeemReq.new_user_id, redeemReq.current_user_id);
        database
          .prepare(
            "UPDATE files SET last_updated_by_user_id = ? WHERE last_updated_by_user_id = ?"
          )
          .run(redeemReq.new_user_id, redeemReq.current_user_id);

        // 6. Update `groups`
        database
          .prepare(
            "UPDATE groups SET owner_user_id = ? WHERE owner_user_id = ?"
          )
          .run(redeemReq.new_user_id, redeemReq.current_user_id);

        // 7. Update `group_invites`
        database
          .prepare(
            "UPDATE group_invites SET inviter_user_id = ? WHERE inviter_user_id = ?"
          )
          .run(redeemReq.new_user_id, redeemReq.current_user_id);
        database
          .prepare(
            "UPDATE group_invites SET invitee_id = ? WHERE invitee_id = ?"
          )
          .run(redeemReq.new_user_id, redeemReq.current_user_id);

        // 8. Update `labels` (created_by_user_id)
        database
          .prepare(
            "UPDATE labels SET created_by_user_id = ? WHERE created_by_user_id = ?"
          )
          .run(redeemReq.new_user_id, redeemReq.current_user_id);

        // 9. Update `permissions_directory`
        database
          .prepare(
            "UPDATE permissions_directory SET granted_by_user_id = ? WHERE granted_by_user_id = ?"
          )
          .run(redeemReq.new_user_id, redeemReq.current_user_id);
        database
          .prepare(
            "UPDATE permissions_directory SET grantee_id = ? WHERE grantee_id = ? AND grantee_type = 'User'"
          )
          .run(redeemReq.new_user_id, redeemReq.current_user_id);

        // 10. Update `permissions_system`
        database
          .prepare(
            "UPDATE permissions_system SET granted_by_user_id = ? WHERE granted_by_user_id = ?"
          )
          .run(redeemReq.new_user_id, redeemReq.current_user_id);
        database
          .prepare(
            "UPDATE permissions_system SET grantee_id = ? WHERE grantee_id = ? AND grantee_type = 'User'"
          )
          .run(redeemReq.new_user_id, redeemReq.current_user_id);

        // 11. Update `contact_id_superswap_history`
        database
          .prepare(
            "INSERT INTO contact_id_superswap_history (old_user_id, new_user_id, swapped_at) VALUES (?, ?, ?)"
          )
          .run(redeemReq.current_user_id, redeemReq.new_user_id, Date.now());

        // A more accurate `updateCount` would be to sum the changes from each affected table.
        updateCount = 1; // Simplified, indicating at least the main contact was updated
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

    // Update the redeemed contact's public note if a note is provided
    const updatedContact = await db.queryDrive(
      org_id,
      "SELECT * FROM contacts WHERE id = ?",
      [redeemReq.new_user_id]
    );
    if (updatedContact.length > 0) {
      let contactToUpdate = updatedContact[0] as Contact;
      if (redeemReq.note) {
        const newPublicNote = contactToUpdate.public_note
          ? `Note from User: ${redeemReq.note}, Prior Original Note: ${contactToUpdate.public_note}`
          : `Note from User: ${redeemReq.note}`;
        await db.queryDrive(
          org_id,
          "UPDATE contacts SET public_note = ? WHERE id = ?",
          [newPublicNote, redeemReq.new_user_id]
        );
        contactToUpdate.public_note = newPublicNote; // Update in memory for response
      }
    }

    // TODO: WEBHOOK Fire a webhook for superswap user
    // `fire_superswap_user_webhook` function is missing, would involve external HTTP calls.
    // await fire_superswap_user_webhook(
    //   WebhookEventLabel.OrganizationSuperswapUser,
    //   [], // active_webhooks - need to fetch these
    //   redeemReq.current_user_id,
    //   redeemReq.new_user_id,
    //   `Redeem Contact - superswap ${redeemReq.current_user_id} to ${redeemReq.new_user_id}, updated ${updateCount} records`
    // );

    // Generate new API key for the new user ID
    const newApiKeyId = `${IDPrefixEnum.ApiKey}${uuidv4()}` as ApiKeyID;
    const newApiKeyValue = await generateApiKey(); // Assuming generateApiKey from auth.ts
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
          generatedApiKey.id,
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
      updatedContact[0] as Contact,
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
        message: "Internal server error",
      })
    );
  }
}
