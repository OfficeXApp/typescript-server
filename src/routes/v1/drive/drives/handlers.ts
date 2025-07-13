import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import {
  Drive,
  DriveFE,
  DriveID,
  ICPPrincipalString,
  IDPrefixEnum,
  IRequestCreateDrive,
  IRequestDeleteDrive,
  IRequestGetDrive,
  IRequestListDrives,
  IRequestUpdateDrive,
  IResponseDeleteDrive,
  IPaginatedResponse,
  SystemPermissionType,
  SystemTableValueEnum,
  URLEndpoint,
  UserID,
  ApiResponse,
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import { authenticateRequest } from "../../../../services/auth";
import { createApiResponse, getDriveOwnerId, OrgIdParams } from "../../types";

// Import the actual permission checking functions from the services/permissions/system.ts
import {
  checkSystemPermissions,
  canUserAccessSystemPermission,
} from "../../../../services/permissions/system";

// Import the redactLabelValue from the labels handler
import { redactLabelValue } from "../../drive/labels/handlers";

// Helper to cast Drive to DriveFE and apply redaction logic
async function castDriveToFE(
  drive: Drive,
  userId: UserID,
  orgId: DriveID
): Promise<DriveFE> {
  const isOwner = userId === (await getDriveOwnerId(orgId));

  // Use the actual checkSystemPermissions for permission previews
  const permissionPreviews = await checkSystemPermissions(
    `TABLE_${SystemTableValueEnum.DRIVES}` as SystemTableValueEnum, // SystemResourceID for the table
    userId,
    orgId
  );

  let privateNote = drive.private_note;
  // Rust's SystemPermission.cast_fe does not redact private_note based on permission_previews.
  // It relies on is_owner. If private_note redaction is tied to EDIT permission here, it's an enhancement.
  // For now, aligning with Rust's stricter redaction for private_note based solely on ownership or a direct "can access record" check.
  // The provided Rust code for DirectoryPermission::cast_fe also doesn't seem to redact private_note based on permissions.
  // It seems the `redacted` function in Rust only applies to labels and external_id/payload.
  // Assuming `private_note` is only accessible to the owner based on general patterns.
  if (!isOwner) {
    privateNote = undefined;
  }

  // Apply redaction to labels using the imported redactLabelValue
  const redactedLabels = (
    await Promise.all(
      drive.labels.map(
        async (label) => await redactLabelValue(orgId, label, userId)
      )
    )
  ).filter((label): label is string => label !== null);

  return {
    ...drive,
    private_note: privateNote,
    labels: redactedLabels,
    permission_previews: permissionPreviews,
  };
}

// Request params for GetDriveHandler
interface GetDriveParams extends OrgIdParams {
  drive_id: string; // This is the drive ID passed in the URL path, not the tenant org_id
}

export async function getDriveHandler(
  request: FastifyRequest<{ Params: GetDriveParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const requesterApiKey = await authenticateRequest(
      request,
      "drive",
      request.params.org_id as DriveID
    );
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const requestedDriveId = request.params.drive_id as DriveID;
    const orgId = request.params.org_id as DriveID; // The tenant's drive ID
    const requesterUserId = requesterApiKey.user_id;

    const isOwner = requesterUserId === (await getDriveOwnerId(orgId));

    let drive: Drive | null = null;
    try {
      const result = await db.queryDrive(
        orgId,
        "SELECT * FROM drives WHERE id = ?",
        [requestedDriveId]
      );
      if (result && result.length > 0) {
        drive = result[0] as Drive;
      }
    } catch (e) {
      request.log.error(
        `Error querying drive ${requestedDriveId} for org ${orgId}:`,
        e
      );
      return reply.status(500).send(
        createApiResponse(undefined, {
          code: 500,
          message: "Internal server error querying drive",
        })
      );
    }

    if (!drive) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Drive not found",
        })
      );
    }

    // Use the imported canUserAccessSystemPermission to check if the user can view this drive record.
    const canViewDrive = await canUserAccessSystemPermission(
      requestedDriveId, // Resource ID (e.g., "DriveID_xyz")
      requesterUserId,
      orgId
    );

    if (!canViewDrive) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const driveFE = await castDriveToFE(drive, requesterUserId, orgId);
    return reply.status(200).send(createApiResponse(driveFE));
  } catch (error) {
    request.log.error("Error in getDriveHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function listDrivesHandler(
  request: FastifyRequest<{ Body: IRequestListDrives; Params: OrgIdParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const requesterApiKey = await authenticateRequest(
      request,
      "drive",
      request.params.org_id as DriveID
    );
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const orgId = request.params.org_id as DriveID;
    const requesterUserId = requesterApiKey.user_id;

    // Check if the user has VIEW permission on the DRIVES table
    const canViewDrivesTable = await checkSystemPermissions(
      `TABLE_${SystemTableValueEnum.DRIVES}` as SystemTableValueEnum,
      requesterUserId,
      orgId
    );

    // If not owner and does not have VIEW permission on the DRIVES table
    const isOwner = requesterUserId === (await getDriveOwnerId(orgId));
    if (!isOwner && !canViewDrivesTable.includes(SystemPermissionType.VIEW)) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const {
      filters,
      page_size = 50,
      direction = "DESC",
      cursor,
    } = request.body;

    // Validate request body (simplified, more robust validation needed)
    if (page_size <= 0 || page_size > 1000) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Page size must be between 1 and 1000",
        })
      );
    }
    if (filters && filters.length > 256) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Filters must be 256 characters or less",
        })
      );
    }
    if (cursor && cursor.length > 256) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Cursor must be 256 characters or less",
        })
      );
    }

    let query = "SELECT * FROM drives";
    const params: any[] = [];
    const whereClauses: string[] = [];

    if (filters) {
      // TODO: REDACT Implement more sophisticated filtering logic based on the Rust `filters` implementation.
      // For now, a basic name filter.
      whereClauses.push("name LIKE ?");
      params.push(`%${filters}%`);
    }

    if (whereClauses.length > 0) {
      query += " WHERE " + whereClauses.join(" AND ");
    }

    query += ` ORDER BY created_at ${direction === "ASC" ? "ASC" : "DESC"}`;

    // Implement cursor-based pagination
    // SQLite does not have native cursor support like some other databases.
    // We'll use LIMIT and OFFSET or WHERE clauses with the last seen value for efficient pagination.
    let offset = 0;
    if (cursor) {
      // Assuming cursor is an index or a timestamp for now, adjust based on actual data
      // For simplicity, treating cursor as an offset here.
      // In a real scenario, you'd want to use the last item's `created_at` or `id` for `keyset pagination`.
      offset = parseInt(cursor, 10);
      if (isNaN(offset) || offset < 0) {
        return reply.status(400).send(
          createApiResponse(undefined, {
            code: 400,
            message: "Invalid cursor format for pagination.",
          })
        );
      }
    }
    query += ` LIMIT ? OFFSET ?`;
    params.push(page_size);
    params.push(offset);

    const totalCountResult = await db.queryDrive(
      orgId,
      `SELECT COUNT(*) as count FROM drives` +
        (whereClauses.length > 0 ? " WHERE " + whereClauses.join(" AND ") : ""),
      params.slice(0, whereClauses.length) // Only pass filter params for count
    );
    const total = totalCountResult[0].count;

    const drives = await db.queryDrive(orgId, query, params);

    // Apply casting and redaction for each drive
    const driveFEs: DriveFE[] = await Promise.all(
      drives.map((d: Drive) => castDriveToFE(d, requesterUserId, orgId))
    );

    const nextCursor =
      offset + drives.length < total
        ? (offset + drives.length).toString()
        : null;

    return reply.status(200).send(
      createApiResponse({
        items: driveFEs,
        page_size: driveFEs.length,
        total: total,
        direction: direction,
        cursor: nextCursor,
      } as IPaginatedResponse<DriveFE>) // Cast to the correct paginated response type
    );
  } catch (error) {
    request.log.error("Error in listDrivesHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function createDriveHandler(
  request: FastifyRequest<{ Body: IRequestCreateDrive; Params: OrgIdParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const requesterApiKey = await authenticateRequest(
      request,
      "drive",
      request.params.org_id as DriveID
    );
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const orgId = request.params.org_id as DriveID;
    const requesterUserId = requesterApiKey.user_id;

    // Check for CREATE permission on the DRIVES table
    const hasCreatePermission = await checkSystemPermissions(
      `TABLE_${SystemTableValueEnum.DRIVES}` as SystemTableValueEnum,
      requesterUserId,
      orgId
    );

    const isOwner = requesterUserId === (await getDriveOwnerId(orgId));
    if (
      !isOwner &&
      !hasCreatePermission.includes(SystemPermissionType.CREATE)
    ) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const {
      id,
      name,
      icp_principal,
      public_note,
      private_note,
      endpoint_url,
      external_id,
      external_payload,
    } = request.body;

    // Validate request body
    // TODO: VALIDATE Check if valid icp principal
    if (id && !id.startsWith(IDPrefixEnum.Drive)) {
      // Basic UUID check
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Invalid Drive ID format",
        })
      );
    }
    if (!name || name.length > 256) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Name is required and must be less than 256 characters",
        })
      );
    }
    if (!icp_principal) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "ICP Principal is required",
        })
      );
    }
    // TODO: VALIDATE Add more specific validation for icp_principal format if needed
    if (public_note && public_note.length > 8192) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Public note too long",
        })
      );
    }
    if (private_note && private_note.length > 8192) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Private note too long",
        })
      );
    }
    if (endpoint_url && endpoint_url.length > 4096) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Endpoint URL too long",
        })
      );
    }
    if (external_id && external_id.length > 256) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "External ID too long",
        })
      );
    }
    if (external_payload && external_payload.length > 8192) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "External payload too long",
        })
      );
    }

    const driveId: DriveID = (id ||
      `${IDPrefixEnum.Drive}${uuidv4()}`) as DriveID;
    const createdAt = Date.now(); // Milliseconds

    const newDrive: Drive = {
      id: driveId,
      name: name,
      icp_principal: icp_principal as ICPPrincipalString,
      public_note: public_note,
      private_note: private_note,
      endpoint_url: (endpoint_url ||
        `https://${icp_principal}.icp0.io`) as URLEndpoint, // Default endpoint
      last_indexed_ms: undefined,
      labels: [], // No labels on creation by default
      external_id: external_id,
      external_payload: external_payload,
      created_at: createdAt,
    };

    await dbHelpers.transaction("drive", orgId, (database) => {
      // Insert into drives table
      const stmt = database.prepare(
        `INSERT INTO drives (id, name, icp_principal, public_note, private_note, endpoint_url, last_indexed_ms, created_at, external_id, external_payload)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );
      stmt.run(
        newDrive.id,
        newDrive.name,
        newDrive.icp_principal,
        newDrive.public_note,
        newDrive.private_note,
        newDrive.endpoint_url,
        newDrive.last_indexed_ms,
        newDrive.created_at,
        newDrive.external_id,
        newDrive.external_payload
      );

      // TODO: VALIDATE Update external ID mapping in about_drive table (Rust's `update_external_id_mapping`)
      // This part assumes that `EXTERNAL_ID_MAPPINGS` in Rust is a separate stable structure.
      // In SQLite, this would typically be handled as part of the `drives` table or a separate `external_id_mappings` table.
      // For now, we'll assume `external_id` is stored directly in the `drives` table.
      // If a separate mapping table is needed, create it and add inserts here.
    });

    const driveFE = await castDriveToFE(newDrive, requesterUserId, orgId);
    return reply.status(200).send(createApiResponse(driveFE));
  } catch (error) {
    request.log.error("Error in createDriveHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function updateDriveHandler(
  request: FastifyRequest<{ Body: IRequestUpdateDrive; Params: OrgIdParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const requesterApiKey = await authenticateRequest(
      request,
      "drive",
      request.params.org_id as DriveID
    );
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const orgId = request.params.org_id as DriveID;
    const requesterUserId = requesterApiKey.user_id;

    const {
      id,
      name,
      public_note,
      private_note,
      endpoint_url,
      external_id,
      external_payload,
    } = request.body;

    // Validate request body
    if (!id || !id.startsWith(IDPrefixEnum.Drive)) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Drive ID is required and must start with DriveID_",
        })
      );
    }
    if (name && name.length > 256) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Name must be less than 256 characters",
        })
      );
    }
    if (public_note && public_note.length > 8192) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Public note too long",
        })
      );
    }
    if (private_note && private_note.length > 8192) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Private note too long",
        })
      );
    }
    if (endpoint_url && endpoint_url.length > 4096) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Endpoint URL too long",
        })
      );
    }
    if (external_id && external_id.length > 256) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "External ID too long",
        })
      );
    }
    if (external_payload && external_payload.length > 8192) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "External payload too long",
        })
      );
    }

    const existingDrives = await db.queryDrive(
      orgId,
      "SELECT * FROM drives WHERE id = ?",
      [id]
    );

    if (!existingDrives || existingDrives.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Drive not found",
        })
      );
    }

    const currentDrive = existingDrives[0] as Drive;

    // Check for EDIT permission on this specific drive record
    const hasEditPermission = await checkSystemPermissions(
      id, // SystemResourceID for the record is the full ID like DriveID_xyz
      requesterUserId,
      orgId
    );

    const isOwner = requesterUserId === (await getDriveOwnerId(orgId));
    if (!isOwner && !hasEditPermission.includes(SystemPermissionType.EDIT)) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    // Build update query dynamically
    const updates: string[] = [];
    const values: any[] = [];
    let oldExternalId: string | undefined;

    if (name !== undefined) {
      updates.push("name = ?");
      values.push(name);
    }
    if (public_note !== undefined) {
      updates.push("public_note = ?");
      values.push(public_note);
    }
    if (private_note !== undefined) {
      updates.push("private_note = ?");
      values.push(private_note);
    }
    if (endpoint_url !== undefined) {
      updates.push("endpoint_url = ?");
      values.push(endpoint_url);
    }
    if (external_id !== undefined) {
      oldExternalId = currentDrive.external_id; // Capture old external_id
      updates.push("external_id = ?");
      values.push(external_id);
    }
    if (external_payload !== undefined) {
      updates.push("external_payload = ?");
      values.push(external_payload);
    }

    if (updates.length === 0) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "No fields to update",
        })
      );
    }

    values.push(id); // The ID for the WHERE clause

    await dbHelpers.transaction("drive", orgId, (database) => {
      const stmt = database.prepare(
        `UPDATE drives SET ${updates.join(", ")} WHERE id = ?`
      );
      stmt.run(...values);

      // TODO: VALIDATE Handle update to external ID mapping, similar to Rust's `update_external_id_mapping`
      // This would involve updating a separate `external_id_mappings` table if one exists.
      // If `external_id` is just a field on `drives` table, the update query above handles it.
      // If the old_external_id was present and new one is different or null, you might need to remove old entries from a mapping table.
    });

    const updatedDrives = await db.queryDrive(
      orgId,
      "SELECT * FROM drives WHERE id = ?",
      [id]
    );

    const updatedDrive = updatedDrives[0] as Drive;
    const updatedDriveFE = await castDriveToFE(
      updatedDrive,
      requesterUserId,
      orgId
    );

    return reply.status(200).send(createApiResponse(updatedDriveFE));
  } catch (error) {
    request.log.error("Error in updateDriveHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function deleteDriveHandler(
  request: FastifyRequest<{ Body: IRequestDeleteDrive; Params: OrgIdParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const requesterApiKey = await authenticateRequest(
      request,
      "drive",
      request.params.org_id as DriveID
    );
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const orgId = request.params.org_id as DriveID;
    const requesterUserId = requesterApiKey.user_id;

    const { id } = request.body;

    // Validate request body
    if (!id || !id.startsWith(IDPrefixEnum.Drive)) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Drive ID is required and must start with DriveID_",
        })
      );
    }

    const existingDrives = await db.queryDrive(
      orgId,
      "SELECT * FROM drives WHERE id = ?",
      [id]
    );

    if (!existingDrives || existingDrives.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Drive not found",
        })
      );
    }

    // Check for DELETE permission on this specific drive record
    const hasDeletePermission = await checkSystemPermissions(
      id, // SystemResourceID for the record is the full ID like DriveID_xyz
      requesterUserId,
      orgId
    );

    const isOwner = requesterUserId === (await getDriveOwnerId(orgId));
    if (
      !isOwner &&
      !hasDeletePermission.includes(SystemPermissionType.DELETE)
    ) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    await dbHelpers.transaction("drive", orgId, (database) => {
      const stmt = database.prepare("DELETE FROM drives WHERE id = ?");
      stmt.run(id);

      // TODO: VALIDATE Handle deletion from external ID mapping if a separate table exists
      // If `external_id` was stored in a separate mapping table, delete corresponding entries here.
    });

    const deletedData: IResponseDeleteDrive["ok"]["data"] = {
      id: id as DriveID,
      deleted: true,
    };

    return reply.status(200).send(createApiResponse(deletedData));
  } catch (error) {
    request.log.error("Error in deleteDriveHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}
