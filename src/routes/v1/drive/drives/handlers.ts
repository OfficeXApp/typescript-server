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
import { OrgIdParams } from "../../types";

// TODO: Implement getOwnerId() to retrieve the owner_id for permission checks.
// This will likely involve querying the `about_drive` table in the drive's database.
async function getOwnerId(driveId: DriveID): Promise<UserID> {
  try {
    const result = await db.queryDrive(
      driveId,
      "SELECT owner_id FROM about_drive LIMIT 1"
    );
    if (result && result.length > 0) {
      return result[0].owner_id as UserID;
    }
    // Fallback or throw an error if owner_id cannot be determined
    return "UserID_UNKNOWN"; // TODO: Replace with a more robust error handling
  } catch (error) {
    console.error(`Error getting owner ID for drive ${driveId}:`, error);
    throw new Error(`Unable to determine drive owner for ${driveId}`);
  }
}

// TODO: Implement checkSystemPermissions equivalent.
// This function will check permissions against the system_permissions table.
// It should return an array of SystemPermissionType that the user has for the given resource.
async function checkSystemPermissions(
  driveId: DriveID,
  resourceId: string, // Corresponds to SystemResourceID in Rust
  granteeId: UserID // Corresponds to PermissionGranteeID::User in Rust
): Promise<SystemPermissionType[]> {
  // Placeholder implementation: Assume all permissions if owner, otherwise none.
  // In a real scenario, this would involve complex database queries joining permissions_system and potentially groups/contacts tables.

  // Check if the grantee is the owner of the drive
  const ownerId = await getOwnerId(driveId);
  if (granteeId === ownerId) {
    return [
      SystemPermissionType.CREATE,
      SystemPermissionType.EDIT,
      SystemPermissionType.DELETE,
      SystemPermissionType.VIEW,
      SystemPermissionType.INVITE,
    ];
  }

  // TODO: Implement actual database logic to query permissions_system table
  // Example query structure (highly simplified, needs to handle resource_type, grantee_type, expiry, etc.):
  const permissionsFromDb = await db.queryDrive(
    driveId,
    `
    SELECT T2.permission_type
    FROM permissions_system AS T1
    JOIN permissions_system_types AS T2 ON T1.id = T2.permission_id
    WHERE T1.resource_identifier = ?
    AND T1.grantee_id = ?
    AND T1.expiry_date_ms > ?
    AND T1.begin_date_ms <= ?;
    `,
    [resourceId, granteeId, Date.now(), Date.now()]
  );

  return permissionsFromDb.map(
    (p: any) => p.permission_type as SystemPermissionType
  );
}

// TODO: Implement redactLabel equivalent.
// This function should filter labels based on user permissions, similar to the Rust implementation.
function redactLabel(
  labelValue: string,
  userId: UserID // This is not used but kept for API compatibility. In Rust it checked ownership of the label.
): string | undefined {
  // Placeholder: For now, no labels are redacted. Implement actual logic based on label permissions.
  // In a real application, you might have label privacy settings that determine if a label
  // should be visible to a non-owner/non-admin.
  return labelValue;
}

// Extend Drive with FE properties for API responses
interface DriveFEWithRedaction extends Drive {
  permission_previews: SystemPermissionType[];
  // Include other FE-specific fields if necessary, e.g., clipped_directory_path
}

// Helper to cast Drive to DriveFE and apply redaction logic
function castDriveToFE(drive: Drive, userId: UserID): DriveFEWithRedaction {
  const isOwner = drive.icp_principal === userId.replace("UserID_", ""); // Crude check, refine if needed
  // TODO: Replace with actual permission check using checkSystemPermissions
  const permissionPreviews = isOwner
    ? [
        SystemPermissionType.CREATE,
        SystemPermissionType.EDIT,
        SystemPermissionType.DELETE,
        SystemPermissionType.VIEW,
        SystemPermissionType.INVITE,
      ]
    : [SystemPermissionType.VIEW]; // Default view for non-owners

  let privateNote = drive.private_note;
  if (!isOwner && !permissionPreviews.includes(SystemPermissionType.EDIT)) {
    privateNote = undefined; // Redact private_note if not owner and no edit permissions
  }

  const redactedLabels = isOwner
    ? drive.labels
    : drive.labels.filter((label) => redactLabel(label, userId)); // Apply redaction if necessary

  return {
    ...drive,
    private_note: privateNote,
    labels: redactedLabels,
    permission_previews: permissionPreviews,
  };
}

// Generic API Response Helper
function createApiResponse<T>(
  data?: T,
  error?: { code: number; message: string }
): ApiResponse<T> {
  return {
    status: error ? "error" : "success",
    data,
    error,
    timestamp: Date.now(),
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

    const isOwner = requesterApiKey.user_id === (await getOwnerId(orgId));

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

    // Permission checks
    const hasTablePermission = await checkSystemPermissions(
      orgId,
      SystemTableValueEnum.DRIVES,
      requesterApiKey.user_id
    );
    const hasRecordPermission = await checkSystemPermissions(
      orgId,
      `DriveID_${requestedDriveId}`, // TODO: Adjust resource ID format if different in DB
      requesterApiKey.user_id
    );

    if (
      !isOwner &&
      !(
        hasRecordPermission.includes(SystemPermissionType.VIEW) ||
        hasTablePermission.includes(SystemPermissionType.VIEW)
      )
    ) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const driveFE = castDriveToFE(drive, requesterApiKey.user_id);
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
    const isOwner = requesterApiKey.user_id === (await getOwnerId(orgId));

    const hasTablePermission = await checkSystemPermissions(
      orgId,
      SystemTableValueEnum.DRIVES,
      requesterApiKey.user_id
    );

    if (!isOwner && !hasTablePermission.includes(SystemPermissionType.VIEW)) {
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
      // TODO: Implement more sophisticated filtering logic based on the Rust `filters` implementation.
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

    const driveFEs: DriveFE[] = drives.map((d: Drive) =>
      castDriveToFE(d, requesterApiKey.user_id)
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
    const isOwner = requesterApiKey.user_id === (await getOwnerId(orgId));

    const hasPermission = await checkSystemPermissions(
      orgId,
      SystemTableValueEnum.DRIVES,
      requesterApiKey.user_id
    );

    if (!isOwner && !hasPermission.includes(SystemPermissionType.CREATE)) {
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
    // TODO: Check if valid icp principal
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
    // TODO: Add more specific validation for icp_principal format if needed
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

      // TODO: Update external ID mapping in about_drive table (Rust's `update_external_id_mapping`)
      // This part assumes that `EXTERNAL_ID_MAPPINGS` in Rust is a separate stable structure.
      // In SQLite, this would typically be handled as part of the `drives` table or a separate `external_id_mappings` table.
      // For now, we'll assume `external_id` is stored directly in the `drives` table.
      // If a separate mapping table is needed, create it and add inserts here.
    });

    const driveFE = castDriveToFE(newDrive, requesterApiKey.user_id);
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
    const isOwner = requesterApiKey.user_id === (await getOwnerId(orgId));

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

    // Permission check for updating
    const hasPermission = await checkSystemPermissions(
      orgId,
      `DriveID_${id}`, // TODO: Adjust resource ID format if different in DB
      requesterApiKey.user_id
    );

    if (!isOwner && !hasPermission.includes(SystemPermissionType.EDIT)) {
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

      // TODO: Handle update to external ID mapping, similar to Rust's `update_external_id_mapping`
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
    const updatedDriveFE = castDriveToFE(updatedDrive, requesterApiKey.user_id);

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
    const isOwner = requesterApiKey.user_id === (await getOwnerId(orgId));

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

    const currentDrive = existingDrives[0] as Drive;

    // Permission check for deleting
    const hasPermission = await checkSystemPermissions(
      orgId,
      `DriveID_${id}`, // TODO: Adjust resource ID format if different in DB
      requesterApiKey.user_id
    );

    if (!isOwner && !hasPermission.includes(SystemPermissionType.DELETE)) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    await dbHelpers.transaction("drive", orgId, (database) => {
      const stmt = database.prepare("DELETE FROM drives WHERE id = ?");
      stmt.run(id);

      // TODO: Handle deletion from external ID mapping if a separate table exists
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
