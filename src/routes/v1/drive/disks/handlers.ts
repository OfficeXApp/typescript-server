import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import {
  Disk,
  DiskFE,
  DiskTypeEnum,
  IDPrefixEnum,
  IRequestCreateDisk,
  IRequestDeleteDisk,
  IRequestGetDisk,
  IRequestListDisks,
  IRequestUpdateDisk,
  IResponseDeleteDisk,
  IPaginatedResponse,
  ApiResponse,
  UserID,
  DriveID,
  SystemPermissionType,
  SortDirection,
} from "@officexapp/types"; // Assuming these are correctly defined
import { db, dbHelpers } from "../../../../services/database";
import { authenticateRequest } from "../../../../services/auth";
import { getDriveOwnerId, OrgIdParams } from "../../types";

// --- Helper Types for Request Params ---

interface GetDiskParams extends OrgIdParams {
  disk_id: string; // Corresponds to DiskID
}

// --- Internal Helper Functions (Adapted from Rust) ---

// Helper function to validate request body for creating a disk
function validateCreateDiskRequest(body: IRequestCreateDisk): {
  valid: boolean;
  error?: string;
} {
  // TODO: Implement more comprehensive validation based on Rust's `CreateDiskRequestBody::validate_body`
  // Placeholder for now
  if (!body.name || body.name.length === 0 || body.name.length > 256) {
    return {
      valid: false,
      error: "Name is required and must be between 1 and 256 characters.",
    };
  }
  if (!body.disk_type) {
    return {
      valid: false,
      error: "Disk type is required.",
    };
  }

  // Rust's `validate_unclaimed_uuid` and `validate_uuid4_string_with_prefix` need to be replicated if `id` is provided
  if (body.id) {
    if (!body.id.startsWith(IDPrefixEnum.Disk)) {
      return {
        valid: false,
        error: `Disk ID must start with '${IDPrefixEnum.Disk}'.`,
      };
    }
    // TODO: Implement `validate_unclaimed_uuid` equivalent (check if ID already exists in DB)
  }

  // Validate notes
  if (body.public_note && body.public_note.length > 8192) {
    return {
      valid: false,
      error: "Public note must be 8,192 characters or less.",
    };
  }
  if (body.private_note && body.private_note.length > 8192) {
    return {
      valid: false,
      error: "Private note must be 8,192 characters or less.",
    };
  }

  // Validate auth_json based on disk_type, as in Rust's `validate_auth_json`
  if (
    (body.disk_type === DiskTypeEnum.AwsBucket ||
      body.disk_type === DiskTypeEnum.StorjWeb3) &&
    !body.auth_json
  ) {
    return {
      valid: false,
      error: `Auth JSON is required for disk type ${body.disk_type}.`,
    };
  }
  if (body.auth_json && body.auth_json.length > 8192) {
    return {
      valid: false,
      error: "Auth JSON must be 8,192 characters or less.",
    };
  }
  // TODO: Further parse and validate `auth_json` content based on `AwsBucketAuth` structure for AWS and Storj
  // For example, if DiskTypeEnum.AwsBucket, check if auth_json is valid JSON and contains expected fields.
  if (body.disk_type === DiskTypeEnum.AwsBucket && body.auth_json) {
    try {
      const auth = JSON.parse(body.auth_json);
      if (
        !auth.endpoint ||
        !auth.access_key ||
        !auth.secret_key ||
        !auth.bucket ||
        !auth.region
      ) {
        return {
          valid: false,
          error:
            "Auth JSON for AWS_BUCKET must contain endpoint, access_key, secret_key, bucket, and region.",
        };
      }
    } catch (e) {
      return { valid: false, error: "Auth JSON is not valid JSON." };
    }
  }

  // TODO: validate `external_id`, `external_payload`, `endpoint` based on Rust's `validate_external_id`, `validate_external_payload`, `validate_url`

  return { valid: true };
}

// Helper function to validate request body for updating a disk
function validateUpdateDiskRequest(body: IRequestUpdateDisk): {
  valid: boolean;
  error?: string;
} {
  // TODO: Implement comprehensive validation based on Rust's `UpdateDiskRequestBody::validate_body`
  if (!body.id || !body.id.startsWith(IDPrefixEnum.Disk)) {
    return {
      valid: false,
      error: `Disk ID must start with '${IDPrefixEnum.Disk}'.`,
    };
  }

  if (body.name !== undefined && body.name.length > 256) {
    return { valid: false, error: "Name must be less than 256 characters." };
  }

  if (body.public_note !== undefined && body.public_note.length > 8192) {
    return {
      valid: false,
      error: "Public note must be 8,192 characters or less.",
    };
  }
  if (body.private_note !== undefined && body.private_note.length > 8192) {
    return {
      valid: false,
      error: "Private note must be 8,192 characters or less.",
    };
  }
  if (body.auth_json !== undefined && body.auth_json.length > 8192) {
    return {
      valid: false,
      error: "Auth JSON must be 8,192 characters or less.",
    };
  }

  // TODO: Re-validate auth_json structure if provided, similar to create. Requires fetching existing disk type.
  // This will need to fetch the existing disk to know its `disk_type`.
  // For now, assume it's structurally valid if present.

  // TODO: validate `external_id`, `external_payload`, `endpoint`

  return { valid: true };
}

// Helper function to validate request body for deleting a disk
function validateDeleteDiskRequest(body: IRequestDeleteDisk): {
  valid: boolean;
  error?: string;
} {
  // TODO: Implement comprehensive validation based on Rust's `DeleteDiskRequest::validate_body`
  if (!body.id || !body.id.startsWith(IDPrefixEnum.Disk)) {
    return {
      valid: false,
      error: `Disk ID must start with '${IDPrefixEnum.Disk}'.`,
    };
  }
  return { valid: true };
}

// Helper function to create a standardized API response
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

// TODO: Implement `check_system_permissions` equivalent in TypeScript.
// This would involve querying the `permissions_system` table for the given resource and grantee.
// For now, this is a mock implementation.
async function checkSystemPermissions(
  orgId: DriveID,
  resourceId: string, // Corresponds to SystemResourceID in Rust
  granteeId: UserID // Corresponds to PermissionGranteeID::User in Rust
): Promise<Array<SystemPermissionType>> {
  // TODO: Implement actual database query to check permissions for the given resource and grantee.
  // For now, return a mock based on the `isOwner` logic.
  const ownerId = await getDriveOwnerId(orgId);
  if (granteeId === ownerId) {
    return [
      SystemPermissionType.CREATE,
      SystemPermissionType.EDIT,
      SystemPermissionType.DELETE,
      SystemPermissionType.VIEW,
      SystemPermissionType.INVITE,
    ];
  }
  // This is a placeholder; real permissions would be fetched from the 'permissions_system' table.
  if (resourceId.includes("Table_DISKS") || resourceId.includes("DiskID_")) {
    return [SystemPermissionType.VIEW];
  }
  return [];
}

// TODO: Implement `ensure_disk_root_and_trash_folder` equivalent
// This function would ensure that the root and trash folders for a disk exist in the `folders` table.
// It should interact with the database to create these records if they don't exist.
async function ensureDiskRootAndTrashFolder(
  orgId: DriveID,
  diskId: string,
  ownerId: UserID,
  diskType: DiskTypeEnum
): Promise<{ rootFolderId: string; trashFolderId: string }> {
  // TODO: Implement actual logic to check/create folders in `folders` table in the drive DB.
  // This will require functions to insert/query the 'folders' table.
  // The folder IDs will need to follow the `FolderID_` prefix convention.

  const now = Date.now();
  const rootFolderId = `${IDPrefixEnum.Folder}${uuidv4()}`;
  const trashFolderId = `${IDPrefixEnum.Folder}${uuidv4()}`;
  const driveId = orgId; // DRIVE_ID in Rust context

  // Mock folder records for now. In a real implementation, these would be inserted into the DB.
  const rootFolder: any = {
    id: rootFolderId,
    name: "Root",
    parent_folder_id: null,
    full_directory_path: `${diskId}::/`, // Rust's `format!("{}::/", disk_id.to_string())`
    created_by_user_id: ownerId,
    created_at: now,
    last_updated_at: now,
    last_updated_by_user_id: ownerId,
    disk_id: diskId,
    disk_type: diskType,
    is_deleted: 0,
    expires_at: -1,
    drive_id: driveId,
    has_sovereign_permissions: 1, // true in Rust
  };

  const trashFolder: any = {
    id: trashFolderId,
    name: "Trash",
    parent_folder_id: null, // In Rust, this was also None. If it should be a subfolder of root, adjust here.
    full_directory_path: `${diskId}::.trash/`, // Rust's `format!("{}::.trash/", disk_id.to_string())`
    created_by_user_id: ownerId,
    created_at: now,
    last_updated_at: now,
    last_updated_by_user_id: ownerId,
    disk_id: diskId,
    disk_type: diskType,
    is_deleted: 0,
    expires_at: -1,
    drive_id: driveId,
    has_sovereign_permissions: 1,
  };

  // Insert into DB using a transaction
  await dbHelpers.transaction("drive", orgId, (database) => {
    const insertFolderStmt = database.prepare(
      `INSERT INTO folders (id, name, parent_folder_id, full_directory_path, created_by_user_id, created_at, last_updated_at, last_updated_by_user_id, disk_id, disk_type, is_deleted, expires_at, drive_id, has_sovereign_permissions)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    );
    insertFolderStmt.run(
      rootFolder.id,
      rootFolder.name,
      rootFolder.parent_folder_id,
      rootFolder.full_directory_path,
      rootFolder.created_by_user_id,
      rootFolder.created_at,
      rootFolder.last_updated_at,
      rootFolder.last_updated_by_user_id,
      rootFolder.disk_id,
      rootFolder.disk_type,
      rootFolder.is_deleted,
      rootFolder.expires_at,
      rootFolder.drive_id,
      rootFolder.has_sovereign_permissions
    );
    insertFolderStmt.run(
      trashFolder.id,
      trashFolder.name,
      trashFolder.parent_folder_id,
      trashFolder.full_directory_path,
      trashFolder.created_by_user_id,
      trashFolder.created_at,
      trashFolder.last_updated_at,
      trashFolder.last_updated_by_user_id,
      trashFolder.disk_id,
      trashFolder.disk_type,
      trashFolder.is_deleted,
      trashFolder.expires_at,
      trashFolder.drive_id,
      trashFolder.has_sovereign_permissions
    );
  });

  return { rootFolderId, trashFolderId };
}

// TODO: Implement `update_external_id_mapping` equivalent
// This function would manage a mapping of external IDs to internal IDs.
// It's likely updating a separate table for external ID lookups.
async function updateExternalIdMapping(
  orgId: DriveID,
  oldExternalId: string | null | undefined,
  newExternalId: string | null | undefined,
  internalId: string
): Promise<void> {
  // TODO: Implement actual database interaction for external ID mapping.
  // This might involve a dedicated `external_id_mappings` table.
  console.log(
    `[TODO] Simulating external ID mapping update for org ${orgId}: ${oldExternalId} -> ${newExternalId} for internal ID ${internalId}`
  );
}

// TODO: Implement `mark_claimed_uuid` equivalent
// This function would mark a generated UUID as "claimed" to prevent reuse.
async function markClaimedUuid(orgId: DriveID, uuid: string): Promise<void> {
  // TODO: Implement logic to store claimed UUIDs, possibly in a dedicated table or a set.
  console.log(
    `[TODO] Simulating marking UUID as claimed for org ${orgId}: ${uuid}`
  );
}

// TODO: Implement `snapshot_prestate` and `snapshot_poststate` equivalent
// These are likely for state diffing/auditing. In a Fastify server, this might be
// less about canister state snapshots and more about database transaction logging or
// event sourcing if such a system is in place. For now, they are mocks.
function snapshotPrestate(): any {
  console.log("[TODO] Simulating pre-state snapshot.");
  return {}; // Placeholder for a pre-state snapshot
}

function snapshotPoststate(prestate: any, notes?: string): void {
  console.log(`[TODO] Simulating post-state snapshot. Notes: ${notes}`);
  // Compare with prestate, log diffs if necessary.
}

// --- Handlers ---

export async function getDiskHandler(
  request: FastifyRequest<{ Params: GetDiskParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id, disk_id } = request.params;

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    // Get the disk from the database
    const disks = await db.queryDrive(
      org_id,
      "SELECT * FROM disks WHERE id = ?",
      [disk_id]
    );

    if (!disks || disks.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Disk not found",
        })
      );
    }

    const disk = disks[0] as Disk;

    // Check permissions if not owner
    if (!isOwner) {
      const tablePermissions = await checkSystemPermissions(
        org_id,
        "Table_DISKS", // Corresponds to SystemTableEnum::Disks
        requesterApiKey.user_id
      );
      const resourcePermissions = await checkSystemPermissions(
        org_id,
        `DiskID_${disk_id}`, // Corresponds to SystemRecordIDEnum::Disk
        requesterApiKey.user_id
      );

      if (
        !resourcePermissions.includes(SystemPermissionType.VIEW) &&
        !tablePermissions.includes(SystemPermissionType.VIEW)
      ) {
        return reply
          .status(403)
          .send(
            createApiResponse(undefined, { code: 403, message: "Forbidden" })
          );
      }
    }

    // Cast to DiskFE and redact sensitive fields based on permissions
    const diskFE: DiskFE = {
      ...disk,
      permission_previews: isOwner
        ? [
            SystemPermissionType.CREATE,
            SystemPermissionType.EDIT,
            SystemPermissionType.DELETE,
            SystemPermissionType.VIEW,
            SystemPermissionType.INVITE,
          ]
        : await checkSystemPermissions(
            org_id,
            `DiskID_${disk_id}`,
            requesterApiKey.user_id
          ).then((perms) =>
            checkSystemPermissions(
              org_id,
              "Table_DISKS",
              requesterApiKey.user_id
            ).then((tablePerms) =>
              Array.from(new Set([...perms, ...tablePerms]))
            )
          ), // Combine record and table permissions
    };

    // Redaction logic, replicating Rust's DiskFE::redacted
    if (
      !isOwner &&
      !diskFE.permission_previews.includes(SystemPermissionType.EDIT)
    ) {
      diskFE.auth_json = undefined;
      diskFE.private_note = undefined;
    }
    // TODO: Implement label redaction logic (requiring `redact_label` equivalent)
    diskFE.labels = []; // Placeholder for labels after redaction

    return reply.status(200).send(createApiResponse(diskFE));
  } catch (error) {
    request.log.error("Error in getDiskHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function listDisksHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestListDisks }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const body = request.body;

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    // Validate request body
    const validation = validateListDisksRequest(body);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    const hasTablePermission = await checkSystemPermissions(
      org_id,
      "Table_DISKS",
      requesterApiKey.user_id
    ).then((perms) => perms.includes(SystemPermissionType.VIEW));

    // SQL query for listing disks. Filters and pagination need to be handled carefully.
    // The Rust code iterates through a StableVec and then filters. For SQLite,
    // we should leverage SQL's WHERE, ORDER BY, LIMIT, and OFFSET.
    let sql = `SELECT * FROM disks`;
    const params: any[] = [];
    const orderBy =
      body.direction === SortDirection.DESC
        ? "created_at DESC"
        : "created_at ASC"; // Assuming sort by created_at
    const pageSize = body.page_size || 50;
    let offset = 0;

    // Handle cursor for pagination. Rust's cursor is an index.
    if (body.cursor) {
      offset = parseInt(body.cursor, 10);
      if (isNaN(offset)) {
        return reply.status(400).send(
          createApiResponse(undefined, {
            code: 400,
            message: "Invalid cursor format",
          })
        );
      }
    }

    // TODO: Implement filtering based on `body.filters`. This requires parsing the filter string.
    // For now, assuming no filters are applied to the SQL directly.
    if (body.filters && body.filters.length > 0) {
      // This is a placeholder. Real filtering logic would be complex.
      // E.g., `sql += " WHERE name LIKE ?"`, `params.push(`%${body.filters}%`)`
      request.log.warn(
        `Filtering by '${body.filters}' is not yet implemented.`
      );
    }

    sql += ` ORDER BY ${orderBy} LIMIT ? OFFSET ?`;
    params.push(pageSize + 1, offset); // Fetch one extra to check for next cursor

    const allDisks = await db.queryDrive(org_id, sql, params);

    let itemsToReturn: Disk[] = [];
    let nextCursor: string | null = null;
    let totalCount = 0; // This will be the actual count in the DB if the user has permission

    if (allDisks.length > pageSize) {
      nextCursor = (offset + pageSize).toString();
      itemsToReturn = allDisks.slice(0, pageSize) as Disk[];
    } else {
      itemsToReturn = allDisks as Disk[];
    }

    // Get total count for the response (this part of Rust logic is tricky)
    if (isOwner || hasTablePermission) {
      // If owner or has table view permission, get actual total count
      const totalResult = await db.queryDrive(
        org_id,
        "SELECT COUNT(*) as count FROM disks"
      );
      totalCount = totalResult[0].count;
    } else {
      // If limited permissions, total count is an approximation.
      // Rust's logic `total_count_to_return = filtered_disks.len() + 1;` if next_cursor is Some
      totalCount = itemsToReturn.length;
      if (nextCursor) {
        totalCount += 1; // Indicate there might be more
      }
    }

    const redactedDisks = await Promise.all(
      itemsToReturn.map(async (disk) => {
        const diskFE: DiskFE = {
          ...disk,
          permission_previews: isOwner
            ? [
                SystemPermissionType.CREATE,
                SystemPermissionType.EDIT,
                SystemPermissionType.DELETE,
                SystemPermissionType.VIEW,
                SystemPermissionType.INVITE,
              ]
            : await checkSystemPermissions(
                org_id,
                `DiskID_${disk.id}`,
                requesterApiKey.user_id
              ).then((perms) =>
                checkSystemPermissions(
                  org_id,
                  "Table_DISKS",
                  requesterApiKey.user_id
                ).then((tablePerms) =>
                  Array.from(new Set([...perms, ...tablePerms]))
                )
              ),
        };
        // Apply redaction
        if (
          !isOwner &&
          !diskFE.permission_previews.includes(SystemPermissionType.EDIT)
        ) {
          diskFE.auth_json = undefined;
          diskFE.private_note = undefined;
        }
        diskFE.labels = []; // TODO: Implement label redaction
        return diskFE;
      })
    );

    const responseData: IPaginatedResponse<DiskFE> = {
      items: redactedDisks,
      page_size: itemsToReturn.length,
      total: totalCount,
      direction: body.direction || SortDirection.ASC, // Default as per Rust's default
      cursor: nextCursor,
    };

    return reply.status(200).send(createApiResponse(responseData));
  } catch (error) {
    request.log.error("Error in listDisksHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

// Helper for validating ListDisksRequestBody
function validateListDisksRequest(body: IRequestListDisks): {
  valid: boolean;
  error?: string;
} {
  if (body.filters && body.filters.length > 256) {
    return {
      valid: false,
      error: "Filters must be 256 characters or less",
    };
  }
  if (
    body.page_size !== undefined &&
    (body.page_size === 0 || body.page_size > 1000)
  ) {
    return {
      valid: false,
      error: "Page size must be between 1 and 1000",
    };
  }
  if (body.cursor && body.cursor.length > 256) {
    return { valid: false, error: "Cursor must be 256 characters or less" };
  }
  if (body.cursor && body.cursor.length > 256) {
    return { valid: false, error: "Cursor must be 256 characters or less" };
  }
  return { valid: true };
}

export async function createDiskHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestCreateDisk }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const body = request.body;

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    // Validate request body
    const validation = validateCreateDiskRequest(body);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    // Check create permission if not owner
    if (!isOwner) {
      const permissions = await checkSystemPermissions(
        org_id,
        "Table_DISKS",
        requesterApiKey.user_id
      );
      if (!permissions.includes(SystemPermissionType.CREATE)) {
        return reply
          .status(403)
          .send(
            createApiResponse(undefined, { code: 403, message: "Forbidden" })
          );
      }
    }

    const prestate = snapshotPrestate(); // For state diffing/auditing

    const diskId = (body.id || `${IDPrefixEnum.Disk}${uuidv4()}`) as Disk["id"];

    const { rootFolderId, trashFolderId } = await ensureDiskRootAndTrashFolder(
      org_id,
      diskId,
      requesterApiKey.user_id,
      body.disk_type
    );

    const newDisk: Disk = {
      id: diskId,
      name: body.name,
      disk_type: body.disk_type,
      private_note: body.private_note,
      public_note: body.public_note,
      auth_json: body.auth_json,
      labels: [], // Labels are handled separately, Rust had vec![] initially
      created_at: Date.now(),
      root_folder: rootFolderId,
      trash_folder: trashFolderId,
      external_id: body.external_id,
      external_payload: body.external_payload,
      endpoint: body.endpoint,
    };

    // Store the disk in the database
    await dbHelpers.transaction("drive", org_id, (database) => {
      const stmt = database.prepare(
        `INSERT INTO disks (id, name, disk_type, private_note, public_note, auth_json, labels, created_at, root_folder_id, trash_folder_id, external_id, external_payload, endpoint)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );
      stmt.run(
        newDisk.id,
        newDisk.name,
        newDisk.disk_type,
        newDisk.private_note || null,
        newDisk.public_note || null,
        newDisk.auth_json || null,
        JSON.stringify(newDisk.labels), // Store labels as JSON string
        newDisk.created_at,
        newDisk.root_folder,
        newDisk.trash_folder,
        newDisk.external_id || null,
        newDisk.external_payload || null,
        newDisk.endpoint || null
      );
    });

    await updateExternalIdMapping(
      org_id,
      undefined, // Old external ID is undefined for creation
      newDisk.external_id,
      newDisk.id
    );
    await markClaimedUuid(org_id, newDisk.id);

    snapshotPoststate(
      prestate,
      `${requesterApiKey.user_id}: Create Disk ${newDisk.id}`
    );

    const diskFE: DiskFE = {
      ...newDisk,
      permission_previews: [
        SystemPermissionType.CREATE,
        SystemPermissionType.EDIT,
        SystemPermissionType.DELETE,
        SystemPermissionType.VIEW,
        SystemPermissionType.INVITE,
      ],
    };
    // Redaction logic, replicating Rust's DiskFE::redacted
    if (
      !isOwner &&
      !diskFE.permission_previews.includes(SystemPermissionType.EDIT)
    ) {
      diskFE.auth_json = undefined;
      diskFE.private_note = undefined;
    }
    diskFE.labels = []; // Placeholder for labels after redaction

    return reply.status(200).send(createApiResponse(diskFE));
  } catch (error) {
    request.log.error("Error in createDiskHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function updateDiskHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestUpdateDisk }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const body = request.body;

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    // Validate request body
    const validation = validateUpdateDiskRequest(body);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    const diskId = body.id;

    // Get existing disk
    const existingDisks = await db.queryDrive(
      org_id,
      "SELECT * FROM disks WHERE id = ?",
      [diskId]
    );

    if (!existingDisks || existingDisks.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Disk not found",
        })
      );
    }
    const existingDisk = existingDisks[0] as Disk;

    // Check update permission if not owner
    if (!isOwner) {
      const tablePermissions = await checkSystemPermissions(
        org_id,
        "Table_DISKS",
        requesterApiKey.user_id
      );
      const resourcePermissions = await checkSystemPermissions(
        org_id,
        `DiskID_${diskId}`,
        requesterApiKey.user_id
      );

      if (
        !resourcePermissions.includes(SystemPermissionType.EDIT) &&
        !tablePermissions.includes(SystemPermissionType.EDIT)
      ) {
        return reply
          .status(403)
          .send(
            createApiResponse(undefined, { code: 403, message: "Forbidden" })
          );
      }
    }

    const prestate = snapshotPrestate(); // For state diffing/auditing

    const updates: string[] = [];
    const values: any[] = [];

    if (body.name !== undefined) {
      updates.push("name = ?");
      values.push(body.name);
    }
    if (body.public_note !== undefined) {
      updates.push("public_note = ?");
      values.push(body.public_note);
    }
    if (body.private_note !== undefined) {
      updates.push("private_note = ?");
      values.push(body.private_note);
    }
    if (body.auth_json !== undefined) {
      // TODO: Re-validate auth_json based on `existingDisk.disk_type`
      updates.push("auth_json = ?");
      values.push(body.auth_json);
    }
    if (body.external_id !== undefined) {
      updates.push("external_id = ?");
      values.push(body.external_id);
      await updateExternalIdMapping(
        org_id,
        existingDisk.external_id,
        body.external_id,
        diskId
      );
    }
    if (body.external_payload !== undefined) {
      updates.push("external_payload = ?");
      values.push(body.external_payload);
    }
    if (body.endpoint !== undefined) {
      updates.push("endpoint = ?");
      values.push(body.endpoint);
    }

    if (updates.length === 0) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "No fields to update",
        })
      );
    }

    values.push(diskId);

    // Update the disk in the database
    await dbHelpers.transaction("drive", org_id, (database) => {
      const stmt = database.prepare(
        `UPDATE disks SET ${updates.join(", ")} WHERE id = ?`
      );
      stmt.run(...values);
    });

    snapshotPoststate(
      prestate,
      `${requesterApiKey.user_id}: Update Disk ${diskId}`
    );

    // Fetch the updated disk to return in the response
    const updatedDisks = await db.queryDrive(
      org_id,
      "SELECT * FROM disks WHERE id = ?",
      [diskId]
    );
    const updatedDisk = updatedDisks[0] as Disk;

    const diskFE: DiskFE = {
      ...updatedDisk,
      permission_previews: isOwner
        ? [
            SystemPermissionType.CREATE,
            SystemPermissionType.EDIT,
            SystemPermissionType.DELETE,
            SystemPermissionType.VIEW,
            SystemPermissionType.INVITE,
          ]
        : await checkSystemPermissions(
            org_id,
            `DiskID_${diskId}`,
            requesterApiKey.user_id
          ).then((perms) =>
            checkSystemPermissions(
              org_id,
              "Table_DISKS",
              requesterApiKey.user_id
            ).then((tablePerms) =>
              Array.from(new Set([...perms, ...tablePerms]))
            )
          ),
    };
    // Redaction logic
    if (
      !isOwner &&
      !diskFE.permission_previews.includes(SystemPermissionType.EDIT)
    ) {
      diskFE.auth_json = undefined;
      diskFE.private_note = undefined;
    }
    diskFE.labels = []; // Placeholder for labels after redaction

    return reply.status(200).send(createApiResponse(diskFE));
  } catch (error) {
    request.log.error("Error in updateDiskHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function deleteDiskHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestDeleteDisk }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const body = request.body;

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    const prestate = snapshotPrestate(); // For state diffing/auditing

    // Validate request body
    const validation = validateDeleteDiskRequest(body);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    const diskId = body.id;

    // Check delete permission if not owner
    if (!isOwner) {
      const tablePermissions = await checkSystemPermissions(
        org_id,
        "Table_DISKS",
        requesterApiKey.user_id
      );
      const resourcePermissions = await checkSystemPermissions(
        org_id,
        `DiskID_${diskId}`,
        requesterApiKey.user_id
      );

      if (
        !resourcePermissions.includes(SystemPermissionType.DELETE) &&
        !tablePermissions.includes(SystemPermissionType.DELETE)
      ) {
        return reply
          .status(403)
          .send(
            createApiResponse(undefined, { code: 403, message: "Forbidden" })
          );
      }
    }

    // Get disk for external ID cleanup before deleting
    const disksToDelete = await db.queryDrive(
      org_id,
      "SELECT external_id FROM disks WHERE id = ?",
      [diskId]
    );
    const externalIdToDelete =
      disksToDelete.length > 0 ? disksToDelete[0].external_id : null;

    // Delete the disk from the database
    await dbHelpers.transaction("drive", org_id, (database) => {
      const stmt = database.prepare("DELETE FROM disks WHERE id = ?");
      stmt.run(diskId);
    });

    if (externalIdToDelete) {
      await updateExternalIdMapping(
        org_id,
        externalIdToDelete,
        undefined, // New external ID is undefined for deletion
        diskId
      );
    }

    snapshotPoststate(
      prestate,
      `${requesterApiKey.user_id}: Delete Disk ${diskId}`
    );

    const deletedData: IResponseDeleteDisk["ok"]["data"] = {
      id: diskId,
      deleted: true,
    };

    return reply.status(200).send(createApiResponse(deletedData));
  } catch (error) {
    request.log.error("Error in deleteDiskHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}
