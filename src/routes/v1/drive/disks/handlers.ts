// typescript-server/src/routes/v1/drive/disks/handlers.ts
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
  SystemResourceID,
  SystemTableValueEnum,
  IRequestListDirectory,
  IResponseListDirectory,
  DirectoryPermissionType,
  DirectoryResourceID,
  FilePathBreadcrumb,
  FileRecord,
  FolderRecord,
  FolderRecordFE,
  FileRecordFE,
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import { authenticateRequest } from "../../../../services/auth";
import { createApiResponse, getDriveOwnerId } from "../../types";
import {
  checkSystemPermissions as checkSystemPermissionsService,
  canUserAccessSystemPermission as canUserAccessSystemPermissionService,
  redactLabelValue,
} from "../../../../services/permissions/system";
import {
  checkDirectoryPermissions,
  deriveBreadcrumbVisibilityPreviews,
} from "../../../../services/permissions/directory";
import { clipDirectoryPath } from "../../../../services/directory/internals";
import {
  claimUUID,
  isUUIDClaimed,
  updateExternalIDMapping,
} from "../../../../services/external";
import Database from "better-sqlite3";

// --- Helper Types for Request Params ---

interface GetDiskParams extends OrgIdParams {
  disk_id: string; // Corresponds to DiskID
}

interface OrgIdParams {
  org_id: DriveID;
}

// Helper function to validate request body for creating a disk
async function validateCreateDiskRequest(
  body: IRequestCreateDisk,
  orgID: DriveID
): Promise<{
  valid: boolean;
  error?: string;
}> {
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
    // Check if the provided ID is already claimed (Rust's `validate_unclaimed_uuid`)
    const alreadyClaimed = await isUUIDClaimed(orgID, body.id);
    if (alreadyClaimed) {
      return {
        valid: false,
        error: `Provided Disk ID '${body.id}' is already claimed.`,
      };
    }
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
  if (body.external_id && body.external_id.length > 256) {
    return {
      valid: false,
      error: "External ID must be 256 characters or less.",
    };
  }
  if (body.external_payload && body.external_payload.length > 8192) {
    return {
      valid: false,
      error: "External payload must be 8,192 characters or less.",
    };
  }
  if (body.endpoint && body.endpoint.length > 2048) {
    return {
      valid: false,
      error: "Endpoint must be 2048 characters or less.",
    };
  }

  // mark it claimed
  if (body.id) {
    await claimUUID(orgID, body.id);
  }

  return { valid: true };
}

// Helper function to validate request body for updating a disk
async function validateUpdateDiskRequest(
  body: IRequestUpdateDisk,
  orgID: DriveID
): Promise<{
  valid: boolean;
  error?: string;
}> {
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

  // Re-validate auth_json structure if provided, similar to create. Requires fetching existing disk type.
  // This will need to fetch the existing disk to know its `disk_type`.
  const disks = await db.queryDrive(orgID, "SELECT * FROM disks WHERE id = ?", [
    body.id,
  ]);

  if (!disks || disks.length === 0) {
    return {
      valid: false,
      error: "Disk not found.",
    };
  }

  const disk = disks[0];

  if (
    (disk.disk_type === DiskTypeEnum.AwsBucket && body.auth_json) ||
    (disk.disk_type === DiskTypeEnum.StorjWeb3 && body.auth_json)
  ) {
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

  if (body.external_id && body.external_id.length > 256) {
    return {
      valid: false,
      error: "External ID must be 256 characters or less.",
    };
  }
  if (body.external_payload && body.external_payload.length > 8192) {
    return {
      valid: false,
      error: "External payload must be 8,192 characters or less.",
    };
  }
  if (body.endpoint && body.endpoint.length > 2048) {
    return {
      valid: false,
      error: "Endpoint must be 2048 characters or less.",
    };
  }

  return { valid: true };
}

// Helper function to validate request body for deleting a disk
function validateDeleteDiskRequest(body: IRequestDeleteDisk): {
  valid: boolean;
  error?: string;
} {
  if (!body.id || !body.id.startsWith(IDPrefixEnum.Disk)) {
    return {
      valid: false,
      error: `Disk ID must start with '${IDPrefixEnum.Disk}'.`,
    };
  }
  return { valid: true };
}

// TODO: SNAPSHOT: Implement `snapshot_prestate` and `snapshot_poststate` equivalent
// These are likely for state diffing/auditing. In a Fastify server, this might be
// less about canister state snapshots and more about database transaction logging or
// event sourcing if such a system is in place. For now, they are mocks.
function snapshotPrestate(): any {
  console.log("[TODO: SNAPSHOT] Simulating pre-state snapshot.");
  return {}; // Placeholder for a pre-state snapshot
}

function snapshotPoststate(prestate: any, notes?: string): void {
  console.log(
    `[TODO: SNAPSHOT] Simulating post-state snapshot. Notes: ${notes}`
  );
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

    const canAccessDisk = await canUserAccessSystemPermissionService(
      `${disk_id}` as SystemResourceID, // Construct SystemResourceID for record
      requesterApiKey.user_id,
      org_id
    );

    if (!canAccessDisk && !isOwner) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    // Determine permission_previews using the actual permission service
    const permissionPreviews = isOwner
      ? [
          SystemPermissionType.CREATE,
          SystemPermissionType.EDIT,
          SystemPermissionType.DELETE,
          SystemPermissionType.VIEW,
          SystemPermissionType.INVITE,
        ]
      : await Promise.resolve().then(async () => {
          // Use Promise.resolve().then for async operations
          const recordPermissions = await checkSystemPermissionsService(
            `${disk_id}` as SystemResourceID,
            requesterApiKey.user_id,
            org_id
          );
          const tablePermissions = await checkSystemPermissionsService(
            `TABLE_${SystemTableValueEnum.DISKS}` as SystemResourceID,
            requesterApiKey.user_id,
            org_id
          );
          return Array.from(
            new Set([...recordPermissions, ...tablePermissions])
          );
        });

    // Cast to DiskFE and redact sensitive fields based on permissions
    const diskFE: DiskFE = {
      ...disk,
      permission_previews: permissionPreviews,
    };

    // Redaction logic, replicating Rust's DiskFE::redacted
    if (
      !isOwner &&
      !diskFE.permission_previews.includes(SystemPermissionType.EDIT)
    ) {
      diskFE.auth_json = undefined;
      diskFE.private_note = undefined;
    }
    const diskLabelsRaw = await db.queryDrive(
      org_id,
      `SELECT T2.value FROM disk_labels AS T1 JOIN labels AS T2 ON T1.label_id = T2.id WHERE T1.disk_id = ?`,
      [disk.id]
    );
    diskFE.labels = (
      await Promise.all(
        diskLabelsRaw.map((row: any) =>
          redactLabelValue(org_id, row.value, requesterApiKey.user_id)
        )
      )
    ).filter((label): label is string => label !== null);

    return reply.status(200).send(createApiResponse(diskFE));
  } catch (error) {
    request.log.error("Error in getDiskHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
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

    const hasTablePermission = await checkSystemPermissionsService(
      `TABLE_${SystemTableValueEnum.DISKS}` as SystemResourceID,
      requesterApiKey.user_id,
      org_id
    ).then((perms) => perms.includes(SystemPermissionType.VIEW));

    // If not owner and no table view permission, return forbidden
    if (!isOwner && !hasTablePermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

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

    // TODO: FEATURE: Implement filtering based on `body.filters`. This requires parsing the filter string.
    // For now, assuming no filters are applied to the SQL directly.
    if (body.filters && body.filters.length > 0) {
      // This is a placeholder. Real filtering logic would be complex.
      // E.g., `sql += " WHERE name LIKE ?"`, `params.push(`%${body.filters}%`)`
      request.log.warn(
        `[TODO: FEATURE] Filtering by '${body.filters}' is not yet implemented.`
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
          labels: [],
          permission_previews: isOwner
            ? [
                SystemPermissionType.CREATE,
                SystemPermissionType.EDIT,
                SystemPermissionType.DELETE,
                SystemPermissionType.VIEW,
                SystemPermissionType.INVITE,
              ]
            : await Promise.resolve().then(async () => {
                const recordPermissions = await checkSystemPermissionsService(
                  `${disk.id}` as SystemResourceID,
                  requesterApiKey.user_id,
                  org_id
                );
                const tablePermissions = await checkSystemPermissionsService(
                  `TABLE_${SystemTableValueEnum.DISKS}` as SystemResourceID,
                  requesterApiKey.user_id,
                  org_id
                );
                return Array.from(
                  new Set([...recordPermissions, ...tablePermissions])
                );
              }),
        };
        // Apply redaction
        if (
          !isOwner &&
          !diskFE.permission_previews.includes(SystemPermissionType.EDIT)
        ) {
          diskFE.auth_json = undefined;
          diskFE.private_note = undefined;
        }
        const listDiskLabelsRaw = await db.queryDrive(
          org_id,
          `SELECT T2.value FROM disk_labels AS T1 JOIN labels AS T2 ON T1.label_id = T2.id WHERE T1.disk_id = ?`,
          [disk.id]
        );
        diskFE.labels = (
          await Promise.all(
            listDiskLabelsRaw.map((row: any) =>
              redactLabelValue(org_id, row.value, requesterApiKey.user_id)
            )
          )
        ).filter((label): label is string => label !== null);
        return diskFE;
      })
    );

    const responseData: IPaginatedResponse<DiskFE> = {
      items: redactedDisks,
      page_size: itemsToReturn.length,
      total: totalCount,
      direction: body.direction || SortDirection.ASC, // Default as per Rust's default
      cursor: nextCursor || undefined,
    };

    return reply.status(200).send(createApiResponse(responseData));
  } catch (error) {
    request.log.error("Error in listDisksHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function createDiskHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestCreateDisk }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const body = request.body;

    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    const validation = await validateCreateDiskRequest(body, org_id);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }
    const userPermissions = await checkSystemPermissionsService(
      `TABLE_${SystemTableValueEnum.DISKS}` as SystemResourceID,
      requesterApiKey.user_id,
      org_id
    );

    console.log(`userPermissions==`, userPermissions);

    const hasCreatePermission =
      isOwner || userPermissions.includes(SystemPermissionType.CREATE);

    console.log(`hasCreatePermission==`, hasCreatePermission);

    if (!hasCreatePermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const prestate = snapshotPrestate();
    const diskId = (body.id || `${IDPrefixEnum.Disk}${uuidv4()}`) as Disk["id"];
    const now = Date.now();

    const newDisk: Disk = await dbHelpers.transaction(
      "drive",
      org_id,
      (database) => {
        const generatedRootFolderId = `${IDPrefixEnum.Folder}${uuidv4()}`;
        const generatedTrashFolderId = `${IDPrefixEnum.Folder}${uuidv4()}`;
        const ownerId = requesterApiKey.user_id;
        const diskType = body.disk_type;

        // 1. Insert the disk record with NULL for root_folder and trash_folder
        const insertDiskStmt = database.prepare(
          `INSERT INTO disks (id, name, disk_type, private_note, public_note, auth_json, created_at, root_folder, trash_folder, external_id, external_payload, endpoint)
           VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?)` // Set to NULL initially
        );
        insertDiskStmt.run(
          diskId,
          body.name,
          body.disk_type,
          body.private_note || null,
          body.public_note || null,
          body.auth_json || null,
          now,
          body.external_id || null,
          body.external_payload || null,
          body.endpoint || null
        );

        const rootPath = `${diskId}::/`;
        const trashPath = `${diskId}::.trash/`;

        // 2. Insert Root Folder
        const insertRootStmt = database.prepare(
          `INSERT INTO folders (id, name, parent_folder_id, full_directory_path, created_by, created_at, last_updated_date_ms, last_updated_by, disk_id, disk_type, deleted, expires_at, drive_id, has_sovereign_permissions, shortcut_to, notes, external_id, external_payload, restore_trash_prior_folder_uuid, subfolder_uuids, file_uuids)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '[]', '[]')`
        );
        insertRootStmt.run(
          generatedRootFolderId,
          "",
          null, // Root folder has no parent
          rootPath,
          ownerId,
          now,
          now,
          ownerId,
          diskId, // This now correctly references an *existing* diskId
          diskType,
          0,
          -1,
          org_id,
          1,
          null,
          null,
          null,
          null,
          null
        );

        // Add permissions for root folder
        const rootPermissionId = `${IDPrefixEnum.DirectoryPermission}${uuidv4()}`;
        database
          .prepare(
            `
            INSERT INTO permissions_directory (
              id, resource_type, resource_id, resource_path, grantee_type, grantee_id, granted_by,
              begin_date_ms, expiry_date_ms, inheritable, note, created_at, last_modified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          `
          )
          .run(
            rootPermissionId,
            "Folder",
            generatedRootFolderId,
            rootPath,
            "User",
            ownerId,
            ownerId,
            0,
            -1,
            1,
            "Default permissions for disk root folder owner",
            now,
            now
          );

        const insertRootPermissionTypes = database.prepare(`
            INSERT INTO permissions_directory_types (permission_id, permission_type) VALUES (?, ?)
          `);
        Object.values(DirectoryPermissionType).forEach((type) => {
          insertRootPermissionTypes.run(rootPermissionId, type);
        });

        // 3. Insert Trash Folder
        const insertTrashStmt = database.prepare(
          `INSERT INTO folders (id, name, parent_folder_id, full_directory_path, created_by, created_at, last_updated_date_ms, last_updated_by, disk_id, disk_type, deleted, expires_at, drive_id, has_sovereign_permissions, shortcut_to, notes, external_id, external_payload, restore_trash_prior_folder_uuid, subfolder_uuids, file_uuids)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '[]', '[]')`
        );
        insertTrashStmt.run(
          generatedTrashFolderId,
          ".trash",
          null,
          trashPath,
          ownerId,
          now,
          now,
          ownerId,
          diskId, // This now correctly references an *existing* diskId
          diskType,
          0,
          -1,
          org_id,
          1,
          null,
          null,
          null,
          null,
          null
        );

        // Add permissions for trash folder
        const trashPermissionId = `${IDPrefixEnum.DirectoryPermission}${uuidv4()}`;
        database
          .prepare(
            `
            INSERT INTO permissions_directory (
              id, resource_type, resource_id, resource_path, grantee_type, grantee_id, granted_by,
              begin_date_ms, expiry_date_ms, inheritable, note, created_at, last_modified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          `
          )
          .run(
            trashPermissionId,
            "Folder",
            generatedTrashFolderId,
            trashPath,
            "User",
            ownerId,
            ownerId,
            0,
            -1,
            0, // Not inheritable
            "Default permissions for disk trash folder owner",
            now,
            now
          );

        const insertTrashPermissionTypes = database.prepare(`
            INSERT INTO permissions_directory_types (permission_id, permission_type) VALUES (?, ?)
          `);
        Object.values(DirectoryPermissionType).forEach((type) => {
          insertTrashPermissionTypes.run(trashPermissionId, type);
        });

        // 4. Update the disk record with the actual root_folder and trash_folder
        const updateDiskFoldersStmt = database.prepare(
          `UPDATE disks SET root_folder = ?, trash_folder = ? WHERE id = ?`
        );
        updateDiskFoldersStmt.run(
          generatedRootFolderId,
          generatedTrashFolderId,
          diskId
        );

        // 5. Construct and RETURN the `newDisk` object from the transaction callback
        const constructedDisk: Disk = {
          id: diskId,
          name: body.name,
          disk_type: body.disk_type,
          private_note: body.private_note,
          public_note: body.public_note,
          auth_json: body.auth_json,
          labels: [], // Labels are handled after the transaction
          created_at: now,
          root_folder: generatedRootFolderId,
          trash_folder: generatedTrashFolderId,
          external_id: body.external_id,
          external_payload: body.external_payload,
          endpoint: body.endpoint,
        };
        return constructedDisk;
      }
    );

    // ... (rest of your existing post-transaction logic remains the same)
    await updateExternalIDMapping(
      org_id,
      undefined,
      newDisk.external_id,
      newDisk.id
    );

    if (!body.id) {
      await claimUUID(org_id, newDisk.id);
    }

    snapshotPoststate(
      prestate,
      `${requesterApiKey.user_id}: Create Disk ${newDisk.id}`
    );

    const permissionPreviews = isOwner
      ? [
          SystemPermissionType.CREATE,
          SystemPermissionType.EDIT,
          SystemPermissionType.DELETE,
          SystemPermissionType.VIEW,
          SystemPermissionType.INVITE,
        ]
      : await Promise.resolve().then(async () => {
          const recordPermissions = await checkSystemPermissionsService(
            `${diskId}` as SystemResourceID,
            requesterApiKey.user_id,
            org_id
          );
          const tablePermissions = await checkSystemPermissionsService(
            `TABLE_${SystemTableValueEnum.DISKS}` as SystemResourceID,
            requesterApiKey.user_id,
            org_id
          );
          return Array.from(
            new Set([...recordPermissions, ...tablePermissions])
          );
        });

    const diskFE: DiskFE = {
      ...newDisk,
      permission_previews: permissionPreviews,
    };

    if (
      !isOwner &&
      !diskFE.permission_previews.includes(SystemPermissionType.EDIT)
    ) {
      diskFE.auth_json = undefined;
      diskFE.private_note = undefined;
    }

    const createDiskLabelsRaw = await db.queryDrive(
      org_id,
      `SELECT T2.value FROM disk_labels AS T1 JOIN labels AS T2 ON T1.label_id = T2.id WHERE T1.disk_id = ?`,
      [newDisk.id]
    );
    diskFE.labels = (
      await Promise.all(
        createDiskLabelsRaw.map((row: any) =>
          redactLabelValue(org_id, row.value, requesterApiKey.user_id)
        )
      )
    ).filter((label): label is string => label !== null);

    return reply.status(200).send(createApiResponse(diskFE));
  } catch (error) {
    request.log.error("Error in createDiskHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
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
    const validation = await validateUpdateDiskRequest(body, org_id);
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

    const hasEditPermission = await Promise.resolve().then(async () => {
      const recordPermissions = await checkSystemPermissionsService(
        `${diskId}` as SystemResourceID,
        requesterApiKey.user_id,
        org_id
      );
      const tablePermissions = await checkSystemPermissionsService(
        `TABLE_${SystemTableValueEnum.DISKS}` as SystemResourceID,
        requesterApiKey.user_id,
        org_id
      );
      return (
        recordPermissions.includes(SystemPermissionType.EDIT) ||
        tablePermissions.includes(SystemPermissionType.EDIT)
      );
    });

    // Check update permission if not owner
    if (!isOwner && !hasEditPermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
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
      updates.push("auth_json = ?");
      values.push(body.auth_json);
    }
    if (body.external_id !== undefined) {
      updates.push("external_id = ?");
      values.push(body.external_id);
      await updateExternalIDMapping(
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
        : await Promise.resolve().then(async () => {
            const recordPermissions = await checkSystemPermissionsService(
              `${diskId}` as SystemResourceID,
              requesterApiKey.user_id,
              org_id
            );
            const tablePermissions = await checkSystemPermissionsService(
              `TABLE_${SystemTableValueEnum.DISKS}` as SystemResourceID,
              requesterApiKey.user_id,
              org_id
            );
            return Array.from(
              new Set([...recordPermissions, ...tablePermissions])
            );
          }),
    };
    // Redaction logic
    if (
      !isOwner &&
      !diskFE.permission_previews.includes(SystemPermissionType.EDIT)
    ) {
      diskFE.auth_json = undefined;
      diskFE.private_note = undefined;
    }
    const updateDiskLabelsRaw = await db.queryDrive(
      org_id,
      `SELECT T2.value FROM disk_labels AS T1 JOIN labels AS T2 ON T1.label_id = T2.id WHERE T1.disk_id = ?`,
      [updatedDisk.id]
    );
    diskFE.labels = (
      await Promise.all(
        updateDiskLabelsRaw.map((row: any) =>
          redactLabelValue(org_id, row.value, requesterApiKey.user_id)
        )
      )
    ).filter((label): label is string => label !== null);

    return reply.status(200).send(createApiResponse(diskFE));
  } catch (error) {
    request.log.error("Error in updateDiskHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
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

    const hasDeletePermission = await Promise.resolve().then(async () => {
      const recordPermissions = await checkSystemPermissionsService(
        `${diskId}` as SystemResourceID,
        requesterApiKey.user_id,
        org_id
      );
      const tablePermissions = await checkSystemPermissionsService(
        `TABLE_${SystemTableValueEnum.DISKS}` as SystemResourceID,
        requesterApiKey.user_id,
        org_id
      );
      return (
        recordPermissions.includes(SystemPermissionType.DELETE) ||
        tablePermissions.includes(SystemPermissionType.DELETE)
      );
    });

    // Check delete permission if not owner
    if (!isOwner && !hasDeletePermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
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
      await updateExternalIDMapping(
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
        message: `Internal server error - ${error}`,
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
  // The original Rust code had this line twice, keeping it for parity although redundant.
  if (body.cursor && body.cursor.length > 256) {
    return { valid: false, error: "Cursor must be 256 characters or less" };
  }
  return { valid: true };
}
