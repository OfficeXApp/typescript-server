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
import { getDriveOwnerId } from "../../types";
import {
  checkSystemPermissions as checkSystemPermissionsService,
  canUserAccessSystemPermission as canUserAccessSystemPermissionService,
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

export async function fetch_root_shortcuts_of_user(
  config: IRequestListDirectory,
  userId: UserID,
  orgId: DriveID // Added orgId as a parameter
): Promise<IResponseListDirectory> {
  console.log(
    `[DRIVE] Fetching root shortcuts for user ${userId} in organization ${orgId} with config:`,
    config
  );

  const diskId = config.disk_id;
  if (!diskId) {
    throw new Error("DiskID must be provided for fetching root shortcuts.");
  }

  // 1. Get permissions explicitly granted to this user
  const userPermissionsRows = await db.queryDrive(
    orgId,
    `
    SELECT
      pd.resource_id,
      pd.resource_type,
      pd.resource_path,
      GROUP_CONCAT(pdt.permission_type) AS permission_types
    FROM permissions_directory pd
    JOIN permissions_directory_types pdt ON pd.id = pdt.permission_id
    WHERE pd.grantee_type = 'User' AND pd.grantee_id = ?
    GROUP BY pd.resource_id, pd.resource_type, pd.resource_path
    `,
    [userId.substring(IDPrefixEnum.User.length)]
  );

  // 2. Get permissions granted to groups the user is a member of
  // First, find all groups the user is a member of (via group_invites)
  const userGroupInvites = await db.queryFactory(
    // Group invites are factory-level
    `
    SELECT group_id FROM group_invites
    WHERE invitee_type = 'USER' AND invitee_id = ? AND expires_at > ? AND active_from <= ?
    `,
    [userId.substring(IDPrefixEnum.User.length), Date.now(), Date.now()]
  );
  const userGroupIds = userGroupInvites.map((row: any) => row.group_id);

  let groupPermissionsRows: any[] = [];
  if (userGroupIds.length > 0) {
    const placeholders = userGroupIds.map(() => "?").join(",");
    groupPermissionsRows = await db.queryDrive(
      orgId,
      `
      SELECT
        pd.resource_id,
        pd.resource_type,
        pd.resource_path,
        GROUP_CONCAT(pdt.permission_type) AS permission_types
      FROM permissions_directory pd
      JOIN permissions_directory_types pdt ON pd.id = pdt.permission_id
      WHERE pd.grantee_type = 'Group' AND pd.grantee_id IN (${placeholders})
      GROUP BY pd.resource_id, pd.resource_type, pd.resource_path
      `,
      userGroupIds
    );
  }

  // 3. Get public permissions
  const publicPermissionsRows = await db.queryDrive(
    orgId,
    `
    SELECT
      pd.resource_id,
      pd.resource_type,
      pd.resource_path,
      GROUP_CONCAT(pdt.permission_type) AS permission_types
    FROM permissions_directory pd
    JOIN permissions_directory_types pdt ON pd.id = pdt.permission_id
    WHERE pd.grantee_type = 'PUBLIC'
    GROUP BY pd.resource_id, pd.resource_type, pd.resource_path
    `
  );

  // Combine and deduplicate resources that the user has VIEW permission for
  const uniqueResources = new Map<
    string,
    {
      resource_id: string;
      resource_type: string;
      resource_path: string;
      permission_types: DirectoryPermissionType[];
    }
  >();

  const processPermissionRows = (rows: any[]) => {
    for (const row of rows) {
      const resourceIdFull =
        row.resource_type === "Folder"
          ? `${IDPrefixEnum.Folder}${row.resource_id}`
          : `${IDPrefixEnum.File}${row.resource_id}`;

      let currentPermissions: DirectoryPermissionType[] = [];
      if (uniqueResources.has(resourceIdFull)) {
        currentPermissions =
          uniqueResources.get(resourceIdFull)!.permission_types;
      }
      // Ensure row.permission_types is a string before splitting
      const newPermissions = ((row.permission_types as string) || "")
        .split(",")
        .filter(Boolean)
        .map((p) => p as DirectoryPermissionType);
      const combinedPermissions = Array.from(
        new Set([...currentPermissions, ...newPermissions])
      );

      uniqueResources.set(resourceIdFull, {
        resource_id: row.resource_id,
        resource_type: row.resource_type,
        resource_path: row.resource_path,
        permission_types: combinedPermissions,
      });
    }
  };

  processPermissionRows(userPermissionsRows);
  processPermissionRows(groupPermissionsRows);
  processPermissionRows(publicPermissionsRows);

  const isOwner = (await getDriveOwnerId(orgId)) === userId;

  let foldersRaw: FolderRecord[] = [];
  let filesRaw: FileRecord[] = [];

  const diskRecords = await db.queryDrive(
    orgId,
    "SELECT id, root_folder_id FROM disks WHERE id = ?",
    [config.disk_id]
  );
  const diskRootFolderId =
    diskRecords.length > 0 ? diskRecords[0].root_folder_id : null;

  for (const [fullResourceId, resourceData] of uniqueResources.entries()) {
    // Check if the resource has VIEW permission
    if (
      !isOwner &&
      !resourceData.permission_types.includes(DirectoryPermissionType.VIEW)
    ) {
      continue; // Skip if no view permission
    }

    if (resourceData.resource_type === "Folder") {
      const folder = await db.queryDrive(
        orgId,
        `SELECT id, name, parent_folder_id, full_directory_path, created_by_user_id, created_at, last_updated_at, last_updated_by_user_id, disk_id, disk_type, is_deleted, expires_at, drive_id, restore_trash_prior_folder_id, has_sovereign_permissions, shortcut_to_folder_id, notes, external_id, external_payload FROM folders WHERE id = ?`,
        [fullResourceId.substring(IDPrefixEnum.Folder.length)]
      );
      if (folder.length > 0) {
        const folderRecord = folder[0] as FolderRecord;
        // Additional check: filter for folders that are direct children of the disk root
        if (folderRecord.parent_folder_uuid === diskRootFolderId) {
          foldersRaw.push(folderRecord);
        }
      }
    } else if (resourceData.resource_type === "File") {
      const file = await db.queryDrive(
        orgId,
        `SELECT id, name, parent_folder_id, version_id, extension, full_directory_path, created_by_user_id, created_at, disk_id, disk_type, file_size, raw_url, last_updated_at, last_updated_by_user_id, is_deleted, drive_id, upload_status, expires_at, restore_trash_prior_folder_id, has_sovereign_permissions, shortcut_to_file_id, notes, external_id, external_payload FROM files WHERE id = ?`,
        [fullResourceId.substring(IDPrefixEnum.File.length)]
      );
      if (file.length > 0) {
        const fileRecord = file[0] as FileRecord;
        // Additional check: filter for files that are direct children of the disk root
        if (fileRecord.parent_folder_uuid === diskRootFolderId) {
          filesRaw.push(fileRecord);
        }
      }
    }
  }

  // Sort the results (Rust sorts by last_modified_at descending for permissions)
  // Here, we'll sort by last_updated_date_ms if available, otherwise by created_at.
  let allItems = [
    ...foldersRaw.map((f) => ({ ...f, type: "folder" as const })),
    ...filesRaw.map((f) => ({ ...f, type: "file" as const })),
  ];

  allItems.sort((a, b) => {
    const aTime = a.last_updated_date_ms || a.created_at || 0;
    const bTime = b.last_updated_date_ms || b.created_at || 0;
    if (config.direction === SortDirection.DESC) {
      return bTime - aTime;
    }
    return aTime - bTime;
  });

  let startIndex = 0;
  if (config.cursor) {
    const cursorIndex = allItems.findIndex((item) => item.id === config.cursor);
    if (cursorIndex !== -1) {
      startIndex = cursorIndex + 1;
    }
  }

  // Use optional chaining with a default value for page_size
  const paginatedItems = allItems.slice(
    startIndex,
    startIndex + (config.page_size ?? 50)
  ); // Default to 50 if page_size is undefined

  const feFolders: FolderRecordFE[] = [];
  const feFiles: FileRecordFE[] = [];

  for (const item of paginatedItems) {
    if (item.type === "folder") {
      const folder = item as FolderRecord;
      const resourceId =
        `${IDPrefixEnum.Folder}${folder.id}` as DirectoryResourceID;
      const permission_previews = await checkDirectoryPermissions(
        resourceId,
        userId,
        orgId
      );
      feFolders.push({
        ...folder,
        clipped_directory_path: clipDirectoryPath(folder.full_directory_path),
        permission_previews,
      });
    } else if (item.type === "file") {
      const file = item as FileRecord;
      const resourceId =
        `${IDPrefixEnum.File}${file.id}` as DirectoryResourceID;
      const permission_previews = await checkDirectoryPermissions(
        resourceId,
        userId,
        orgId
      );
      feFiles.push({
        ...file,
        clipped_directory_path: clipDirectoryPath(file.full_directory_path),
        permission_previews,
      });
    }
  }

  const nextCursor =
    paginatedItems.length > 0 &&
    startIndex + paginatedItems.length < allItems.length
      ? paginatedItems[paginatedItems.length - 1].id
      : null;

  // Breadcrumbs for root shortcuts: Disk Name -> "Shared with me" (as per Rust example)
  let breadcrumbs: FilePathBreadcrumb[] = [];

  const disk = (
    await db.queryDrive(orgId, "SELECT id, name FROM disks WHERE id = ?", [
      config.disk_id,
    ])
  )[0];
  if (disk) {
    const diskRootPath = `${disk.id}::/`;
    const diskRootFolder = (
      await db.queryDrive(
        orgId,
        "SELECT id FROM folders WHERE full_directory_path = ?",
        [diskRootPath]
      )
    )[0];
    if (diskRootFolder) {
      breadcrumbs.push({
        resource_id: diskRootFolder.id,
        resource_name: disk.name,
        visibility_preview: await deriveBreadcrumbVisibilityPreviews(
          `${IDPrefixEnum.Folder}${diskRootFolder.id}` as DirectoryResourceID,
          orgId
        ),
      });
    }
  }
  breadcrumbs.push({
    resource_id: "shared-with-me", // Placeholder ID for "Shared with me"
    resource_name: "Shared with me",
    visibility_preview: [], // No specific visibility for this virtual folder
  });

  return {
    ok: {
      data: {
        folders: feFolders,
        files: feFiles,
        total_folders: feFolders.length,
        total_files: feFiles.length,
        cursor: nextCursor,
        breadcrumbs: breadcrumbs,
        // Removed `permission_previews` from here as it's not part of IResponseListDirectory.data
      },
    },
  };
}

export async function ensureDiskRootAndTrashFolder(
  orgId: DriveID,
  diskId: string,
  ownerId: UserID,
  diskType: DiskTypeEnum
): Promise<{ rootFolderId: string; trashFolderId: string }> {
  const now = Date.now();
  const rootPath = `${diskId}::/`;
  const trashPath = `${diskId}::.trash/`;

  let rootFolderId: string;
  let trashFolderId: string;

  return await dbHelpers.transaction("drive", orgId, (database) => {
    // Check for existing root folder
    let existingRoot = database
      .prepare("SELECT id FROM folders WHERE full_directory_path = ?")
      .get(rootPath) as { id: string } | undefined;

    if (existingRoot) {
      rootFolderId = existingRoot.id;
    } else {
      rootFolderId = `${IDPrefixEnum.Folder}${crypto.randomUUID()}`;
      const insertRootStmt = database.prepare(
        `INSERT INTO folders (id, name, parent_folder_id, full_directory_path, created_by_user_id, created_at, last_updated_at, last_updated_by_user_id, disk_id, disk_type, is_deleted, expires_at, drive_id, has_sovereign_permissions, shortcut_to_folder_id, notes, external_id, external_payload, restore_trash_prior_folder_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );
      insertRootStmt.run(
        rootFolderId,
        "", // Name for root is empty in Rust
        null, // No parent for root
        rootPath,
        ownerId.substring(IDPrefixEnum.User.length), // Store plain ID
        now,
        now,
        ownerId.substring(IDPrefixEnum.User.length), // Store plain ID
        diskId,
        diskType,
        0, // is_deleted (false)
        -1, // expires_at (-1 for never)
        orgId,
        1, // has_sovereign_permissions (true for root as per Rust)
        null,
        null,
        null,
        null,
        null
      );

      // Add default permissions for the newly created root folder (All permissions for owner)
      const rootPermissionId = `${IDPrefixEnum.DirectoryPermission}${crypto.randomUUID()}`;
      database
        .prepare(
          `
        INSERT INTO permissions_directory (
          id, resource_type, resource_id, resource_path, grantee_type, grantee_id, granted_by_user_id,
          begin_date_ms, expiry_date_ms, inheritable, note, created_at, last_modified_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `
        )
        .run(
          rootPermissionId,
          "Folder",
          rootFolderId.substring(IDPrefixEnum.Folder.length),
          rootPath,
          "User",
          ownerId.substring(IDPrefixEnum.User.length),
          ownerId.substring(IDPrefixEnum.User.length),
          0, // Immediate
          -1, // Never expires
          1, // Inheritable
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
    }

    // Check for existing trash folder
    let existingTrash = database
      .prepare("SELECT id FROM folders WHERE full_directory_path = ?")
      .get(trashPath) as { id: string } | undefined;

    if (existingTrash) {
      trashFolderId = existingTrash.id;
    } else {
      trashFolderId = `${IDPrefixEnum.Folder}${crypto.randomUUID()}`;
      const insertTrashStmt = database.prepare(
        `INSERT INTO folders (id, name, parent_folder_id, full_directory_path, created_by_user_id, created_at, last_updated_at, last_updated_by_user_id, disk_id, disk_type, is_deleted, expires_at, drive_id, has_sovereign_permissions, shortcut_to_folder_id, notes, external_id, external_payload, restore_trash_prior_folder_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );
      insertTrashStmt.run(
        trashFolderId,
        ".trash",
        rootFolderId, // Trash is a subfolder of root
        trashPath,
        ownerId.substring(IDPrefixEnum.User.length), // Store plain ID
        now,
        now,
        ownerId.substring(IDPrefixEnum.User.length), // Store plain ID
        diskId,
        diskType,
        0, // is_deleted (false)
        -1, // expires_at (-1 for never)
        orgId,
        1, // has_sovereign_permissions (true for trash as per Rust)
        null,
        null,
        null,
        null,
        null
      );

      // Add default permissions for the newly created trash folder (All permissions for owner)
      const trashPermissionId = `${IDPrefixEnum.DirectoryPermission}${crypto.randomUUID()}`;
      database
        .prepare(
          `
        INSERT INTO permissions_directory (
          id, resource_type, resource_id, resource_path, grantee_type, grantee_id, granted_by_user_id,
          begin_date_ms, expiry_date_ms, inheritable, note, created_at, last_modified_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `
        )
        .run(
          trashPermissionId,
          "Folder",
          trashFolderId.substring(IDPrefixEnum.Folder.length),
          trashPath,
          "User",
          ownerId.substring(IDPrefixEnum.User.length),
          ownerId.substring(IDPrefixEnum.User.length),
          0, // Immediate
          -1, // Never expires
          0, // Not inheritable (sovereign permissions in Rust means not inherited by children)
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
    }
    return { rootFolderId, trashFolderId };
  });
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
      `${IDPrefixEnum.Disk}${disk_id}` as SystemResourceID, // Construct SystemResourceID for record
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
            `${IDPrefixEnum.Disk}${disk_id}` as SystemResourceID,
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
    // TODO: LABEL: Implement label redaction logic (requiring `redact_label` equivalent)
    diskFE.labels = []; // Placeholder for labels after redaction. Needs actual implementation.

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
                  `${IDPrefixEnum.Disk}${disk.id}` as SystemResourceID,
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
        // TODO: LABEL: Implement label redaction
        diskFE.labels = []; // Placeholder for labels after redaction
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
    const validation = await validateCreateDiskRequest(body, org_id);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    const hasCreatePermission = await checkSystemPermissionsService(
      `TABLE_${SystemTableValueEnum.DISKS}` as SystemResourceID,
      requesterApiKey.user_id,
      org_id
    ).then((perms) => perms.includes(SystemPermissionType.CREATE));

    // Check create permission if not owner
    if (!isOwner && !hasCreatePermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
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
        `INSERT INTO disks (id, name, disk_type, private_note, public_note, auth_json, created_at, root_folder_id, trash_folder_id, external_id, external_payload, endpoint)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );
      stmt.run(
        newDisk.id,
        newDisk.name,
        newDisk.disk_type,
        newDisk.private_note || null,
        newDisk.public_note || null,
        newDisk.auth_json || null,
        newDisk.created_at,
        newDisk.root_folder,
        newDisk.trash_folder,
        newDisk.external_id || null,
        newDisk.external_payload || null,
        newDisk.endpoint || null
      );
    });

    await updateExternalIDMapping(
      org_id,
      undefined,
      newDisk.external_id,
      newDisk.id
    );
    await claimUUID(org_id, newDisk.id);

    snapshotPoststate(
      prestate,
      `${requesterApiKey.user_id}: Create Disk ${newDisk.id}`
    );

    const diskFE: DiskFE = {
      ...newDisk,
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
              `${IDPrefixEnum.Disk}${diskId}` as SystemResourceID,
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
    // Redaction logic, replicating Rust's DiskFE::redacted
    if (
      !isOwner &&
      !diskFE.permission_previews.includes(SystemPermissionType.EDIT)
    ) {
      diskFE.auth_json = undefined;
      diskFE.private_note = undefined;
    }
    // TODO: LABEL: Implement label redaction
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
        `${IDPrefixEnum.Disk}${diskId}` as SystemResourceID,
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
              `${IDPrefixEnum.Disk}${diskId}` as SystemResourceID,
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
    // TODO: LABEL: Implement label redaction
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

    const hasDeletePermission = await Promise.resolve().then(async () => {
      const recordPermissions = await checkSystemPermissionsService(
        `${IDPrefixEnum.Disk}${diskId}` as SystemResourceID,
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
  // The original Rust code had this line twice, keeping it for parity although redundant.
  if (body.cursor && body.cursor.length > 256) {
    return { valid: false, error: "Cursor must be 256 characters or less" };
  }
  return { valid: true };
}
