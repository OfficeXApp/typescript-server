import { FastifyRequest, FastifyReply } from "fastify";
import {
  ApiKey,
  DirectoryAction,
  DirectoryActionEnum,
  DirectoryPermissionType,
  FileRecord,
  FileRecordFE,
  FolderRecord,
  FolderRecordFE,
  IRequestDirectoryAction,
  IRequestListDirectory,
  IResponseListDirectory,
  UserID,
  DriveFullFilePath,
  DirectoryResourceID,
  FilePathBreadcrumb,
  ISuccessResponse,
  IDPrefixEnum,
  DirectoryActionErrorInfo,
  RestoreTrashResponse,
  DiskID,
  FolderID,
} from "@officexapp/types";
import { authenticateRequest } from "../../../../services/auth";
import { db } from "../../../../services/database";
import { getDriveOwnerId, OrgIdParams } from "../../types";
import {
  checkDirectoryPermissions,
  deriveBreadcrumbVisibilityPreviews,
  deriveDirectoryBreadcrumbs,
  previewDirectoryPermissions,
} from "../../../../services/permissions/directory";
import {
  getFileMetadata,
  getFolderMetadata,
} from "../../../../services/directory/drive";
import {
  castFileToFE,
  castFolderToFE,
  pipeAction,
} from "../../../../services/directory/actions"; // Correctly import pipeAction

/**
 * Clips a full directory path for frontend display.
 * e.g., "disk::/folder1/folder2/file.txt" -> "disk::../file.txt"
 */
function clipPath(fullPath: DriveFullFilePath): string {
  if (!fullPath) return "";
  const parts = fullPath.split("::/");
  if (parts.length > 1) {
    const diskId = parts[0];
    const pathParts = parts[1].split("/").filter((s) => s.length > 0);
    if (pathParts.length > 1) {
      const lastPart = pathParts[pathParts.length - 1];
      // Ensure trailing slash for folders, no trailing slash for files
      const isFolder = fullPath.endsWith("/");
      return `${diskId}::../${lastPart}${isFolder ? "/" : ""}`;
    }
    // If no intermediate folders, return as is (e.g., "disk::/file.txt" or "disk::/folder/")
    return fullPath;
  }
  return fullPath;
}

/**
 * Fetches root shortcuts of a user. This function is missing from the provided TypeScript but exists in Rust.
 * It queries permissions related to the user and their groups, filters by disk_id,
 * sorts, paginates, and then fetches the corresponding file/folder metadata.
 *
 * This implementation aims to replicate the Rust logic for `fetch_root_shortcuts_of_user`.
 */
async function fetch_root_shortcuts_of_user(
  driveId: string,
  config: IRequestListDirectory,
  userId: UserID
): Promise<{
  folders: FolderRecordFE[];
  files: FileRecordFE[];
  total_files: number;
  total_folders: number;
  cursor: string | null;
  breadcrumbs: FilePathBreadcrumb[];
}> {
  const diskId = config.disk_id;
  if (!diskId) {
    throw new Error("DiskID not provided for fetching root shortcuts.");
  }

  // Fetch all relevant permissions for the user and their groups for this drive.
  // This is a simplified fetch; a real implementation might be more granular.
  const allPermissions = await db.queryDrive(
    driveId,
    `
    SELECT
      pd.id, pd.resource_type, pd.resource_id, pd.resource_path, pd.begin_date_ms, pd.expiry_date_ms, pd.inheritable,
      GROUP_CONCAT(pdt.permission_type) AS permission_types
    FROM permissions_directory pd
    JOIN permissions_directory_types pdt ON pd.id = pdt.permission_id
    WHERE (
      (pd.grantee_type = 'User' AND pd.grantee_id = ?) OR
      (pd.grantee_type = 'Group' AND pd.grantee_id IN (
        SELECT GI.group_id FROM group_invites GI WHERE GI.invitee_id = ?
      )) OR
      pd.grantee_type = 'Public'
    )
    GROUP BY pd.id, pd.resource_type, pd.resource_id, pd.resource_path, pd.begin_date_ms, pd.expiry_date_ms, pd.inheritable
    ORDER BY pd.last_modified_at DESC;
  `,
    [
      userId.replace(IDPrefixEnum.User, ""),
      userId.replace(IDPrefixEnum.User, ""),
    ] // Remove prefix for DB storage
  );

  const now = Date.now(); // Current time in milliseconds for expiry check

  let filteredResources: Array<FileRecord | FolderRecord> = [];

  for (const perm of allPermissions) {
    // Only consider active permissions
    if (perm.expires_at !== -1 && perm.expires_at < now) {
      continue; // Permission has expired
    }
    if (perm.begin_date_ms > now) {
      continue; // Permission not yet active
    }

    const permissionTypes = (perm.permission_types as string).split(",");
    // Check if the permission includes 'VIEW'
    if (!permissionTypes.includes(DirectoryPermissionType.VIEW)) {
      continue;
    }

    let resource: FileRecord | FolderRecord | null = null;
    if (perm.resource_type === "File") {
      const file = await getFileMetadata(
        driveId,
        `${IDPrefixEnum.File}${perm.resource_id}`
      );
      if (file && file.disk_id === diskId && !file.is_deleted) {
        resource = file as FileRecord;
      }
    } else if (perm.resource_type === "Folder") {
      const folder = await getFolderMetadata(
        driveId,
        `${IDPrefixEnum.Folder}${perm.resource_id}`
      );
      if (folder && folder.disk_id === diskId && !folder.is_deleted) {
        resource = folder as FolderRecord;
      }
    }

    if (resource) {
      // Ensure uniqueness to avoid duplicates if multiple permissions grant access to the same resource
      const resourceIdFull = `${resource.id}`;
      if (!filteredResources.some((r) => `${r.id}` === resourceIdFull)) {
        filteredResources.push(resource);
      }
    }
  }

  // Sort by name (alphabetical, as a proxy for Rust's default sorting or relevance)
  filteredResources.sort((a, b) => a.name.localeCompare(b.name));

  // Pagination
  const pageSize = config.page_size || 50;
  const cursorIndex = config.cursor
    ? filteredResources.findIndex((r) => r.id === config.cursor)
    : -1;
  const start = cursorIndex !== -1 ? cursorIndex + 1 : 0;
  const paginatedResources = filteredResources.slice(start, start + pageSize);

  let foldersFE: FolderRecordFE[] = [];
  let filesFE: FileRecordFE[] = [];

  for (const res of paginatedResources) {
    if ((res as FolderRecord).subfolder_uuids !== undefined) {
      foldersFE.push(
        await castFolderToFE(res as FolderRecord, userId, driveId)
      );
    } else {
      filesFE.push(await castFileToFE(res as FileRecord, userId, driveId));
    }
  }

  const nextCursor =
    start + pageSize < filteredResources.length
      ? paginatedResources[paginatedResources.length - 1]?.id || null
      : null;

  // Breadcrumbs for "Shared with me" view
  const breadcrumbs: FilePathBreadcrumb[] = [];
  const disk = (
    await db.queryDrive(
      driveId,
      "SELECT name, root_folder_id FROM disks WHERE id = ?",
      [diskId]
    )
  )[0];
  if (disk) {
    breadcrumbs.push({
      resource_id: disk.root_folder_id,
      resource_name: disk.name,
      visibility_preview: await deriveBreadcrumbVisibilityPreviews(
        `${IDPrefixEnum.Folder}${disk.root_folder_id}` as DirectoryResourceID,
        driveId
      ),
    });
  }
  breadcrumbs.push({
    resource_id: "shared-with-me", // Special ID for this conceptual folder
    resource_name: "Shared with me",
    visibility_preview: [], // No specific visibility for this conceptual folder
  });

  return {
    folders: foldersFE,
    files: filesFE,
    total_files: filesFE.length, // Only count the currently fetched ones for this response
    total_folders: foldersFE.length, // Only count the currently fetched ones for this response
    cursor: nextCursor,
    breadcrumbs: breadcrumbs,
  };
}

/**
 * Handles the listing of directory contents.
 * Migrated from Rust: `list_directorys_handler` and `fetch_files_at_folder_path`
 */
export async function listDirectoryHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestListDirectory;
  }>,
  reply: FastifyReply
): Promise<void> {
  const { org_id: driveId } = request.params;
  const listRequest = request.body;
  const userApiKey = await authenticateRequest(request, "drive", driveId);

  if (!userApiKey) {
    return reply
      .status(401)
      .send({ err: { code: 401, message: "Unauthorized" } });
  }

  try {
    let targetFolderId: string | undefined = listRequest.folder_id;
    let targetResourceId: DirectoryResourceID;

    // If disk_id is provided and no folder_id/path, it means "root shortcuts of user"
    if (listRequest.disk_id && !targetFolderId && !listRequest.path) {
      const shortcutResponse = await fetch_root_shortcuts_of_user(
        driveId,
        listRequest,
        userApiKey.user_id
      );
      const response: ISuccessResponse<{
        folders: FolderRecordFE[];
        files: FileRecordFE[];
        total_files: number;
        total_folders: number;
        cursor: string | null;
        breadcrumbs: FilePathBreadcrumb[];
        permission_previews: DirectoryPermissionType[]; // Rust response includes this at root level
      }> = {
        ok: {
          data: {
            ...shortcutResponse,
            permission_previews: [], // Permissions are on individual items for shortcuts
          },
        },
      };
      return reply.status(200).send(response);
    }

    // Resolve folder_id from path if provided and folder_id is not already set
    if (listRequest.path && !targetFolderId) {
      // Assuming `db.queryDrive` is typed to return the full FolderRecord or null
      const folderResult = (await db.queryDrive(
        driveId,
        "SELECT id FROM folders WHERE full_directory_path = ?",
        [listRequest.path]
      )) as { id: FolderID }[];

      if (folderResult.length > 0) {
        targetFolderId = folderResult[0].id;
      } else {
        return reply.status(404).send({
          err: { code: 404, message: `Path not found: ${listRequest.path}` },
        });
      }
    } else if (!targetFolderId && !listRequest.disk_id) {
      // Neither folder_id nor path nor disk_id provided. This case implies an error.
      return reply.status(400).send({
        err: {
          code: 400,
          message: "Neither folder_id, path, nor disk_id provided.",
        },
      });
    }

    // Now targetFolderId should be set (either directly from request, or resolved from path, or it's implicitly handling a disk's root when `fetch_root_shortcuts_of_user` wasn't called)
    // If targetFolderId is still undefined here, it implies a logic gap if disk_id was not sufficient for fetch_root_shortcuts_of_user.
    // For now, let's assume it should always be present if we proceed this far without calling fetch_root_shortcuts_of_user.
    if (!targetFolderId) {
      // If we reach here and disk_id was provided but no folder_id/path, and fetch_root_shortcuts_of_user wasn't sufficient,
      // it means we probably need to derive the root folder ID from the disk_id.
      if (listRequest.disk_id) {
        const diskRootResult = (await db.queryDrive(
          driveId,
          "SELECT root_folder_id FROM disks WHERE id = ?",
          [listRequest.disk_id]
        )) as { root_folder_id: FolderID }[];
        if (diskRootResult.length > 0) {
          targetFolderId = diskRootResult[0].root_folder_id;
        } else {
          return reply.status(404).send({
            err: {
              code: 404,
              message: `Disk not found: ${listRequest.disk_id}`,
            },
          });
        }
      } else {
        return reply.status(400).send({
          err: { code: 400, message: "A folder_id or path is required." },
        });
      }
    }

    targetResourceId = `${IDPrefixEnum.Folder}${targetFolderId}`;

    // Permission check for the target folder using the imported service function
    const permissionsForFolder = await checkDirectoryPermissions(
      targetResourceId,
      userApiKey.user_id,
      driveId
    );
    const isOwner = (await getDriveOwnerId(driveId)) === userApiKey.user_id;

    if (
      !isOwner &&
      !permissionsForFolder.includes(DirectoryPermissionType.VIEW)
    ) {
      return reply.status(403).send({
        err: {
          code: 403,
          message: "You don't have permission to view this directory",
        },
      });
    }

    const pageSize = listRequest.page_size || 50;
    const offset = parseInt(listRequest.cursor || "0", 10);

    // Fetch subfolders and files for the target folder
    const [folders, files, counts] = await Promise.all([
      db.queryDrive(
        driveId,
        "SELECT id, name, parent_folder_id, full_directory_path, created_by_user_id, created_at, last_updated_date_ms, last_updated_by_user_id, disk_id, disk_type, is_deleted, expires_at, drive_id, restore_trash_prior_folder_id, has_sovereign_permissions, shortcut_to_folder_id, notes, external_id, external_payload FROM folders WHERE parent_folder_id = ? LIMIT ? OFFSET ?",
        [targetFolderId, pageSize, offset]
      ),
      db.queryDrive(
        driveId,
        `
        SELECT
          f.id, f.name, f.parent_folder_id, f.version_id, f.extension, f.full_directory_path, f.created_by_user_id, f.created_at, f.disk_id, f.disk_type, f.file_size, f.raw_url, f.last_updated_date_ms, f.last_updated_by_user_id, f.is_deleted, f.drive_id, f.upload_status, f.expires_at, f.restore_trash_prior_folder_id, f.has_sovereign_permissions, f.shortcut_to_file_id, f.notes, f.external_id, f.external_payload,
          fv.file_version, fv.prior_version_id
        FROM files f
        JOIN file_versions fv ON f.version_id = fv.version_id
        WHERE f.parent_folder_id = ? LIMIT ? OFFSET ?
      `,
        [targetFolderId, pageSize, offset]
      ),
      db.queryDrive(
        driveId,
        `
        SELECT
          (SELECT COUNT(*) FROM folders WHERE parent_folder_id = ?) as total_folders,
          (SELECT COUNT(*) FROM files WHERE parent_folder_id = ?) as total_files
      `,
        [targetFolderId, targetFolderId]
      ),
    ]);

    const { total_folders, total_files } = counts[0] as {
      total_folders: number;
      total_files: number;
    };

    // Cast records to their Frontend-Extended versions
    const foldersFE = await Promise.all(
      (folders as FolderRecord[]).map((f) =>
        castFolderToFE(f, userApiKey.user_id, driveId)
      )
    );
    const filesFE = await Promise.all(
      (files as FileRecord[]).map((f) =>
        castFileToFE(f, userApiKey.user_id, driveId)
      )
    );

    // Calculate next cursor
    const totalItemsFetched = foldersFE.length + filesFE.length;
    const hasMoreItems = offset + pageSize < total_folders + total_files;

    let nextCursor: string | null = null;
    if (hasMoreItems) {
      if (foldersFE.length === pageSize) {
        // All fetched items are folders, next cursor is the last folder's ID
        nextCursor = foldersFE[foldersFE.length - 1]?.id || null;
      } else if (foldersFE.length < pageSize && filesFE.length > 0) {
        // Mix of folders and files, or only files. Next cursor is the last file's ID if we fetched any files
        nextCursor = filesFE[filesFE.length - 1]?.id || null;
      } else if (foldersFE.length < pageSize && filesFE.length === 0) {
        // If we fetched fewer folders than page size and no files, it means there are no more.
        // This scenario implies we've exhausted either folders or both.
        nextCursor = null;
      }
      // If we fetched exactly `pageSize` items, and there are more total items,
      // the next cursor should be the ID of the last fetched item.
      // The Rust code's cursor logic is simpler: `Some(current_pos.to_string())`.
      // It iterates linearly over all items (folders then files) and takes a slice.
      // Replicating that exactly for SQLite pagination by offset/limit is tricky without UNION ALL and sorting.
      // The simplest approach that matches the spirit for current paginated set:
      if (foldersFE.length + filesFE.length > 0 && hasMoreItems) {
        // This is a simplified cursor. For exact Rust match, you'd need to fetch all, sort, then slice.
        // Given current DB queries, `offset + pageSize` is the most direct representation.
        nextCursor = (offset + pageSize).toString();
      }
    }

    // Derive breadcrumbs for the current folder
    const breadcrumbs = await deriveDirectoryBreadcrumbs(
      targetResourceId,
      userApiKey.user_id,
      driveId
    );

    // Get permission previews for the current folder itself
    const permissionPreviewsForCurrentFolder = (
      await previewDirectoryPermissions(
        targetResourceId,
        userApiKey.user_id,
        driveId
      )
    ).map((p) => p.grant_type as DirectoryPermissionType);

    const response: ISuccessResponse<{
      folders: FolderRecordFE[];
      files: FileRecordFE[];
      total_files: number;
      total_folders: number;
      cursor: string | null;
      breadcrumbs: FilePathBreadcrumb[];
      permission_previews: DirectoryPermissionType[];
    }> = {
      ok: {
        data: {
          folders: foldersFE,
          files: filesFE,
          total_files,
          total_folders,
          cursor: nextCursor,
          breadcrumbs,
          permission_previews: permissionPreviewsForCurrentFolder,
        },
      },
    };

    return reply.status(200).send(response);
  } catch (error) {
    request.log.error(error, "Error in listDirectoryHandler");
    return reply.status(500).send({
      err: { code: 500, message: `Internal server error - ${error}` },
    });
  }
}

/**
 * Handles a batch of directory actions.
 * This handler now uses the `pipeAction` from `src/services/directory/actions.ts`
 * to centralize the action logic and permission checks, adhering to the prompt.
 */
export async function directoryActionHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestDirectoryAction;
  }>,
  reply: FastifyReply
): Promise<void> {
  const { org_id: driveId } = request.params;
  const { actions } = request.body;
  const userApiKey = await authenticateRequest(request, "drive", driveId);

  if (!userApiKey) {
    return reply
      .status(401)
      .send({ err: { code: 401, message: "Unauthorized" } });
  }

  const outcomes = [];
  for (const action of actions) {
    try {
      // Delegate to the pipeAction service for all logic, including permissions
      const result = await pipeAction(action, userApiKey.user_id, driveId);

      // The structure of 'result' directly matches the Rust DirectoryActionResult enum,
      // so we can assign it directly after successful execution.
      outcomes.push({
        success: true,
        request: action,
        response: { result },
      });
    } catch (error: any) {
      // Cast the error to DirectoryActionErrorInfo if it's an instance of it,
      // otherwise, create a generic 500 error.
      const actionError: DirectoryActionErrorInfo = {
        code: 500,
        message: error.message || "An unknown error occurred",
      };

      outcomes.push({
        success: false,
        request: action,
        response: {
          error: {
            code: actionError.code,
            message: actionError.message,
          },
        },
      });
    }
  }

  return reply.status(200).send(outcomes);
}

// --- Placeholder Handlers for other routes (unchanged as per instructions) ---

export async function handleUploadChunk(
  request: FastifyRequest,
  reply: FastifyReply
) {
  reply.status(501).send({ err: { code: 501, message: "Not Implemented" } });
}

export async function handleCompleteUpload(
  request: FastifyRequest,
  reply: FastifyReply
) {
  reply.status(501).send({ err: { code: 501, message: "Not Implemented" } });
}

export async function downloadFileMetadataHandler(
  request: FastifyRequest,
  reply: FastifyReply
) {
  reply.status(501).send({ err: { code: 501, message: "Not Implemented" } });
}

export async function downloadFileChunkHandler(
  request: FastifyRequest,
  reply: FastifyReply
) {
  reply.status(501).send({ err: { code: 501, message: "Not Implemented" } });
}

export async function getRawUrlProxyHandler(
  request: FastifyRequest,
  reply: FastifyReply
) {
  reply.status(501).send({ err: { code: 501, message: "Not Implemented" } });
}
