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
} from "@officexapp/types";
import { authenticateRequest } from "../../../../services/auth";
import { db } from "../../../../services/database";
import { getDriveOwnerId, OrgIdParams } from "../../types";

// TODO: PERMIT Implement a proper permission checking service
async function checkDirectoryPermissions(
  driveId: string,
  resourceId: DirectoryResourceID,
  userId: UserID
): Promise<DirectoryPermissionType[]> {
  console.log(
    `TODO: PERMIT Checking permissions for ${userId} on ${resourceId} in drive ${driveId}`
  );
  // Placeholder: Return full permissions for now
  return [
    DirectoryPermissionType.VIEW,
    DirectoryPermissionType.EDIT,
    DirectoryPermissionType.DELETE,
    DirectoryPermissionType.INVITE,
    DirectoryPermissionType.MANAGE,
    DirectoryPermissionType.UPLOAD,
  ];
}

/**
 * Clips a full directory path for frontend display.
 * e.g., "disk::/folder1/folder2/file.txt" -> "disk::.. /file.txt"
 */
function clipPath(fullPath: DriveFullFilePath): string {
  if (!fullPath) return "";
  const parts = fullPath.split("/");
  if (parts.length > 2) {
    const diskAndRoot = parts[0];
    const lastPart = parts[parts.length - 1];
    return `${diskAndRoot}::../${lastPart}`;
  }
  return fullPath;
}

/**
 * Converts a database folder record to its frontend equivalent.
 */
async function castFolderToFE(
  driveId: string,
  folder: FolderRecord,
  userId: UserID
): Promise<FolderRecordFE> {
  const resourceId: DirectoryResourceID = `FolderID_${folder.id}`;
  // TODO: PERMIT Implement actual permission fetching
  const permission_previews = await checkDirectoryPermissions(
    driveId,
    resourceId,
    userId
  );

  return {
    ...folder,
    clipped_directory_path: clipPath(folder.full_directory_path),
    permission_previews: permission_previews,
  };
}

/**
 * Converts a database file record to its frontend equivalent.
 */
async function castFileToFE(
  driveId: string,
  file: FileRecord,
  userId: UserID
): Promise<FileRecordFE> {
  const resourceId: DirectoryResourceID = `FileID_${file.id}`;
  // TODO: PERMIT Implement actual permission fetching
  const permission_previews = await checkDirectoryPermissions(
    driveId,
    resourceId,
    userId
  );

  return {
    ...file,
    clipped_directory_path: clipPath(file.full_directory_path),
    permission_previews: permission_previews,
  };
}

/**
 * Derives the breadcrumb path for a given folder.
 */
async function deriveDirectoryBreadcrumbs(
  driveId: string,
  folderId: string,
  userId: UserID
): Promise<FilePathBreadcrumb[]> {
  const breadcrumbs: FilePathBreadcrumb[] = [];
  let currentFolderId: string | null = folderId;

  while (currentFolderId) {
    const result = await db.queryDrive(
      driveId,
      "SELECT id, name, parent_folder_id FROM folders WHERE id = ?",
      [currentFolderId]
    );

    if (result.length === 0) {
      break;
    }

    const folder = result[0] as {
      id: string;
      name: string;
      parent_folder_id: string | null;
    };

    // TODO: PERMIT Implement real visibility preview logic
    breadcrumbs.unshift({
      resource_id: folder.id,
      resource_name: folder.name || "Root", // Root folder might have empty name
      visibility_preview: [], // Placeholder
    });

    currentFolderId = folder.parent_folder_id;
  }

  return breadcrumbs;
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
    let targetFolderId = listRequest.folder_id;

    // If path is provided, resolve it to a folder_id
    if (listRequest.path && !targetFolderId) {
      const folderResult = await db.queryDrive(
        driveId,
        "SELECT id FROM folders WHERE full_directory_path = ?",
        [listRequest.path]
      );
      if (folderResult.length > 0) {
        targetFolderId = (folderResult[0] as { id: string }).id;
      } else {
        return reply.status(404).send({
          err: { code: 404, message: `Path not found: ${listRequest.path}` },
        });
      }
    }

    // Special case for fetching "Shared with me" items at a disk's root
    if (listRequest.disk_id && !targetFolderId) {
      // TODO: DRIVE Implement the logic for fetching root shortcuts (`fetch_root_shortcuts_of_user`).
      // This likely involves complex permission queries.
      console.log("TODO: DRIVE Implement fetch_root_shortcuts_of_user");
      const emptyResponse: IResponseListDirectory = {
        ok: {
          data: {
            folders: [],
            files: [],
            total_files: 0,
            total_folders: 0,
            cursor: null,
            breadcrumbs: [],
          },
        },
      };
      return reply.status(200).send(emptyResponse);
    }

    if (!targetFolderId) {
      return reply.status(400).send({
        err: { code: 400, message: "A folder_id or path is required." },
      });
    }

    // Permission check
    const permissions = await checkDirectoryPermissions(
      driveId,
      `FolderID_${targetFolderId}`,
      userApiKey.user_id
    );
    const owner = await getDriveOwnerId(driveId);
    if (!owner && !permissions.includes(DirectoryPermissionType.VIEW)) {
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
        "SELECT * FROM folders WHERE parent_folder_id = ? LIMIT ? OFFSET ?",
        [targetFolderId, pageSize, offset]
      ),
      db.queryDrive(
        driveId,
        "SELECT * FROM files WHERE parent_folder_id = ? LIMIT ? OFFSET ?",
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
        castFolderToFE(driveId, f, userApiKey.user_id)
      )
    );
    const filesFE = await Promise.all(
      (files as FileRecord[]).map((f) =>
        castFileToFE(driveId, f, userApiKey.user_id)
      )
    );

    // Calculate next cursor
    const nextCursor =
      offset + pageSize < total_folders + total_files
        ? (offset + pageSize).toString()
        : null;

    const breadcrumbs = await deriveDirectoryBreadcrumbs(
      driveId,
      targetFolderId,
      userApiKey.user_id
    );

    const response: ISuccessResponse<{
      folders: FolderRecordFE[];
      files: FileRecordFE[];
      total_files: number;
      total_folders: number;
      cursor: string | null;
      breadcrumbs: FilePathBreadcrumb[];
    }> = {
      ok: {
        data: {
          folders: foldersFE,
          files: filesFE,
          total_files,
          total_folders,
          cursor: nextCursor,
          breadcrumbs,
        },
      },
    };

    return reply.status(200).send(response);
  } catch (error) {
    request.log.error(error, "Error in listDirectoryHandler");
    return reply
      .status(500)
      .send({ err: { code: 500, message: "Internal Server Error" } });
  }
}

/**
 * Handles a batch of directory actions.
 * Migrated from Rust: `action_directory_handler` and `pipe_action`
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
      // TODO: DRIVE Implement the full logic for each action type from `pipe_action`.
      // This is a placeholder that demonstrates the structure.
      console.log(
        `TODO: DRIVE Executing action ${action.action}`,
        action.payload
      );

      let result: any;
      switch (action.action) {
        case DirectoryActionEnum.GET_FILE:
        case DirectoryActionEnum.GET_FOLDER:
          // TODO: DRIVE Implement GET actions
          result = { notes: "Action not fully implemented yet." };
          break;
        case DirectoryActionEnum.CREATE_FILE:
        case DirectoryActionEnum.CREATE_FOLDER:
          // TODO: DRIVE Implement CREATE actions with permission checks and DB inserts.
          result = { notes: "Action not fully implemented yet." };
          break;
        case DirectoryActionEnum.UPDATE_FILE:
        case DirectoryActionEnum.UPDATE_FOLDER:
          // TODO: DRIVE Implement UPDATE actions with permission checks and DB updates.
          result = { notes: "Action not fully implemented yet." };
          break;
        case DirectoryActionEnum.DELETE_FILE:
        case DirectoryActionEnum.DELETE_FOLDER:
          // TODO: DRIVE Implement DELETE actions, handling `permanent` flag and trash logic.
          result = { notes: "Action not fully implemented yet." };
          break;
        // ... other cases
        default:
          throw new Error(`Action ${action.action} is not supported.`);
      }

      outcomes.push({
        success: true,
        request: action,
        response: { result },
      });
    } catch (error: any) {
      outcomes.push({
        success: false,
        request: action,
        response: {
          error: {
            code: error.code || 500,
            message: error.message,
          },
        },
      });
    }
  }

  return reply.status(200).send(outcomes);
}

// --- Placeholder Handlers for other routes ---

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
