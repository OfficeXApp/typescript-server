// src/services/directory/actions.ts

import {
  DirectoryAction,
  DirectoryActionEnum,
  CopyFilePayload,
  CopyFolderPayload,
  CreateFilePayload,
  CreateFolderPayload,
  DeleteFilePayload,
  DeleteFolderPayload,
  GetFilePayload,
  GetFolderPayload,
  MoveFilePayload,
  MoveFolderPayload,
  RestoreTrashPayload,
  UpdateFilePayload,
  UpdateFolderPayload,
} from "@officexapp/types";
import {
  DriveID,
  UserID,
  FileID,
  FolderID,
  DirectoryResourceID,
  IDPrefixEnum,
  DirectoryPermissionType,
  FileConflictResolutionEnum,
  DriveFullFilePath,
  UploadStatus,
  GenerateID,
} from "@officexapp/types";
import {
  FileRecord,
  FolderRecord,
  FileRecordFE,
  FolderRecordFE,
  FilePathBreadcrumb,
} from "@officexapp/types";
import { db } from "../database";
import path from "path";

// #region Service Imports
// =========================================================================
// These services are now correctly imported from the permissions service.
// =========================================================================
import {
  deriveDirectoryBreadcrumbs as deriveDirectoryBreadcrumbsService,
  checkDirectoryPermissions as checkDirectoryPermissionsService,
} from "../permissions/directory";
import { getDriveOwnerId } from "../../routes/v1/types";

// Import actual drive services
import {
  createFile as driveCreateFile,
  createFolder as driveCreateFolder,
  deleteResource as driveDeleteResource,
  copyFile as driveCopyFile,
  copyFolder as driveCopyFolder,
  moveFile as driveMoveFile,
  moveFolder as driveMoveFolder,
  restoreFromTrash as driveRestoreFromTrash,
  getFileMetadata as driveGetFileMetadata,
  getFolderMetadata as driveGetFolderMetadata,
} from "./drive";

// TODO: WEBHOOK
// import { fireDirectoryWebhook } from "../webhooks/directory";
// TODO: SHARE_TRACKING
// import { generateShareTrackHash, decodeShareTrackHash } from "../share_tracking";

// #endregion

/**
 * Custom error class for directory actions to return structured errors.
 */
export class DirectoryActionError extends Error {
  constructor(
    public code: number,
    public message: string
  ) {
    super(message);
    this.name = "DirectoryActionError";
  }
}

// #region Data Transformation Helpers (castFE)

/**
 * Transforms a raw FileRecord from the DB into a frontend-ready object.
 * This function now expects the `file` object to already conform to `FileRecord` structure
 * after being fetched and mapped from the database.
 */
export async function castFileToFE(
  file: FileRecord, // Expecting hydrated FileRecord
  userId: UserID,
  driveId: DriveID
): Promise<FileRecordFE> {
  const resourceId = `${IDPrefixEnum.File}${file.id}` as DirectoryResourceID;
  const permission_previews = await checkDirectoryPermissionsService(
    resourceId,
    userId,
    driveId
  );

  const pathParts = file.full_directory_path.split("::");
  let clipped_directory_path: string;

  if (pathParts.length > 1) {
    const diskId = pathParts[0];
    const filePathSegment = pathParts[1]; // e.g., "/path/to/folder/file.txt"
    const segments = filePathSegment.split("/").filter((s) => s.length > 0);

    if (segments.length > 1) {
      clipped_directory_path = `${diskId}::../${segments[segments.length - 1]}`;
    } else if (segments.length === 1) {
      clipped_directory_path = `${diskId}::${segments[0]}`;
    } else {
      clipped_directory_path = `${diskId}::/`; // Root of the disk
    }
  } else {
    clipped_directory_path = file.full_directory_path; // Fallback, should ideally not happen
  }

  return {
    ...file,
    clipped_directory_path:
      clipped_directory_path as FileRecordFE["clipped_directory_path"],
    permission_previews,
  };
}

/**
 * Transforms a raw FolderRecord from the DB into a frontend-ready object.
 * This function now expects the `folder` object to already conform to `FolderRecord` structure
 * after being fetched and mapped from the database.
 */
export async function castFolderToFE(
  folder: FolderRecord, // Expecting hydrated FolderRecord
  userId: UserID,
  driveId: DriveID
): Promise<FolderRecordFE> {
  const resourceId =
    `${IDPrefixEnum.Folder}${folder.id}` as DirectoryResourceID;
  const permission_previews = await checkDirectoryPermissionsService(
    resourceId,
    userId,
    driveId
  );

  const pathParts = folder.full_directory_path.split("::");
  let clipped_directory_path: string;

  if (pathParts.length > 1) {
    const diskId = pathParts[0];
    const folderPathSegment = pathParts[1]; // e.g., "/path/to/folder/"
    const segments = folderPathSegment.split("/").filter((s) => s.length > 0);

    if (segments.length > 1) {
      clipped_directory_path = `${diskId}::../${segments[segments.length - 1]}/`;
    } else if (segments.length === 1) {
      clipped_directory_path = `${diskId}::${segments[0]}/`;
    } else {
      clipped_directory_path = `${diskId}::/`; // Root of the disk
    }
  } else {
    clipped_directory_path = folder.full_directory_path; // Fallback, should ideally not happen
  }

  return {
    ...folder,
    clipped_directory_path:
      clipped_directory_path as FolderRecordFE["clipped_directory_path"],
    permission_previews,
  };
}

// #endregion

/**
 * Main function to handle all directory actions. This is a migration of the Rust `pipe_action` function.
 * It uses a switch statement to route actions to the appropriate logic.
 *
 * @param action The directory action to perform.
 * @param userId The ID of the user performing the action.
 * @param driveId The ID of the drive (organization) this action applies to.
 * @returns A promise that resolves with the result of the action.
 */
export async function pipeAction(
  action: DirectoryAction,
  userId: UserID,
  driveId: DriveID
) {
  // Check if the user is the owner of the drive. This grants full access.
  const isOwner = (await getDriveOwnerId(driveId)) === userId;

  switch (action.action) {
    // =========================================================================
    // GET FILE
    // =========================================================================
    case DirectoryActionEnum.GET_FILE: {
      const payload = action.payload as GetFilePayload;
      const file = await driveGetFileMetadata(driveId, payload.id);
      if (!file) {
        throw new DirectoryActionError(404, "File not found");
      }

      // PERMIT: Check permissions for viewing the file
      if (!isOwner) {
        const permissions = await checkDirectoryPermissionsService(
          `${IDPrefixEnum.File}${file.id}` as DirectoryResourceID,
          userId,
          driveId
        );
        if (!permissions.includes(DirectoryPermissionType.VIEW)) {
          throw new DirectoryActionError(
            403,
            "You don't have permission to view this file"
          );
        }
      }

      // TODO: WEBHOOK Implement webhook logic from Rust
      // fireDirectoryWebhook(...)

      // TODO: SHARE_TRACKING Implement share tracking logic from Rust
      // decodeShareTrackHash(...)
      // generateShareTrackHash(...)

      const breadcrumbs = await deriveDirectoryBreadcrumbsService(
        `${IDPrefixEnum.File}${file.id}` as DirectoryResourceID,
        userId, // Pass userId for breadcrumb permission checks
        driveId
      );
      const fileFE = await castFileToFE(file, userId, driveId);

      return {
        GetFile: {
          file: fileFE,
          breadcrumbs,
        },
      };
    }

    // =========================================================================
    // GET FOLDER
    // =========================================================================
    case DirectoryActionEnum.GET_FOLDER: {
      const payload = action.payload as GetFolderPayload;
      const folder = await driveGetFolderMetadata(driveId, payload.id);
      if (!folder) {
        throw new DirectoryActionError(404, "Folder not found");
      }

      // PERMIT: Check permissions for viewing the folder
      if (!isOwner) {
        const permissions = await checkDirectoryPermissionsService(
          `${IDPrefixEnum.Folder}${folder.id}` as DirectoryResourceID,
          userId,
          driveId
        );
        if (!permissions.includes(DirectoryPermissionType.VIEW)) {
          throw new DirectoryActionError(
            403,
            "You don't have permission to view this folder"
          );
        }
      }

      // TODO: WEBHOOK Implement webhook and share tracking logic...

      const breadcrumbs = await deriveDirectoryBreadcrumbsService(
        `${IDPrefixEnum.Folder}${folder.id}` as DirectoryResourceID,
        userId, // Pass userId for breadcrumb permission checks
        driveId
      );
      const folderFE = await castFolderToFE(folder, userId, driveId);

      return {
        GetFolder: {
          folder: folderFE,
          breadcrumbs,
        },
      };
    }

    // =========================================================================
    // CREATE FILE
    // =========================================================================
    case DirectoryActionEnum.CREATE_FILE: {
      const payload = action.payload as CreateFilePayload;

      // The permission check for `CREATE_FILE` is now handled inside `driveCreateFile` itself,
      // simplifying this action handler. `driveCreateFile` will throw an error if permissions are insufficient.
      const [fileRecord, uploadResponse] = await driveCreateFile(
        driveId,
        userId,
        payload
      );

      return {
        CreateFile: {
          file: await castFileToFE(fileRecord, userId, driveId),
          upload: uploadResponse,
          notes: "File created successfully",
        },
      };
    }

    // =========================================================================
    // CREATE FOLDER
    // =========================================================================
    case DirectoryActionEnum.CREATE_FOLDER: {
      const payload = action.payload as CreateFolderPayload;

      // The permission check for `CREATE_FOLDER` is now handled inside `driveCreateFolder` itself,
      // simplifying this action handler. `driveCreateFolder` will throw an error if permissions are insufficient.
      const folderRecord = await driveCreateFolder(driveId, userId, payload);

      return {
        CreateFolder: {
          folder: await castFolderToFE(folderRecord, userId, driveId),
          notes: "Folder created successfully",
        },
      };
    }

    // =========================================================================
    // UPDATE FILE / FOLDER
    // =========================================================================
    case DirectoryActionEnum.UPDATE_FILE: {
      const payload = action.payload as UpdateFilePayload;
      const file = await driveGetFileMetadata(driveId, payload.id);
      if (!file) throw new DirectoryActionError(404, "File not found");

      // PERMIT: Permission checks for EDIT/MANAGE
      if (!isOwner) {
        const permissions = await checkDirectoryPermissionsService(
          `${IDPrefixEnum.File}${file.id}` as DirectoryResourceID,
          userId,
          driveId
        );
        const hasPermission =
          permissions.includes(DirectoryPermissionType.EDIT) ||
          permissions.includes(DirectoryPermissionType.MANAGE);
        if (!hasPermission) {
          throw new DirectoryActionError(
            403,
            "You don't have permission to update this file."
          );
        }
      }

      // Handle name update separately since it requires path updates
      if (payload.name !== undefined && payload.name !== file.name) {
        await db.queryDrive(
          driveId,
          "UPDATE files SET name = ?, last_updated_at = ?, last_updated_by_user_id = ? WHERE id = ?",
          [payload.name, Date.now(), userId, payload.id]
        );
        // Update full_directory_path as well if name changes
        const parentFolder = await driveGetFolderMetadata(
          driveId,
          file.parent_folder_uuid
        );
        if (parentFolder) {
          const newFullPath = `${parentFolder.full_directory_path}${payload.name}`;
          await db.queryDrive(
            driveId,
            "UPDATE files SET full_directory_path = ?, last_updated_at = ?, last_updated_by_user_id = ? WHERE id = ?",
            [newFullPath, Date.now(), userId, payload.id]
          );
          // PERMIT FIX: Update resource_path for directory permissions associated with moved/renamed files
          await db.queryDrive(
            driveId,
            "UPDATE permissions_directory SET resource_path = ? WHERE resource_id = ?",
            [
              newFullPath,
              file.id.substring(IDPrefixEnum.File.length), // Store plain ID
            ]
          );
        }
      }

      // Update other metadata fields directly
      const updateFields: string[] = [];
      const updateValues: any[] = [];

      if (payload.labels !== undefined) {
        // TODO: This should update the `file_labels` junction table
        // For now, it's ignored as it's a separate persistence concern.
      }
      if (payload.expires_at !== undefined) {
        updateFields.push("expires_at = ?");
        updateValues.push(payload.expires_at);
      }
      if (payload.upload_status !== undefined) {
        updateFields.push("upload_status = ?");
        updateValues.push(payload.upload_status);
      }
      if (payload.external_id !== undefined) {
        updateFields.push("external_id = ?");
        updateValues.push(payload.external_id);
      }
      if (payload.external_payload !== undefined) {
        updateFields.push("external_payload = ?");
        updateValues.push(payload.external_payload);
      }
      if (payload.notes !== undefined) {
        updateFields.push("notes = ?");
        updateValues.push(payload.notes);
      }
      if (payload.shortcut_to !== undefined) {
        // Check for existence before accessing
        updateFields.push("shortcut_to_file_id = ?");
        updateValues.push(payload.shortcut_to);
      }

      if (updateFields.length > 0) {
        const query = `UPDATE files SET ${updateFields.join(", ")}, last_updated_at = ?, last_updated_by_user_id = ? WHERE id = ?`;
        await db.queryDrive(driveId, query, [
          ...updateValues,
          Date.now(),
          userId,
          payload.id,
        ]);
      }

      const updatedFile = await driveGetFileMetadata(driveId, payload.id);
      if (!updatedFile)
        throw new DirectoryActionError(404, "File not found after update");

      return { UpdateFile: await castFileToFE(updatedFile, userId, driveId) };
    }

    case DirectoryActionEnum.UPDATE_FOLDER: {
      const payload = action.payload as UpdateFolderPayload;
      const folder = await driveGetFolderMetadata(driveId, payload.id);
      if (!folder) throw new DirectoryActionError(404, "Folder not found");

      // PERMIT: Permission checks for EDIT/MANAGE
      if (!isOwner) {
        const permissions = await checkDirectoryPermissionsService(
          `${IDPrefixEnum.Folder}${folder.id}` as DirectoryResourceID,
          userId,
          driveId
        );
        const hasPermission =
          permissions.includes(DirectoryPermissionType.EDIT) ||
          permissions.includes(DirectoryPermissionType.MANAGE);
        if (!hasPermission) {
          throw new DirectoryActionError(
            403,
            "You don't have permission to update this folder."
          );
        }
      }

      // Handle name update separately since it requires path updates for children
      if (payload.name !== undefined && payload.name !== folder.name) {
        // Update the folder's name and full_directory_path first
        const oldPath = folder.full_directory_path;
        // Ensure newPath also has a trailing slash for consistency
        let newPath =
          path.join(
            oldPath.substring(0, oldPath.lastIndexOf(folder.name + "/")),
            payload.name
          ) + "/";

        await db.queryDrive(
          driveId,
          "UPDATE folders SET name = ?, full_directory_path = ?, last_updated_at = ?, last_updated_by_user_id = ? WHERE id = ?",
          [payload.name, newPath, Date.now(), userId, payload.id]
        );

        // Recursively update paths for all subfolders and files
        await updateSubfolderPathsRecursive(
          driveId,
          payload.id,
          oldPath,
          newPath,
          userId
        );
      }

      // Update other metadata fields directly
      const updateFields: string[] = [];
      const updateValues: any[] = [];

      if (payload.labels !== undefined) {
        // TODO: This should update the `folder_labels` junction table
        // For now, it's ignored.
      }
      if (payload.expires_at !== undefined) {
        updateFields.push("expires_at = ?");
        updateValues.push(payload.expires_at);
      }
      if (payload.external_id !== undefined) {
        updateFields.push("external_id = ?");
        updateValues.push(payload.external_id);
      }
      if (payload.external_payload !== undefined) {
        updateFields.push("external_payload = ?");
        updateValues.push(payload.external_payload);
      }
      if (payload.notes !== undefined) {
        updateFields.push("notes = ?");
        updateValues.push(payload.notes);
      }
      if (payload.shortcut_to !== undefined) {
        // Check for existence before accessing
        updateFields.push("shortcut_to_folder_id = ?");
        updateValues.push(payload.shortcut_to);
      }

      if (updateFields.length > 0) {
        const query = `UPDATE folders SET ${updateFields.join(", ")}, last_updated_at = ?, last_updated_by_user_id = ? WHERE id = ?`;
        await db.queryDrive(driveId, query, [
          ...updateValues,
          Date.now(),
          userId,
          payload.id,
        ]);
      }

      const updatedFolder = await driveGetFolderMetadata(driveId, payload.id);
      if (!updatedFolder)
        throw new DirectoryActionError(404, "Folder not found after update");

      return {
        UpdateFolder: await castFolderToFE(updatedFolder, userId, driveId),
      };
    }

    // =========================================================================
    // DELETE FILE / FOLDER
    // =========================================================================
    case DirectoryActionEnum.DELETE_FILE: {
      const payload = action.payload as DeleteFilePayload;
      const file = await driveGetFileMetadata(driveId, payload.id);
      if (!file) throw new DirectoryActionError(404, "File not found");

      // PERMIT: Permission checks for DELETE/MANAGE on the parent folder
      if (!isOwner) {
        const parentFolder = await driveGetFolderMetadata(
          driveId,
          file.parent_folder_uuid
        );
        if (!parentFolder) {
          throw new DirectoryActionError(
            500,
            "Parent folder not found for file."
          );
        }
        const permissions = await checkDirectoryPermissionsService(
          `${IDPrefixEnum.Folder}${parentFolder.id}` as DirectoryResourceID,
          userId,
          driveId
        );
        const hasPermission =
          permissions.includes(DirectoryPermissionType.DELETE) ||
          permissions.includes(DirectoryPermissionType.MANAGE);
        if (!hasPermission) {
          throw new DirectoryActionError(
            403,
            "You don't have permission to delete this file."
          );
        }
      }

      // Call the `driveDeleteResource` service
      await driveDeleteResource(driveId, payload.id, payload.permanent, userId);

      // In Rust, `delete_file` returns `DriveFullFilePath` to trash or empty string for permanent.
      // We need to determine this to match the response.
      let path_to_trash: DriveFullFilePath = "" as DriveFullFilePath;
      if (!payload.permanent) {
        // If not permanent, it was moved to trash. Get the trash path.
        const [diskResult] = await db.queryDrive(
          driveId,
          "SELECT trash_folder_id FROM disks WHERE id = ?",
          [file.disk_id]
        );
        if (diskResult) {
          const trashFolder = await driveGetFolderMetadata(
            driveId,
            diskResult.trash_folder_id
          );
          if (trashFolder) {
            // Path in trash will be the trash folder's path + original file name
            path_to_trash =
              `${trashFolder.full_directory_path}${file.name}` as DriveFullFilePath;
          }
        }
      }

      return { DeleteFile: { file_id: payload.id, path_to_trash } };
    }

    case DirectoryActionEnum.DELETE_FOLDER: {
      const payload = action.payload as DeleteFolderPayload;
      const folder = await driveGetFolderMetadata(driveId, payload.id);
      if (!folder) throw new DirectoryActionError(404, "Folder not found");

      // PERMIT: Prevent deletion of root and .trash folders (Rust logic)
      if (folder.parent_folder_uuid === null || folder.name === ".trash") {
        throw new DirectoryActionError(
          403,
          "Cannot delete root or .trash folders."
        );
      }

      // PERMIT: Permission checks for DELETE/MANAGE on the parent folder
      // Rust checks `parent_folder_uuid` on the folder being deleted, and permissions on that parent.
      if (!isOwner) {
        const parentFolder = await driveGetFolderMetadata(
          driveId,
          folder.parent_folder_uuid!
        ); // Parent is guaranteed to exist by above check
        if (!parentFolder) {
          // Defensive check, should not be hit
          throw new DirectoryActionError(
            500,
            "Parent folder not found for folder."
          );
        }
        const permissions = await checkDirectoryPermissionsService(
          `${IDPrefixEnum.Folder}${parentFolder.id}` as DirectoryResourceID,
          userId,
          driveId
        );
        const hasPermission =
          permissions.includes(DirectoryPermissionType.DELETE) ||
          permissions.includes(DirectoryPermissionType.MANAGE);
        if (!hasPermission) {
          throw new DirectoryActionError(
            403,
            "You don't have permission to delete this folder."
          );
        }
      }

      // Call the `driveDeleteResource` service
      await driveDeleteResource(driveId, payload.id, payload.permanent, userId);

      // In Rust, `delete_folder` returns `DriveFullFilePath` to trash or empty string for permanent.
      let path_to_trash: DriveFullFilePath = "" as DriveFullFilePath;
      if (!payload.permanent) {
        // If not permanent, it was moved to trash.
        const [diskResult] = await db.queryDrive(
          driveId,
          "SELECT trash_folder_id FROM disks WHERE id = ?",
          [folder.disk_id]
        );
        if (diskResult) {
          const trashFolder = await driveGetFolderMetadata(
            driveId,
            diskResult.trash_folder_id
          );
          if (trashFolder) {
            // Path in trash will be the trash folder's path + original folder name + trailing slash
            path_to_trash =
              `${trashFolder.full_directory_path}${folder.name}/` as DriveFullFilePath;
          }
        }
      }

      // TODO: Rust had `deleted_files` and `deleted_folders` in response for `DeleteFolderResponse`.
      // We might need to gather these from `driveDeleteResource` if it provided them.
      // For now, returning empty arrays as placeholders based on current `driveDeleteResource` return.
      return {
        DeleteFolder: {
          folder_id: payload.id,
          path_to_trash,
          deleted_files: [], // TODO: Populate from driveDeleteResource
          deleted_folders: [], // TODO: Populate from driveDeleteResource
        },
      };
    }

    // =========================================================================
    // COPY / MOVE / RESTORE
    // =========================================================================
    case DirectoryActionEnum.COPY_FILE: {
      const payload = action.payload as CopyFilePayload;
      const file = await driveGetFileMetadata(driveId, payload.id);
      if (!file) throw new DirectoryActionError(404, "Source file not found");

      // Permissions are checked inside `driveCopyFile` service.
      // `destination_folder_id` is now guaranteed to be present by validation, so direct access is safe.
      const copiedFile = await driveCopyFile(
        driveId,
        userId,
        payload.id,
        payload.destination_folder_id!,
        payload.file_conflict_resolution,
        undefined
      );

      return { CopyFile: await castFileToFE(copiedFile, userId, driveId) };
    }

    case DirectoryActionEnum.COPY_FOLDER: {
      const payload = action.payload as CopyFolderPayload;
      const folder = await driveGetFolderMetadata(driveId, payload.id);
      if (!folder)
        throw new DirectoryActionError(404, "Source folder not found");

      // Permissions are checked inside `driveCopyFolder` service.
      // `destination_folder_id` is now guaranteed to be present by validation.
      const copiedFolder = await driveCopyFolder(
        driveId,
        userId,
        payload.id,
        payload.destination_folder_id!,
        payload.file_conflict_resolution,
        undefined
      );

      return {
        CopyFolder: await castFolderToFE(copiedFolder, userId, driveId),
      };
    }

    case DirectoryActionEnum.MOVE_FILE: {
      const payload = action.payload as MoveFilePayload;
      const file = await driveGetFileMetadata(driveId, payload.id);
      if (!file) throw new DirectoryActionError(404, "File not found");

      // Permissions are checked inside `driveMoveFile` service.
      // `destination_folder_id` is now guaranteed to be present by validation.
      const movedFile = await driveMoveFile(
        driveId,
        userId,
        payload.id,
        payload.destination_folder_id!,
        payload.file_conflict_resolution || FileConflictResolutionEnum.KEEP_BOTH
      );

      return { MoveFile: await castFileToFE(movedFile, userId, driveId) };
    }

    case DirectoryActionEnum.MOVE_FOLDER: {
      const payload = action.payload as MoveFolderPayload;
      const folder = await driveGetFolderMetadata(driveId, payload.id);
      if (!folder) throw new DirectoryActionError(404, "Folder not found");

      // Permissions are checked inside `driveMoveFolder` service.
      // `destination_folder_id` is now guaranteed to be present by validation.
      const movedFolder = await driveMoveFolder(
        driveId,
        userId,
        payload.id,
        payload.destination_folder_id!,
        payload.file_conflict_resolution || FileConflictResolutionEnum.KEEP_BOTH
      );

      return { MoveFolder: await castFolderToFE(movedFolder, userId, driveId) };
    }

    case DirectoryActionEnum.RESTORE_TRASH: {
      const payload = action.payload as RestoreTrashPayload;

      // Permissions are checked inside `driveRestoreFromTrash` service.
      const restoreResponse = await driveRestoreFromTrash(
        driveId,
        payload,
        userId
      );

      return {
        RestoreTrash: restoreResponse,
      };
    }

    default:
      throw new DirectoryActionError(
        400,
        "Unsupported or unimplemented directory action"
      );
  }
}

/**
 * Recursively updates the full_directory_path for all children of a moved/renamed folder.
 * This is a helper function adapted from `src/services/directory/internals.ts`
 * to be directly callable within `actions.ts` for folder updates.
 */
async function updateSubfolderPathsRecursive(
  driveId: DriveID,
  folderId: FolderID,
  oldPath: string,
  newPath: string,
  userId: UserID // Used for updating `last_updated_by_user_id` and permission path updates
): Promise<void> {
  const queue: FolderID[] = [folderId];

  while (queue.length > 0) {
    const currentFolderId = queue.shift()!;

    // Fetch the current folder's path from the DB.
    const currentFolder = (
      await db.queryDrive(
        driveId,
        "SELECT full_directory_path, name FROM folders WHERE id = ?",
        [currentFolderId]
      )
    )[0];

    if (!currentFolder) continue;

    const currentOldPath = currentFolder.full_directory_path;
    const updatedPath = currentOldPath.replace(oldPath, newPath);

    // Update the folder itself
    await db.queryDrive(
      driveId,
      "UPDATE folders SET full_directory_path = ?, last_updated_at = ?, last_updated_by_user_id = ? WHERE id = ?",
      [updatedPath, Date.now(), userId, currentFolderId]
    );

    // Update child files
    const childFiles = await db.queryDrive(
      driveId,
      "SELECT id, full_directory_path FROM files WHERE parent_folder_id = ?",
      [currentFolderId]
    );

    for (const file of childFiles) {
      const newFilePath = (file.full_directory_path as string).replace(
        currentOldPath,
        updatedPath
      );
      await db.queryDrive(
        driveId,
        "UPDATE files SET full_directory_path = ?, last_updated_at = ?, last_updated_by_user_id = ? WHERE id = ?",
        [newFilePath, Date.now(), userId, file.id]
      );
      // PERMIT FIX: Update resource_path for directory permissions associated with moved/renamed files
      await db.queryDrive(
        driveId,
        "UPDATE permissions_directory SET resource_path = ? WHERE resource_id = ?",
        [
          newFilePath,
          file.id.substring(IDPrefixEnum.File.length), // Store plain ID
        ]
      );
    }

    // Enqueue child folders and update their paths
    const childFolders = await db.queryDrive(
      driveId,
      "SELECT id FROM folders WHERE parent_folder_id = ?",
      [currentFolderId]
    );

    for (const subfolder of childFolders) {
      queue.push(subfolder.id);
    }

    // PERMIT FIX: Update resource_path for directory permissions associated with the current folder
    await db.queryDrive(
      driveId,
      "UPDATE permissions_directory SET resource_path = ? WHERE resource_id = ?",
      [
        updatedPath,
        currentFolderId.substring(IDPrefixEnum.Folder.length), // Store plain ID
      ]
    );
  }
}
