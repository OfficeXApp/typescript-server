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
  WebhookEventLabel,
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
import {
  fireDirectoryWebhook,
  getActiveFileWebhooks,
  getActiveFolderWebhooks,
} from "../webhooks";
import {
  decodeShareTrackHash,
  generateShareTrackHash,
} from "../webhooks/share";

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

      // WEBHOOK: Fire File Viewed webhook
      const fileViewWebhooks = await getActiveFileWebhooks(
        driveId,
        file.id, // Use file.id directly here
        WebhookEventLabel.FILE_VIEWED
      );
      if (fileViewWebhooks.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.FILE_VIEWED,
          fileViewWebhooks,
          { File: { file: file } }, // Use the retrieved 'file' object
          { File: { file: file } }, // Use the retrieved 'file' object
          `File viewed: ${file.name}`
        );
      }

      const subfileViewWebhooks = await getActiveFileWebhooks(
        driveId,
        file.id, // Use file.id directly here
        WebhookEventLabel.SUBFILE_VIEWED
      );
      if (subfileViewWebhooks.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.SUBFILE_VIEWED,
          subfileViewWebhooks,
          { Subfile: { file: file } }, // Use the retrieved 'file' object
          { Subfile: { file: file } }, // Use the retrieved 'file' object
          `Subfile viewed: ${file.name}`
        );
      }

      // WEBHOOK: Implement share tracking logic from Rust
      let shareTrackingOriginId: string | undefined;
      let shareTrackingOriginUser: UserID | undefined;

      if (payload.share_track_hash && payload.share_track_hash.length > 0) {
        // Assuming decodeShareTrackHash exists and returns [ShareTrackID, UserID]
        const [decodedShareTrackId, decodedFromUserId] = decodeShareTrackHash(
          payload.share_track_hash
        );
        shareTrackingOriginId = decodedShareTrackId;
        shareTrackingOriginUser = decodedFromUserId;
      }

      const [myShareTrackId, myShareTrackHash] = generateShareTrackHash(userId);

      const shareTrackingPayload = {
        id: myShareTrackId,
        hash: myShareTrackHash,
        origin_id: shareTrackingOriginId,
        origin_hash: payload.share_track_hash,
        from_user: shareTrackingOriginUser,
        to_user: userId,
        resource_id: `${IDPrefixEnum.File}${file.id}` as DirectoryResourceID,
        resource_name: file.name,
        drive_id: driveId,
        timestamp_ms: Date.now(),
        endpoint_url: "TODO: FETCH_ACTUAL_URL_ENDPOINT", // Rust had URL_ENDPOINT.with(|url| url.borrow().get().clone())
        metadata: undefined,
      };

      const fileShareWebhooks = await getActiveFileWebhooks(
        driveId,
        file.id, // Use file.id directly here
        WebhookEventLabel.FILE_SHARED
      );
      if (fileShareWebhooks.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.FILE_SHARED,
          fileShareWebhooks,
          undefined,
          { ShareTracking: shareTrackingPayload },
          "Tracked file share"
        );
      }

      const subfileShareWebhooks = await getActiveFileWebhooks(
        driveId,
        file.id, // Use file.id directly here
        WebhookEventLabel.SUBFILE_SHARED
      );
      if (subfileShareWebhooks.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.SUBFILE_SHARED,
          subfileShareWebhooks,
          undefined,
          { ShareTracking: shareTrackingPayload },
          "Tracked subfile share"
        );
      }

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

      const folderViewWebhooks = await getActiveFolderWebhooks(
        driveId,
        folder.id,
        WebhookEventLabel.FOLDER_VIEWED
      );
      if (folderViewWebhooks.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.FOLDER_VIEWED,
          folderViewWebhooks,
          { Folder: { folder } },
          { Folder: { folder } },
          `Folder viewed: ${folder.name}`
        );
      }

      const subfolderViewWebhooks = await getActiveFolderWebhooks(
        driveId,
        folder.id,
        WebhookEventLabel.SUBFOLDER_VIEWED
      );
      if (subfolderViewWebhooks.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.SUBFOLDER_VIEWED,
          subfolderViewWebhooks,
          { Subfolder: { folder } },
          { Subfolder: { folder } },
          `Subfolder viewed: ${folder.name}`
        );
      }

      // WEBHOOK: Implement share tracking logic from Rust
      let shareTrackingOriginId: string | undefined;
      let shareTrackingOriginUser: UserID | undefined;

      if (payload.share_track_hash && payload.share_track_hash.length > 0) {
        // Assuming decodeShareTrackHash exists and returns [ShareTrackID, UserID]
        const [decodedShareTrackId, decodedFromUserId] = decodeShareTrackHash(
          payload.share_track_hash
        );
        shareTrackingOriginId = decodedShareTrackId;
        shareTrackingOriginUser = decodedFromUserId;
      }

      const [myShareTrackId, myShareTrackHash] = generateShareTrackHash(userId);

      const shareTrackingPayload = {
        id: myShareTrackId,
        hash: myShareTrackHash,
        origin_id: shareTrackingOriginId,
        origin_hash: payload.share_track_hash,
        from_user: shareTrackingOriginUser,
        to_user: userId,
        resource_id:
          `${IDPrefixEnum.Folder}${folder.id}` as DirectoryResourceID,
        resource_name: folder.name,
        drive_id: driveId,
        timestamp_ms: Date.now(),
        endpoint_url: "TODO: FETCH_ACTUAL_URL_ENDPOINT", // Rust had URL_ENDPOINT.with(|url| url.borrow().get().clone())
        metadata: undefined,
      };

      const folderShareWebhooks = await getActiveFolderWebhooks(
        driveId,
        folder.id,
        WebhookEventLabel.FOLDER_SHARED
      );
      if (folderShareWebhooks.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.FOLDER_SHARED,
          folderShareWebhooks,
          undefined,
          { ShareTracking: shareTrackingPayload },
          "Tracked folder share"
        );
      }

      const subfolderShareWebhooks = await getActiveFolderWebhooks(
        driveId,
        folder.id,
        WebhookEventLabel.SUBFOLDER_SHARED
      );
      if (subfolderShareWebhooks.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.SUBFOLDER_SHARED,
          subfolderShareWebhooks,
          undefined,
          { ShareTracking: shareTrackingPayload },
          "Tracked subfolder share"
        );
      }

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

      // WEBHOOK: Fire File Created webhooks
      const fileCreatedWebhooks = await getActiveFileWebhooks(
        driveId,
        fileRecord.id, // Use the ID of the newly created file
        WebhookEventLabel.FILE_CREATED
      );
      if (fileCreatedWebhooks.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.FILE_CREATED,
          fileCreatedWebhooks,
          undefined, // No before snap for creation
          { File: { file: fileRecord } },
          `File created: ${fileRecord.name}`
        );
      }

      const subfileCreatedWebhooks = await getActiveFolderWebhooks(
        driveId,
        payload.parent_folder_uuid,
        WebhookEventLabel.SUBFILE_CREATED
      );
      if (subfileCreatedWebhooks.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.SUBFILE_CREATED,
          subfileCreatedWebhooks,
          undefined, // No before snap for creation
          { Subfile: { file: fileRecord } },
          `Subfile created in folder: ${fileRecord.parent_folder_uuid}`
        );
      }

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

      // WEBHOOK: Fire Folder Created webhooks
      const folderCreatedWebhooks = await getActiveFolderWebhooks(
        driveId,
        folderRecord.id, // Use the ID of the newly created folder
        WebhookEventLabel.FOLDER_CREATED
      );
      if (folderCreatedWebhooks.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.FOLDER_CREATED,
          folderCreatedWebhooks,
          undefined, // No before snap for creation
          { Folder: { folder: folderRecord } },
          `Folder created: ${folderRecord.name}`
        );
      }

      const subfolderCreatedWebhooks = await getActiveFolderWebhooks(
        driveId,
        payload.parent_folder_uuid,
        WebhookEventLabel.SUBFOLDER_CREATED
      );
      if (subfolderCreatedWebhooks.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.SUBFOLDER_CREATED,
          subfolderCreatedWebhooks,
          undefined, // No before snap for creation
          { Subfolder: { folder: folderRecord } },
          `Subfolder created in folder: ${folderRecord.parent_folder_uuid}`
        );
      }

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
          "UPDATE files SET name = ?, last_updated_date_ms = ?, last_updated_by_user_id = ? WHERE id = ?",
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
            "UPDATE files SET full_directory_path = ?, last_updated_date_ms = ?, last_updated_by_user_id = ? WHERE id = ?",
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
        const query = `UPDATE files SET ${updateFields.join(", ")}, last_updated_date_ms = ?, last_updated_by_user_id = ? WHERE id = ?`;
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

      // WEBHOOK: Fire File Updated webhooks
      const fileUpdatedWebhooks = await getActiveFileWebhooks(
        driveId,
        payload.id,
        WebhookEventLabel.FILE_UPDATED
      );
      if (fileUpdatedWebhooks.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.FILE_UPDATED,
          fileUpdatedWebhooks,
          { File: { file: file } }, // Original file before update
          { File: { file: updatedFile } }, // Updated file
          `File updated: ${updatedFile.name}`
        );
      }

      const subfileUpdatedWebhooks = await getActiveFileWebhooks(
        driveId,
        payload.id, // Rust uses file_id for subfile.updated event
        WebhookEventLabel.SUBFILE_UPDATED
      );
      if (subfileUpdatedWebhooks.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.SUBFILE_UPDATED,
          subfileUpdatedWebhooks,
          { Subfile: { file: file } }, // Original file before update
          { Subfile: { file: updatedFile } }, // Updated file
          `Subfile updated: ${updatedFile.name}`
        );
      }

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
          "UPDATE folders SET name = ?, full_directory_path = ?, last_updated_date_ms = ?, last_updated_by_user_id = ? WHERE id = ?",
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
        const query = `UPDATE folders SET ${updateFields.join(", ")}, last_updated_date_ms = ?, last_updated_by_user_id = ? WHERE id = ?`;
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

      // WEBHOOK: Fire Folder Updated webhooks
      const folderUpdatedWebhooks = await getActiveFolderWebhooks(
        driveId,
        payload.id,
        WebhookEventLabel.FOLDER_UPDATED
      );
      if (folderUpdatedWebhooks.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.FOLDER_UPDATED,
          folderUpdatedWebhooks,
          { Folder: { folder: folder } }, // Original folder before update
          { Folder: { folder: updatedFolder } }, // Updated folder
          `Folder updated: ${updatedFolder.name}`
        );
      }

      const subfolderUpdatedWebhooks = await getActiveFolderWebhooks(
        driveId,
        payload.id, // Rust uses folder_id for subfolder.updated event
        WebhookEventLabel.SUBFOLDER_UPDATED
      );
      if (subfolderUpdatedWebhooks.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.SUBFOLDER_UPDATED,
          subfolderUpdatedWebhooks,
          { Subfolder: { folder: folder } }, // Original folder before update
          { Subfolder: { folder: updatedFolder } }, // Updated folder
          `Subfolder updated: ${updatedFolder.name}`
        );
      }

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

      // WEBHOOK: Fire File Deleted webhooks
      const fileDeletedWebhooks = await getActiveFileWebhooks(
        driveId,
        payload.id,
        WebhookEventLabel.FILE_DELETED
      );
      if (fileDeletedWebhooks.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.FILE_DELETED,
          fileDeletedWebhooks,
          { File: { file: file } }, // Before snap is the file being deleted
          undefined, // No after snap for deletion
          `File deleted: ${file.name}`
        );
      }

      const subfileDeletedWebhooks = await getActiveFileWebhooks(
        driveId,
        payload.id, // Rust uses file_id for subfile.deleted event
        WebhookEventLabel.SUBFILE_DELETED
      );
      if (subfileDeletedWebhooks.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.SUBFILE_DELETED,
          subfileDeletedWebhooks,
          { Subfile: { file: file } }, // Before snap is the file being deleted
          undefined, // No after snap for deletion
          `Subfile deleted: ${file.name}`
        );
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

      // WEBHOOK: Fire Folder Deleted webhooks
      const folderDeletedWebhooks = await getActiveFolderWebhooks(
        driveId,
        payload.id,
        WebhookEventLabel.FOLDER_DELETED
      );
      if (folderDeletedWebhooks.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.FOLDER_DELETED,
          folderDeletedWebhooks,
          { Folder: { folder: folder } }, // Before snap is the folder being deleted
          undefined, // No after snap for deletion
          `Folder deleted: ${folder.name}`
        );
      }

      const subfolderDeletedWebhooks = await getActiveFolderWebhooks(
        driveId,
        payload.id, // Rust uses folder_id for subfolder.deleted event
        WebhookEventLabel.SUBFOLDER_DELETED
      );
      if (subfolderDeletedWebhooks.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.SUBFOLDER_DELETED,
          subfolderDeletedWebhooks,
          { Subfolder: { folder: folder } }, // Before snap is the folder being deleted
          undefined, // No after snap for deletion
          `Subfolder deleted: ${folder.name}`
        );
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

      // WEBHOOK: Fire File Created and Subfile Created webhooks for the copied file
      const fileCreatedWebhooksForCopy = await getActiveFileWebhooks(
        driveId,
        copiedFile.id,
        WebhookEventLabel.FILE_CREATED
      );
      if (fileCreatedWebhooksForCopy.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.FILE_CREATED,
          fileCreatedWebhooksForCopy,
          { File: { file: file } }, // Before snap is the original file
          { File: { file: copiedFile } }, // After snap is the new copied file
          `File copied: ${file.name} to ${copiedFile.name}`
        );
      }

      const subfileCreatedWebhooksForCopy = await getActiveFolderWebhooks(
        driveId,
        copiedFile.parent_folder_uuid, // Destination folder of the copy
        WebhookEventLabel.SUBFILE_CREATED
      );
      if (subfileCreatedWebhooksForCopy.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.SUBFILE_CREATED,
          subfileCreatedWebhooksForCopy,
          { Subfile: { file: file } }, // Before snap is the original file
          { Subfile: { file: copiedFile } }, // After snap is the new copied file
          `Subfile copied into folder: ${copiedFile.parent_folder_uuid}`
        );
      }

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

      // WEBHOOK: Fire Folder Created and Subfolder Created webhooks for the copied folder
      const folderCreatedWebhooksForCopy = await getActiveFolderWebhooks(
        driveId,
        copiedFolder.id,
        WebhookEventLabel.FOLDER_CREATED
      );
      if (folderCreatedWebhooksForCopy.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.FOLDER_CREATED,
          folderCreatedWebhooksForCopy,
          { Folder: { folder: folder } }, // Before snap is the original folder
          { Folder: { folder: copiedFolder } }, // After snap is the new copied folder
          `Folder copied: ${folder.name} to ${copiedFolder.name}`
        );
      }

      const subfolderCreatedWebhooksForCopy = await getActiveFolderWebhooks(
        driveId,
        copiedFolder.parent_folder_uuid!, // Destination folder of the copy (guaranteed to exist for copied folder)
        WebhookEventLabel.SUBFOLDER_CREATED
      );
      if (subfolderCreatedWebhooksForCopy.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.SUBFOLDER_CREATED,
          subfolderCreatedWebhooksForCopy,
          { Subfolder: { folder: folder } }, // Before snap is the original folder
          { Subfolder: { folder: copiedFolder } }, // After snap is the new copied folder
          `Subfolder copied into folder: ${copiedFolder.parent_folder_uuid}`
        );
      }

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

      // WEBHOOK: Fire File Created (at new location) and File Deleted (from old location) webhooks for the moved file
      const fileCreatedWebhooksForMove = await getActiveFileWebhooks(
        driveId,
        movedFile.id,
        WebhookEventLabel.FILE_CREATED
      );
      if (fileCreatedWebhooksForMove.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.FILE_CREATED,
          fileCreatedWebhooksForMove,
          { File: { file: file } }, // Before snap is the original file
          { File: { file: movedFile } }, // After snap is the moved file
          `File moved: ${file.name} to ${movedFile.full_directory_path}`
        );
      }

      const subfileCreatedWebhooksForMove = await getActiveFolderWebhooks(
        driveId,
        movedFile.parent_folder_uuid, // New parent folder
        WebhookEventLabel.SUBFILE_CREATED
      );
      if (subfileCreatedWebhooksForMove.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.SUBFILE_CREATED,
          subfileCreatedWebhooksForMove,
          { Subfile: { file: file } }, // Before snap is the original file
          { Subfile: { file: movedFile } }, // After snap is the moved file
          `Subfile moved into folder: ${movedFile.parent_folder_uuid}`
        );
      }

      const fileDeletedWebhooksForMove = await getActiveFileWebhooks(
        driveId,
        file.id, // Use original file ID for deletion event
        WebhookEventLabel.FILE_DELETED
      );
      if (fileDeletedWebhooksForMove.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.FILE_DELETED,
          fileDeletedWebhooksForMove,
          { File: { file: file } }, // Before snap is the original file
          undefined, // No after snap for deletion
          `File moved (deleted from old location): ${file.name}`
        );
      }

      const subfileDeletedWebhooksForMove = await getActiveFileWebhooks(
        driveId,
        file.id, // Use original file ID for subfile deletion event
        WebhookEventLabel.SUBFILE_DELETED
      );
      if (subfileDeletedWebhooksForMove.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.SUBFILE_DELETED,
          subfileDeletedWebhooksForMove,
          { Subfile: { file: file } }, // Before snap is the original file
          undefined, // No after snap for deletion
          `Subfile moved (deleted from old location): ${file.name}`
        );
      }

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

      // WEBHOOK: Fire Folder Created (at new location) and Folder Deleted (from old location) webhooks for the moved folder
      const folderCreatedWebhooksForMove = await getActiveFolderWebhooks(
        driveId,
        movedFolder.id,
        WebhookEventLabel.FOLDER_CREATED
      );
      if (folderCreatedWebhooksForMove.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.FOLDER_CREATED,
          folderCreatedWebhooksForMove,
          { Folder: { folder: folder } }, // Before snap is the original folder
          { Folder: { folder: movedFolder } }, // After snap is the moved folder
          `Folder moved: ${folder.name} to ${movedFolder.full_directory_path}`
        );
      }

      const subfolderCreatedWebhooksForMove = await getActiveFolderWebhooks(
        driveId,
        movedFolder.parent_folder_uuid!, // New parent folder (guaranteed to exist)
        WebhookEventLabel.SUBFOLDER_CREATED
      );
      if (subfolderCreatedWebhooksForMove.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.SUBFOLDER_CREATED,
          subfolderCreatedWebhooksForMove,
          { Subfolder: { folder: folder } }, // Before snap is the original folder
          { Subfolder: { folder: movedFolder } }, // After snap is the moved folder
          `Subfolder moved into folder: ${movedFolder.parent_folder_uuid}`
        );
      }

      const folderDeletedWebhooksForMove = await getActiveFolderWebhooks(
        driveId,
        folder.id, // Use original folder ID for deletion event
        WebhookEventLabel.FOLDER_DELETED
      );
      if (folderDeletedWebhooksForMove.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.FOLDER_DELETED,
          folderDeletedWebhooksForMove,
          { Folder: { folder: folder } }, // Before snap is the original folder
          undefined, // No after snap for deletion
          `Folder moved (deleted from old location): ${folder.name}`
        );
      }

      const subfolderDeletedWebhooksForMove = await getActiveFolderWebhooks(
        driveId,
        folder.id, // Use original folder ID for subfolder deletion event
        WebhookEventLabel.SUBFOLDER_DELETED
      );
      if (subfolderDeletedWebhooksForMove.length > 0) {
        await fireDirectoryWebhook(
          driveId,
          WebhookEventLabel.SUBFOLDER_DELETED,
          subfolderDeletedWebhooksForMove,
          { Subfolder: { folder: folder } }, // Before snap is the original folder
          undefined, // No after snap for deletion
          `Subfolder moved (deleted from old location): ${folder.name}`
        );
      }

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

      // Moved webhook logic inside the conditional blocks to correctly capture before_snap and after_snap
      if (payload.id.startsWith(IDPrefixEnum.Folder)) {
        const folderId = payload.id as FolderID;
        const folder = await driveGetFolderMetadata(driveId, folderId);
        if (!folder)
          throw new DirectoryActionError(404, "Folder not found for restore.");

        // Before snapshot for folder restore
        const beforeSnapFolder = { Folder: { folder: folder } };

        // After snapshot for folder restore - retrieve the updated folder after restore operation
        const updatedFolder = await driveGetFolderMetadata(driveId, folderId);
        if (!updatedFolder)
          throw new DirectoryActionError(
            500,
            "Folder not found after restore."
          );
        const afterSnapFolder = { Folder: { folder: updatedFolder } };

        const restoreTrashWebhooks = await getActiveFolderWebhooks(
          driveId,
          folderId,
          WebhookEventLabel.DRIVE_RESTORE_TRASH
        );
        if (restoreTrashWebhooks.length > 0) {
          await fireDirectoryWebhook(
            driveId,
            WebhookEventLabel.DRIVE_RESTORE_TRASH,
            restoreTrashWebhooks,
            beforeSnapFolder,
            afterSnapFolder,
            "Folder restored from trash"
          );
        }
      } else if (payload.id.startsWith(IDPrefixEnum.File)) {
        const fileId = payload.id as FileID;
        const file = await driveGetFileMetadata(driveId, fileId);
        if (!file)
          throw new DirectoryActionError(404, "File not found for restore.");

        // Before snapshot for file restore
        const beforeSnapFile = { File: { file: file } };

        // After snapshot for file restore - retrieve the updated file after restore operation
        const updatedFile = await driveGetFileMetadata(driveId, fileId);
        if (!updatedFile)
          throw new DirectoryActionError(500, "File not found after restore.");
        const afterSnapFile = { File: { file: updatedFile } };

        const restoreTrashWebhooks = await getActiveFileWebhooks(
          driveId,
          fileId,
          WebhookEventLabel.DRIVE_RESTORE_TRASH
        );
        if (restoreTrashWebhooks.length > 0) {
          await fireDirectoryWebhook(
            driveId,
            WebhookEventLabel.DRIVE_RESTORE_TRASH,
            restoreTrashWebhooks,
            beforeSnapFile,
            afterSnapFile,
            "File restored from trash"
          );
        }
      } else {
        throw new DirectoryActionError(
          400,
          "Invalid resource ID for restore trash."
        );
      }

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
      "UPDATE folders SET full_directory_path = ?, last_updated_date_ms = ?, last_updated_by_user_id = ? WHERE id = ?",
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
        "UPDATE files SET full_directory_path = ?, last_updated_date_ms = ?, last_updated_by_user_id = ? WHERE id = ?",
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
