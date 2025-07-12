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
  DiskID,
  DiskTypeEnum,
  FileConflictResolutionEnum,
  DriveFullFilePath,
  UploadStatus,
  GenerateID,
  IDPrefixEnum,
  ExternalID,
  ExternalPayload,
  DirectoryPermissionType,
  LabelValue,
} from "@officexapp/types";
import {
  FileRecord,
  FolderRecord,
  FileRecordFE,
  FolderRecordFE,
  FilePathBreadcrumb,
} from "@officexapp/types";
import { db, dbHelpers } from "../database";
import path from "path";

// #region Service Placeholders
// =========================================================================
// TODO: PERMIT The following services need to be implemented.
// They are placeholders to allow the core logic to be migrated.
// =========================================================================

/**
 * Placeholder for checking a user's permissions on a directory resource.
 * @returns A promise resolving to an array of permissions. For now, it's permissive.
 */
async function checkDirectoryPermissions(
  _resourceId: DirectoryResourceID,
  _userId: UserID,
  _driveId: DriveID
): Promise<DirectoryPermissionType[]> {
  // TODO: PERMIT Implement actual permission checking logic against the permissions_directory table.
  // This will involve checking permissions for the user, their groups, and public.
  return Promise.resolve([
    DirectoryPermissionType.VIEW,
    DirectoryPermissionType.UPLOAD,
    DirectoryPermissionType.EDIT,
    DirectoryPermissionType.DELETE,
    DirectoryPermissionType.MANAGE,
  ]);
}

/**
 * Placeholder for firing directory-related webhooks.
 */
async function fireDirectoryWebhook(
  _event: any,
  _webhooks: any[],
  _before: any,
  _after: any,
  _notes: string
): Promise<void> {
  // TODO: WEBHOOK Implement webhook firing logic.
  console.log(`[TODO] Firing webhook for event: ${_event}`);
  return Promise.resolve();
}

/**
 * Placeholder for generating a share tracking hash.
 */
function generateShareTrackHash(_userId: UserID): { id: string; hash: string } {
  // TODO: PERMIT Implement actual share track hash generation.
  return { id: "ShareTrackID_mock", hash: "mock_share_track_hash" };
}

/**
 * Placeholder for decoding a share tracking hash.
 */
function decodeShareTrackHash(_hash: string): { id: string; userId: UserID } {
  // TODO: PERMIT Implement actual share track hash decoding.
  return { id: "ShareTrackID_mock_decoded", userId: "UserID_mock_decoded" };
}

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
 */
async function castFileToFE(
  file: FileRecord,
  userId: UserID,
  driveId: DriveID
): Promise<FileRecordFE> {
  const resourceId = `${IDPrefixEnum.File}${file.id}`;
  const permission_previews = await checkDirectoryPermissions(
    resourceId as DirectoryResourceID,
    userId,
    driveId
  );

  // Example clipped path logic from Rust
  const pathParts = file.full_directory_path.split("/");
  let clipped_directory_path = file.full_directory_path;
  if (pathParts.length > 2) {
    clipped_directory_path = `${pathParts[0]}::../${
      pathParts[pathParts.length - 1]
    }`;
  }

  return {
    ...file,
    clipped_directory_path,
    permission_previews,
  };
}

/**
 * Transforms a raw FolderRecord from the DB into a frontend-ready object.
 */
async function castFolderToFE(
  folder: FolderRecord,
  userId: UserID,
  driveId: DriveID
): Promise<FolderRecordFE> {
  const resourceId = `${IDPrefixEnum.Folder}${folder.id}`;
  const permission_previews = await checkDirectoryPermissions(
    resourceId as DirectoryResourceID,
    userId,
    driveId
  );

  // Example clipped path logic from Rust
  const pathParts = folder.full_directory_path.split("/");
  let clipped_directory_path = folder.full_directory_path;
  if (pathParts.length > 2) {
    clipped_directory_path = `${pathParts[0]}::../${
      pathParts[pathParts.length - 1]
    }`;
  }

  return {
    ...folder,
    clipped_directory_path,
    permission_previews,
  };
}

// #endregion

// #region Internal Data Access Helpers

/**
 * Fetches a single file record from the database.
 * The FileRecord type includes a `version_id` for the current version.
 * This function retrieves the main file and its current version details.
 */
async function getFileById(
  fileId: FileID,
  driveId: DriveID
): Promise<FileRecord | null> {
  const query = `
      SELECT 
        f.*, 
        fv.version_id, 
        fv.file_version, 
        fv.prior_version_id,
        fv.notes as version_notes
      FROM files f
      JOIN file_versions fv ON f.version_id = fv.version_id
      WHERE f.id = ?`;
  const results = await db.queryDrive(driveId, query, [fileId]);
  if (results.length === 0) return null;

  const data = results[0];
  const file: FileRecord = {
    id: data.id,
    name: data.name,
    parent_folder_uuid: data.parent_folder_id,
    version_id: data.version_id,
    file_version: data.file_version,
    prior_version: data.prior_version_id,
    extension: data.extension,
    full_directory_path: data.full_directory_path,
    labels: [], // TODO: REDACT Query and join labels
    created_by: data.created_by_user_id,
    created_at: data.created_at,
    disk_id: data.disk_id,
    disk_type: data.disk_type,
    file_size: data.file_size,
    raw_url: data.raw_url,
    last_updated_date_ms: data.last_updated_at,
    last_updated_by: data.last_updated_by_user_id,
    deleted: !!data.is_deleted,
    drive_id: data.drive_id,
    expires_at: data.expires_at,
    restore_trash_prior_folder_uuid: data.restore_trash_prior_folder_id,
    has_sovereign_permissions: !!data.has_sovereign_permissions,
    shortcut_to: data.shortcut_to_file_id,
    upload_status: data.upload_status,
    external_id: data.external_id,
    external_payload: data.external_payload,
    notes: data.notes,
  };
  return file;
}

/**
 * Fetches a single folder record and its direct children IDs from the database.
 */
async function getFolderById(
  folderId: FolderID,
  driveId: DriveID
): Promise<FolderRecord | null> {
  const folderResults = await db.queryDrive(
    driveId,
    "SELECT * FROM folders WHERE id = ?",
    [folderId]
  );
  if (folderResults.length === 0) return null;
  const data = folderResults[0];

  const subfolders = await db.queryDrive(
    driveId,
    "SELECT id FROM folders WHERE parent_folder_id = ?",
    [folderId]
  );
  const files = await db.queryDrive(
    driveId,
    "SELECT id FROM files WHERE parent_folder_id = ?",
    [folderId]
  );

  const folder: FolderRecord = {
    id: data.id,
    name: data.name,
    parent_folder_uuid: data.parent_folder_id,
    subfolder_uuids: subfolders.map((r: any) => r.id),
    file_uuids: files.map((r: any) => r.id),
    full_directory_path: data.full_directory_path,
    labels: [], // TODO: REDACT Query and join labels from folder_labels table
    created_by: data.created_by_user_id,
    created_at: data.created_at,
    last_updated_date_ms: data.last_updated_at,
    last_updated_by: data.last_updated_by_user_id,
    disk_id: data.disk_id,
    disk_type: data.disk_type,
    deleted: !!data.is_deleted,
    expires_at: data.expires_at,
    drive_id: data.drive_id,
    restore_trash_prior_folder_uuid: data.restore_trash_prior_folder_id,
    has_sovereign_permissions: !!data.has_sovereign_permissions,
    shortcut_to: data.shortcut_to_folder_id,
    external_id: data.external_id,
    external_payload: data.external_payload,
    notes: data.notes,
  };
  return folder;
}

/**
 * Recursively fetches parent folders to build a breadcrumb trail.
 */
async function deriveDirectoryBreadcrumbs(
  resourceId: DirectoryResourceID,
  driveId: DriveID
): Promise<FilePathBreadcrumb[]> {
  const breadcrumbs: FilePathBreadcrumb[] = [];
  let currentId: FolderID | FileID | null;
  let isFile = false;

  if (resourceId.startsWith(IDPrefixEnum.File)) {
    isFile = true;
    currentId = resourceId as FileID;
  } else {
    currentId = resourceId as FolderID;
  }

  while (currentId) {
    let record: any;
    if (isFile) {
      const results = await db.queryDrive(
        driveId,
        "SELECT id, name, parent_folder_id FROM files WHERE id = ?",
        [currentId]
      );
      if (results.length > 0) record = results[0];
      isFile = false; // Next iteration will be a folder
    } else {
      const results = await db.queryDrive(
        driveId,
        "SELECT id, name, parent_folder_id FROM folders WHERE id = ?",
        [currentId]
      );
      if (results.length > 0) record = results[0];
    }

    if (record) {
      breadcrumbs.unshift({
        resource_id: record.id,
        resource_name: record.name || "Root", // Root folder name is empty
        // TODO: REDACT Implement visibility preview logic by checking permissions.
        visibility_preview: [],
      });
      currentId = record.parent_folder_id;
    } else {
      currentId = null;
    }
  }

  return breadcrumbs;
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
  // TODO: PERMIT Add isOwner check similar to Rust's `OWNER_ID` check
) {
  // TODO: VALIDATE Add robust validation for each payload type, similar to `validate_body` in Rust.

  switch (action.action) {
    // =========================================================================
    // GET FILE
    // =========================================================================
    case DirectoryActionEnum.GET_FILE: {
      const payload = action.payload as GetFilePayload;
      const file = await getFileById(payload.id, driveId);
      if (!file) {
        throw new DirectoryActionError(404, "File not found");
      }

      const permissions = await checkDirectoryPermissions(
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

      // TODO: WEBHOOK Implement webhook logic from Rust
      // fireDirectoryWebhook(...)

      // TODO: PERMIT Implement share tracking logic from Rust
      // decodeShareTrackHash(...)
      // generateShareTrackHash(...)

      const breadcrumbs = await deriveDirectoryBreadcrumbs(
        `${IDPrefixEnum.File}${file.id}` as DirectoryResourceID,
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
      const folder = await getFolderById(payload.id, driveId);
      if (!folder) {
        throw new DirectoryActionError(404, "Folder not found");
      }

      const permissions = await checkDirectoryPermissions(
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

      // TODO: WEBHOOK Implement webhook and share tracking logic...

      const breadcrumbs = await deriveDirectoryBreadcrumbs(
        `${IDPrefixEnum.Folder}${folder.id}` as DirectoryResourceID,
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
      // TODO: PERMIT Permission check on parent folder for UPLOAD/EDIT/MANAGE

      const now = Date.now();
      const fileId = payload.id || GenerateID.File();
      const versionId = GenerateID.FileVersionID(); // Custom type in Rust, string here
      const parentFolder = await getFolderById(
        payload.parent_folder_uuid,
        driveId
      );
      if (!parentFolder) {
        throw new DirectoryActionError(404, "Parent folder not found");
      }
      const fullDirectoryPath = path.join(
        parentFolder.full_directory_path,
        payload.name
      );

      // TODO: DRIVE Implement resolve_naming_conflict logic
      // TODO: DRIVE Implement ensure_folder_structure logic

      const newFile: Omit<
        FileRecord,
        "version_id" | "file_version" | "prior_version"
      > = {
        id: fileId,
        name: payload.name, // Placeholder, needs conflict resolution
        parent_folder_uuid: payload.parent_folder_uuid,
        extension: payload.name.split(".").pop() || "",
        full_directory_path: fullDirectoryPath, // Placeholder
        labels: [],
        created_by: userId,
        created_at: now,
        disk_id: payload.disk_id,
        // TODO: DRIVE Get disk_type from the disk record
        disk_type: DiskTypeEnum.IcpCanister,
        file_size: payload.file_size,
        // TODO: DRIVE Generate raw_url based on endpoint
        raw_url: payload.raw_url || `http://localhost:3000/asset/${fileId}`,
        last_updated_date_ms: now,
        last_updated_by: userId,
        deleted: false,
        drive_id: driveId,
        expires_at: payload.expires_at ?? -1,
        has_sovereign_permissions: payload.has_sovereign_permissions ?? false,
        shortcut_to: payload.shortcut_to,
        upload_status: payload.raw_url
          ? UploadStatus.COMPLETED
          : UploadStatus.QUEUED,
        external_id: payload.external_id,
        external_payload: payload.external_payload,
        notes: payload.notes,
      };

      await dbHelpers.transaction("drive", driveId, (tx) => {
        tx.prepare(
          `
            INSERT INTO files (id, name, parent_folder_id, version_id, extension, full_directory_path, created_by_user_id, created_at, disk_id, disk_type, file_size, raw_url, last_updated_at, last_updated_by_user_id, is_deleted, drive_id, upload_status, expires_at, has_sovereign_permissions, shortcut_to_file_id, notes, external_id, external_payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          `
        ).run(
          newFile.id,
          newFile.name,
          newFile.parent_folder_uuid,
          versionId,
          newFile.extension,
          newFile.full_directory_path,
          newFile.created_by,
          newFile.created_at,
          newFile.disk_id,
          newFile.disk_type,
          newFile.file_size,
          newFile.raw_url,
          newFile.last_updated_date_ms,
          newFile.last_updated_by,
          newFile.deleted ? 1 : 0,
          newFile.drive_id,
          newFile.upload_status,
          newFile.expires_at,
          newFile.has_sovereign_permissions ? 1 : 0,
          newFile.shortcut_to,
          newFile.notes,
          newFile.external_id,
          newFile.external_payload
        );

        tx.prepare(
          `
            INSERT INTO file_versions (version_id, file_id, name, file_version, extension, created_by_user_id, created_at, disk_id, disk_type, file_size, raw_url, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          `
        ).run(
          versionId,
          newFile.id,
          newFile.name,
          1,
          newFile.extension,
          userId,
          now,
          newFile.disk_id,
          newFile.disk_type,
          newFile.file_size,
          newFile.raw_url,
          newFile.notes
        );
      });

      const finalFile = await getFileById(fileId, driveId);
      if (!finalFile)
        throw new DirectoryActionError(500, "Failed to retrieve created file.");

      return {
        CreateFile: {
          file: await castFileToFE(finalFile, userId, driveId),
          // TODO: DRIVE Implement upload response generation (e.g. presigned URLs for S3)
          upload: { url: "", fields: {} },
          notes: "File created successfully",
        },
      };
    }

    // =========================================================================
    // CREATE FOLDER
    // =========================================================================
    case DirectoryActionEnum.CREATE_FOLDER: {
      const payload = action.payload as CreateFolderPayload;
      // TODO: PERMIT Permission check on parent folder

      const now = Date.now();
      const folderId = payload.id || GenerateID.Folder();
      const parentFolder = await getFolderById(
        payload.parent_folder_uuid,
        driveId
      );
      if (!parentFolder) {
        throw new DirectoryActionError(404, "Parent folder not found");
      }
      const fullDirectoryPath = path.join(
        parentFolder.full_directory_path,
        payload.name
      );

      const newFolder = {
        id: folderId,
        name: payload.name, // Placeholder, needs conflict resolution
        parent_folder_id: payload.parent_folder_uuid,
        full_directory_path: fullDirectoryPath, // Placeholder
        created_by_user_id: userId,
        created_at: now,
        last_updated_at: now,
        last_updated_by_user_id: userId,
        disk_id: payload.disk_id,
        // TODO: DRIVE Get disk_type from disk record
        disk_type: DiskTypeEnum.IcpCanister,
        is_deleted: 0,
        expires_at: payload.expires_at ?? -1,
        drive_id: driveId,
        has_sovereign_permissions: payload.has_sovereign_permissions ? 1 : 0,
        shortcut_to_folder_id: payload.shortcut_to,
        notes: payload.notes,
        external_id: payload.external_id,
        external_payload: payload.external_payload,
      };

      await db.queryDrive(
        driveId,
        `
          INSERT INTO folders (id, name, parent_folder_id, full_directory_path, created_by_user_id, created_at, last_updated_at, last_updated_by_user_id, disk_id, disk_type, is_deleted, expires_at, drive_id, has_sovereign_permissions, shortcut_to_folder_id, notes, external_id, external_payload)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        Object.values(newFolder)
      );

      const finalFolder = await getFolderById(folderId, driveId);
      if (!finalFolder)
        throw new DirectoryActionError(
          500,
          "Failed to retrieve created folder."
        );

      return {
        CreateFolder: {
          folder: await castFolderToFE(finalFolder, userId, driveId),
          notes: "Folder created successfully",
        },
      };
    }

    // =========================================================================
    // UPDATE FILE / FOLDER
    // =========================================================================
    case DirectoryActionEnum.UPDATE_FILE: {
      const payload = action.payload as UpdateFilePayload;
      // TODO: PERMIT Permission checks
      // TODO: DRIVE Handle rename (path updates) separately and carefully
      // TODO: DRIVE Update versioning records if necessary
      const { id, ...updates } = payload;
      const updateEntries = Object.entries(updates).filter(
        ([_, v]) => v !== undefined
      );
      if (updateEntries.length === 0) {
        throw new DirectoryActionError(400, "No update fields provided.");
      }

      const setClause = updateEntries
        .map(([key, _]) => `${key} = ?`)
        .join(", ");
      const values = updateEntries.map(([_, val]) => val);

      await db.queryDrive(
        driveId,
        `UPDATE files SET ${setClause} WHERE id = ?`,
        [...values, id]
      );

      const updatedFile = await getFileById(id, driveId);
      if (!updatedFile)
        throw new DirectoryActionError(404, "File not found after update");

      return { UpdateFile: await castFileToFE(updatedFile, userId, driveId) };
    }

    case DirectoryActionEnum.UPDATE_FOLDER: {
      const payload = action.payload as UpdateFolderPayload;
      // TODO: PERMIT Permission checks
      // TODO: DRIVE Handle rename (path updates for folder AND all children) carefully
      const { id, ...updates } = payload;
      const updateEntries = Object.entries(updates).filter(
        ([_, v]) => v !== undefined
      );
      if (updateEntries.length === 0) {
        throw new DirectoryActionError(400, "No update fields provided.");
      }

      const setClause = updateEntries
        .map(([key, _]) => `${key} = ?`)
        .join(", ");
      const values = updateEntries.map(([_, val]) => val);

      await db.queryDrive(
        driveId,
        `UPDATE folders SET ${setClause} WHERE id = ?`,
        [...values, id]
      );

      const updatedFolder = await getFolderById(id, driveId);
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
      const file = await getFileById(payload.id, driveId);
      if (!file) throw new DirectoryActionError(404, "File not found");
      // TODO: PERMIT Full permission check logic

      if (payload.permanent) {
        await db.queryDrive(driveId, "DELETE FROM files WHERE id = ?", [
          payload.id,
        ]);
      } else {
        // Soft delete: move to trash
        // TODO: DRIVE Implement "trash" folder logic. For now, we just set the deleted flag.
        await db.queryDrive(
          driveId,
          `UPDATE files SET is_deleted = 1, restore_trash_prior_folder_id = parent_folder_id, parent_folder_id = (SELECT trash_folder_id FROM disks WHERE id = ?) WHERE id = ?`,
          [file.disk_id, payload.id]
        );
      }
      return { DeleteFile: { file_id: payload.id, path_to_trash: "TODO" } };
    }

    case DirectoryActionEnum.DELETE_FOLDER: {
      const payload = action.payload as DeleteFolderPayload;
      // TODO: PERMIT Permission checks
      // TODO: DRIVE Implement RECURSIVE deletion for permanent delete. This is critical.
      if (payload.permanent) {
        // This is a placeholder. A real implementation needs a recursive CTE or iterative logic.
        await db.queryDrive(driveId, "DELETE FROM folders WHERE id = ?", [
          payload.id,
        ]);
      } else {
        await db.queryDrive(
          driveId,
          "UPDATE folders SET is_deleted = 1 WHERE id = ?",
          [payload.id]
        );
      }
      return {
        DeleteFolder: {
          folder_id: payload.id,
          path_to_trash: "TODO",
          deleted_files: [],
          deleted_folders: [],
        },
      };
    }

    // =========================================================================
    // COPY / MOVE / RESTORE
    // =========================================================================
    case DirectoryActionEnum.COPY_FILE: {
      // TODO: DRIVE Proper implementation for CopyFile
      console.warn("TODO: DRIVE COPY_FILE is not fully implemented.");
      const payload = action.payload as CopyFilePayload;
      const file = await getFileById(payload.id, driveId);
      if (!file) throw new DirectoryActionError(404, "File not found");
      return { CopyFile: await castFileToFE(file, userId, driveId) };
    }

    case DirectoryActionEnum.COPY_FOLDER: {
      // TODO: DRIVE Proper implementation for CopyFolder
      console.warn("TODO: DRIVE COPY_FOLDER is not fully implemented.");
      const payload = action.payload as CopyFolderPayload;
      const folder = await getFolderById(payload.id, driveId);
      if (!folder) throw new DirectoryActionError(404, "Folder not found");
      return { CopyFolder: await castFolderToFE(folder, userId, driveId) };
    }

    case DirectoryActionEnum.MOVE_FILE: {
      // TODO: DRIVE Proper implementation for MoveFile
      console.warn("TODO: DRIVE MOVE_FILE is not fully implemented.");
      const payload = action.payload as MoveFilePayload;
      const file = await getFileById(payload.id, driveId);
      if (!file) throw new DirectoryActionError(404, "File not found");
      return { MoveFile: await castFileToFE(file, userId, driveId) };
    }

    case DirectoryActionEnum.MOVE_FOLDER: {
      // TODO: DRIVE Proper implementation for MoveFolder
      console.warn("TODO: DRIVE MOVE_FOLDER is not fully implemented.");
      const payload = action.payload as MoveFolderPayload;
      const folder = await getFolderById(payload.id, driveId);
      if (!folder) throw new DirectoryActionError(404, "Folder not found");
      return { MoveFolder: await castFolderToFE(folder, userId, driveId) };
    }

    case DirectoryActionEnum.RESTORE_TRASH: {
      // TODO: DRIVE Proper implementation for RestoreTrash
      console.warn("TODO: DRIVE RESTORE_TRASH is not fully implemented.");
      const payload = action.payload as RestoreTrashPayload;
      await db.queryDrive(
        driveId,
        "UPDATE files SET is_deleted = 0 WHERE id = ?",
        [payload.id]
      );
      await db.queryDrive(
        driveId,
        "UPDATE folders SET is_deleted = 0 WHERE id = ?",
        [payload.id]
      );
      return { RestoreTrash: { restored_files: [], restored_folders: [] } };
    }

    default:
      throw new DirectoryActionError(
        400,
        "Unsupported or unimplemented directory action"
      );
  }
}
