import {
  DriveID,
  UserID,
  ListDirectoryRequest,
  DirectoryListResponse,
  FileRecord,
  FolderRecord,
  FolderRecordFE,
  FileRecordFE,
  IResponseListDirectory,
  IRequestCreateFile, // Assuming this type exists for file creation params
  IRequestCreateFolder, // Assuming this type exists for folder creation params
  FileID,
  FolderID,
  FileConflictResolutionEnum,
  RestoreTrashPayload,
  DirectoryActionResult,
  RestoreTrashResponse,
  DiskUploadResponse,
  DriveFullFilePath,
  GenerateID,
  UploadStatus,
} from "@officexapp/types";
import { db } from "../database";
import { permissionsService } from "../permissions"; // TODO: Implement permissions service
import { diskService } from "../disk"; // TODO: Implement disk service
import * as internals from "./internals";

/**
 * Fetches the contents (files and folders) of a specific directory.
 * @param driveId - The ID of the drive.
 * @param userId - The ID of the user making the request.
 * @param config - The request configuration for listing the directory.
 * @returns The directory listing response.
 */
export async function listDirectory(
  driveId: DriveID,
  userId: UserID,
  config: ListDirectoryRequest
): Promise<IResponseListDirectory> {
  const { folder_id, path, page_size = 50, cursor } = config;

  let targetFolder: FolderRecord | undefined;

  if (folder_id) {
    const result = await db.queryDrive(
      driveId,
      "SELECT * FROM folders WHERE id = ?",
      [folder_id]
    );
    targetFolder = result[0] as FolderRecord;
  } else if (path) {
    const translation = await internals.translatePathToId(
      driveId,
      path as DriveFullFilePath
    );
    targetFolder = translation.folder;
  } else {
    // TODO: Implement fetch_root_shortcuts_of_user logic
    console.warn("TODO: listDirectory for root shortcuts is not implemented.");
    return {
      ok: {
        data: {
          folders: [],
          files: [],
          total_files: 0,
          total_folders: 0,
          breadcrumbs: [],
        },
      },
    };
  }

  if (!targetFolder) {
    throw new Error("Folder not found");
  }

  // TODO: Add permission check for targetFolder and userId

  const offset = cursor ? parseInt(cursor, 10) : 0;

  const foldersResult = await db.queryDrive(
    driveId,
    "SELECT * FROM folders WHERE parent_folder_id = ? LIMIT ? OFFSET ?",
    [targetFolder.id, page_size, offset]
  );
  const filesResult = await db.queryDrive(
    driveId,
    "SELECT * FROM files WHERE parent_folder_id = ? LIMIT ? OFFSET ?",
    [targetFolder.id, page_size, offset]
  );

  // TODO: The logic in Rust paginates through a combined list. This implementation is simplified.
  // A more accurate migration would require a more complex query or multiple queries with logic to merge and paginate.

  const foldersFE: FolderRecordFE[] = await Promise.all(
    (foldersResult as FolderRecord[]).map((f) =>
      permissionsService.castFolderFE(driveId, userId, f)
    )
  );

  const filesFE: FileRecordFE[] = await Promise.all(
    (filesResult as FileRecord[]).map((f) =>
      permissionsService.castFileFE(driveId, userId, f)
    )
  );

  const [totalFolders] = await db.queryDrive(
    driveId,
    "SELECT COUNT(id) as count FROM folders WHERE parent_folder_id = ?",
    [targetFolder.id]
  );
  const [totalFiles] = await db.queryDrive(
    driveId,
    "SELECT COUNT(id) as count FROM files WHERE parent_folder_id = ?",
    [targetFolder.id]
  );

  const breadcrumbs = await permissionsService.deriveDirectoryBreadcrumbs(
    driveId,
    userId,
    { folder: targetFolder.id }
  );

  const nextCursor =
    filesResult.length + foldersResult.length >= page_size
      ? (offset + page_size).toString()
      : null;

  return {
    ok: {
      data: {
        folders: foldersFE,
        files: filesFE,
        total_folders: (totalFolders as any).count,
        total_files: (totalFiles as any).count,
        cursor: nextCursor,
        breadcrumbs,
      },
    },
  };
}

/**
 * Creates a new file record and generates an upload URL if applicable.
 * @returns A tuple of the created FileRecord and a DiskUploadResponse.
 */
export async function createFile(
  driveId: DriveID,
  userId: UserID,
  params: IRequestCreateFile // TODO: Define this type based on Rust function signature
): Promise<[FileRecord, DiskUploadResponse]> {
  const {
    file_path,
    disk_id,
    file_size,
    expires_at = -1,
    file_conflict_resolution,
    shortcut_to,
    external_id,
    external_payload,
    raw_url,
    notes,
  } = params;

  const sanitizedFilePath = internals.sanitizeFilePath(file_path);
  const [folderPath, fileName] = internals.splitPath(sanitizedFilePath);

  // This is a simplified version. The Rust code has complex logic to handle different conflict resolutions.
  const [finalName, finalPath] = await internals.resolveNamingConflict(
    driveId,
    folderPath,
    fileName,
    false,
    file_conflict_resolution
  );

  if (!finalName) {
    throw new Error("File already exists and resolution is KEEP_ORIGINAL.");
  }

  const parentFolderId = await internals.ensureFolderStructure(
    driveId,
    folderPath,
    disk_id,
    userId
  );

  // TODO: Add full versioning logic as seen in Rust. This is a simplified insertion.
  const newFileId = GenerateID.File();
  const extension = finalName.split(".").pop() || "";
  const now = Date.now();
  const disk = await diskService.getDisk(driveId, disk_id);

  const fileRecord: FileRecord = {
    id: newFileId,
    name: finalName,
    parent_folder_uuid: parentFolderId,
    full_directory_path: finalPath,
    // ... other fields initialized
    created_by: userId,
    created_at: now,
    last_updated_by: userId,
    last_updated_date_ms: now,
    disk_id,
    disk_type: disk.disk_type,
    file_size,
    raw_url:
      raw_url ?? internals.formatFileAssetPath(driveId, newFileId, extension),
    upload_status: raw_url ? UploadStatus.COMPLETED : UploadStatus.QUEUED,
    // ... fill rest of the fields from params
  };

  await db.queryDrive(
    driveId,
    `INSERT INTO files (id, name, parent_folder_id, version_id, extension, full_directory_path, created_by_user_id, created_at, disk_id, disk_type, file_size, raw_url, last_updated_at, last_updated_by_user_id, drive_id, upload_status, expires_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      fileRecord.id,
      fileRecord.name,
      fileRecord.parent_folder_uuid,
      GenerateID.FileVersionID(),
      extension,
      fileRecord.full_directory_path,
      fileRecord.created_by,
      fileRecord.created_at,
      fileRecord.disk_id,
      fileRecord.disk_type,
      fileRecord.file_size,
      fileRecord.raw_url,
      fileRecord.last_updated_date_ms,
      fileRecord.last_updated_by,
      driveId,
      fileRecord.upload_status,
      expires_at,
    ]
  );

  // TODO: Generate a real upload response from a disk/storage service
  const uploadResponse: DiskUploadResponse = {
    url: "",
    fields: {},
  };

  return [fileRecord, uploadResponse];
}

/**
 * Creates a new folder.
 * @returns The created FolderRecord.
 */
export async function createFolder(
  driveId: DriveID,
  userId: UserID,
  params: IRequestCreateFolder // TODO: Define this type based on Rust function signature
): Promise<FolderRecord> {
  const { full_directory_path, disk_id, ...otherParams } = params;

  const folderId = await internals.ensureFolderStructure(
    driveId,
    full_directory_path,
    disk_id,
    userId,
    otherParams.has_sovereign_permissions,
    otherParams.external_id,
    otherParams.external_payload,
    otherParams.shortcut_to,
    otherParams.notes
  );

  const [folder] = await db.queryDrive(
    driveId,
    "SELECT * FROM folders WHERE id = ?",
    [folderId]
  );
  return folder as FolderRecord;
}

/**
 * Deletes a file or folder, either permanently or by moving it to the trash.
 * @param driveId - The ID of the drive.
 * @param resourceId - The ID of the file or folder to delete.
 * @param permanent - If true, permanently deletes the item. Otherwise, moves to trash.
 */
export async function deleteResource(
  driveId: DriveID,
  resourceId: FileID | FolderID,
  permanent: boolean
): Promise<void> {
  return db.transaction("drive", driveId, async (tx) => {
    // This is a highly simplified version. The Rust code has complex recursive logic.
    // A full implementation would require a recursive function to handle folder contents.

    if (permanent) {
      if (resourceId.startsWith("FileID_")) {
        // TODO: Delete file versions from file_versions table
        await tx.prepare("DELETE FROM files WHERE id = ?").run(resourceId);
      } else {
        // TODO: Recursively delete all children files and folders
        console.warn("TODO: Recursive folder deletion is not implemented.");
        await tx.prepare("DELETE FROM folders WHERE id = ?").run(resourceId);
      }
    } else {
      // Move to trash
      const diskIdResult = await tx
        .prepare(
          "SELECT disk_id FROM " +
            (resourceId.startsWith("FileID_") ? "files" : "folders") +
            " WHERE id = ?"
        )
        .get(resourceId);
      const diskId = (diskIdResult as any)?.disk_id;
      if (!diskId) throw new Error("Resource not found");

      const trashFolderResult = await tx
        .prepare(`SELECT id FROM folders WHERE disk_id = ? AND name = '.trash'`)
        .get(diskId);
      const trashFolderId = (trashFolderResult as any)?.id;
      if (!trashFolderId) throw new Error("Trash folder not found");

      // Update parent to trash folder and set restore_trash_prior_folder_id
      if (resourceId.startsWith("FileID_")) {
        await tx
          .prepare(
            "UPDATE files SET restore_trash_prior_folder_id = parent_folder_id, parent_folder_id = ?, is_deleted = 1 WHERE id = ?"
          )
          .run(trashFolderId, resourceId);
      } else {
        // TODO: Recursively move children to trash as well
        console.warn("TODO: Recursive move-to-trash is not implemented.");
        await tx
          .prepare(
            "UPDATE folders SET restore_trash_prior_folder_id = parent_folder_id, parent_folder_id = ?, is_deleted = 1 WHERE id = ?"
          )
          .run(trashFolderId, resourceId);
      }
    }
  });
}

/**
 * Moves a file to a new destination folder.
 */
export async function moveFile(
  driveId: DriveID,
  fileId: FileID,
  destinationFolderId: FolderID,
  resolution: FileConflictResolutionEnum
): Promise<FileRecord> {
  return db.transaction("drive", driveId, async (tx) => {
    const file = (await tx
      .prepare("SELECT * FROM files WHERE id = ?")
      .get(fileId)) as FileRecord;
    const destFolder = (await tx
      .prepare("SELECT * FROM folders WHERE id = ?")
      .get(destinationFolderId)) as FolderRecord;

    if (!file || !destFolder) throw new Error("File or destination not found.");
    if (file.disk_id !== destFolder.disk_id)
      throw new Error("Cannot move between disks.");

    const [finalName, finalPath] = await internals.resolveNamingConflict(
      driveId,
      destFolder.full_directory_path,
      file.name,
      false,
      resolution
    );

    if (!finalName) {
      throw new Error(
        "A file with the same name already exists in the destination."
      );
    }

    await tx
      .prepare(
        "UPDATE files SET name = ?, full_directory_path = ?, parent_folder_id = ? WHERE id = ?"
      )
      .run(finalName, finalPath, destinationFolderId, fileId);

    const [updatedFile] = await tx
      .prepare("SELECT * FROM files WHERE id = ?")
      .all(fileId);
    return updatedFile as FileRecord;
  });
}

/**
 * Moves a folder to a new destination folder.
 */
export async function moveFolder(
  driveId: DriveID,
  folderId: FolderID,
  destinationFolderId: FolderID,
  resolution: FileConflictResolutionEnum
): Promise<FolderRecord> {
  return db.transaction("drive", driveId, async (tx) => {
    const folder = (await tx
      .prepare("SELECT * FROM folders WHERE id = ?")
      .get(folderId)) as FolderRecord;
    const destFolder = (await tx
      .prepare("SELECT * FROM folders WHERE id = ?")
      .get(destinationFolderId)) as FolderRecord;

    if (!folder || !destFolder)
      throw new Error("Folder or destination not found.");
    if (folder.disk_id !== destFolder.disk_id)
      throw new Error("Cannot move between disks.");

    // TODO: Add circular reference check from Rust logic

    const [finalName, finalPath] = await internals.resolveNamingConflict(
      driveId,
      destFolder.full_directory_path,
      folder.name,
      true,
      resolution
    );
    if (!finalName) {
      throw new Error(
        "A folder with the same name already exists in the destination."
      );
    }

    const oldPath = folder.full_directory_path;
    await tx
      .prepare(
        "UPDATE folders SET name = ?, full_directory_path = ?, parent_folder_id = ? WHERE id = ?"
      )
      .run(finalName, finalPath, destinationFolderId, folderId);

    // Recursively update paths of all children
    await internals.updateSubfolderPaths(driveId, folderId, oldPath, finalPath);

    const [updatedFolder] = await tx
      .prepare("SELECT * FROM folders WHERE id = ?")
      .all(folderId);
    return updatedFolder as FolderRecord;
  });
}

/**
 * Restores a file or folder from the trash.
 * @param driveId - The ID of the drive.
 * @param resourceId - The ID of the file or folder to restore.
 * @param payload - The restore configuration.
 * @returns The result of the restore action.
 */
export async function restoreFromTrash(
  driveId: DriveID,
  resourceId: FileID | FolderID,
  payload: RestoreTrashPayload
): Promise<DirectoryActionResult> {
  return db.transaction("drive", driveId, async (tx) => {
    const isFile = resourceId.startsWith("FileID_");
    const tableName = isFile ? "files" : "folders";

    const resource: any = await tx
      .prepare(`SELECT * FROM ${tableName} WHERE id = ? AND is_deleted = 1`)
      .get(resourceId);
    if (!resource) {
      throw new Error("Resource not found in trash.");
    }

    let destinationFolderId = resource.restore_trash_prior_folder_id;

    // This is a simplified version of finding/creating the restore path
    if (payload.restore_to_folder_path) {
      const translation = await internals.translatePathToId(
        driveId,
        payload.restore_to_folder_path as DriveFullFilePath
      );
      if (!translation.folder)
        throw new Error("Custom restore path not found.");
      destinationFolderId = translation.folder.id;
    }

    if (!destinationFolderId) {
      const disk = await diskService.getDisk(driveId, resource.disk_id);
      destinationFolderId = disk.root_folder_id;
    }

    // Simplified restore logic: update path, parent, and flags.
    // A full implementation would use moveFile/moveFolder and handle children recursively.
    await tx
      .prepare(
        `UPDATE ${tableName} SET is_deleted = 0, restore_trash_prior_folder_id = NULL, parent_folder_id = ? WHERE id = ?`
      )
      .run(destinationFolderId, resourceId);

    // TODO: Full implementation should use moveFile/moveFolder and return a proper RestoreTrashResponse
    const response: RestoreTrashResponse = {
      restored_folders: isFile ? [] : [resourceId as FolderID],
      restored_files: isFile ? [resourceId as FileID] : [],
    };

    return { RestoreTrash: response };
  });
}

// TODO: Implement copyFile and copyFolder. These are complex operations
// involving deep object copies and potentially platform-specific logic
// for cloud storage backends (like S3 CopyObject).
