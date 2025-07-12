// src/services/directory/drive.ts

import {
  DriveID,
  UserID,
  FileRecord,
  FolderRecord,
  FolderRecordFE,
  FileRecordFE,
  FileID,
  FolderID,
  FileConflictResolutionEnum,
  RestoreTrashPayload,
  DiskUploadResponse,
  DriveFullFilePath,
  GenerateID,
  UploadStatus,
  IResponseListDirectory,
  IRequestListDirectory,
  CreateFilePayload,
  CreateFolderPayload,
  RestoreTrashResponse,
  IDPrefixEnum,
} from "@officexapp/types";
import { db, dbHelpers } from "../database";
import * as internals from "./internals";
import type { Database } from "better-sqlite3";

// TODO: PERMIT These should be real services. Using mocks for now.
const permissionsService = {
  castFileFE: async (
    driveId: DriveID,
    userId: UserID,
    file: FileRecord
  ): Promise<FileRecordFE> => {
    // Mock implementation
    return {
      ...file,
      clipped_directory_path: "mock/path",
      permission_previews: [],
    };
  },
  castFolderFE: async (
    driveId: DriveID,
    userId: UserID,
    folder: FolderRecord
  ): Promise<FolderRecordFE> => {
    // Mock implementation
    return {
      ...folder,
      clipped_directory_path: "mock/path",
      permission_previews: [],
    };
  },
  deriveDirectoryBreadcrumbs: async (
    driveId: DriveID,
    userId: UserID,
    resource: { file?: FileID; folder?: FolderID }
  ): Promise<any[]> => {
    // Mock implementation
    return [];
  },
};
const diskService = {
  getDisk: async (driveId: DriveID, diskId: string): Promise<any> => {
    // Mock implementation
    const [disk] = await db.queryDrive(
      driveId,
      "SELECT * FROM disks WHERE id = ?",
      [diskId]
    );
    return disk;
  },
};

/**
 * Fetches the contents (files and folders) of a specific directory.
 */
export async function listDirectory(
  driveId: DriveID,
  userId: UserID,
  config: IRequestListDirectory
): Promise<IResponseListDirectory> {
  // FIX: Return the data object directly
  const { folder_id, path, page_size = 50, cursor } = config;

  let targetFolder: FolderRecord | undefined | null;

  if (folder_id) {
    [targetFolder] = await db.queryDrive(
      driveId,
      "SELECT * FROM folders WHERE id = ?",
      [folder_id]
    );
  } else if (path) {
    const translation = await internals.translatePathToId(
      driveId,
      path as DriveFullFilePath
    );
    targetFolder = translation.folder;
  } else {
    // TODO: DRIVE Implement fetch_root_shortcuts_of_user logic for disk_id based listing
    console.warn(
      "TODO: DRIVE listDirectory for root shortcuts is not implemented."
    );
    return {
      ok: {
        data: {
          folders: [],
          files: [],
          total_files: 0,
          total_folders: 0,
          breadcrumbs: [],
          cursor: null,
        },
      },
    };
  }

  if (!targetFolder) {
    throw new Error("Folder not found");
  }

  // TODO: PERMIT Add permission check for targetFolder and userId
  const offset = cursor ? parseInt(cursor, 10) : 0;

  const foldersResult = await db.queryDrive(
    driveId,
    "SELECT * FROM folders WHERE parent_folder_id = ? AND is_deleted = 0 LIMIT ? OFFSET ?",
    [targetFolder.id, page_size, offset]
  );
  const filesResult = await db.queryDrive(
    driveId,
    "SELECT * FROM files WHERE parent_folder_id = ? AND is_deleted = 0 LIMIT ? OFFSET ?",
    [targetFolder.id, page_size, offset]
  );

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

  const [{ count: totalFolders }] = await db.queryDrive(
    driveId,
    "SELECT COUNT(id) as count FROM folders WHERE parent_folder_id = ? AND is_deleted = 0",
    [targetFolder.id]
  );
  const [{ count: totalFiles }] = await db.queryDrive(
    driveId,
    "SELECT COUNT(id) as count FROM files WHERE parent_folder_id = ? AND is_deleted = 0",
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

  // FIX: Return the DirectoryListResponse object directly, matching the function's return type.
  // The route handler should wrap this in the { ok: { data: ... } } structure.
  return {
    ok: {
      data: {
        folders: foldersFE,
        files: filesFE,
        total_folders: totalFolders,
        total_files: totalFiles,
        cursor: nextCursor,
        breadcrumbs,
      },
    },
  };
}

/**
 * Creates a new file record and generates an upload URL if applicable.
 */
export async function createFile(
  driveId: DriveID,
  userId: UserID,
  params: CreateFilePayload
): Promise<[FileRecord, DiskUploadResponse]> {
  const {
    parent_folder_uuid,
    disk_id,
    file_size,
    expires_at = -1,
    file_conflict_resolution,
    ...rest
  } = params;

  const parentFolder = (
    await db.queryDrive(driveId, "SELECT * FROM folders WHERE id = ?", [
      parent_folder_uuid,
    ])
  )[0] as FolderRecord;
  if (!parentFolder) throw new Error("Parent folder not found.");

  const [finalName, finalPath] = await internals.resolveNamingConflict(
    driveId,
    parentFolder.full_directory_path,
    params.name,
    false,
    file_conflict_resolution
  );

  if (!finalName) {
    throw new Error("File already exists and resolution is KEEP_ORIGINAL.");
  }

  const newFileId = params.id || GenerateID.File();
  const extension = finalName.split(".").pop() || "";
  const now = Date.now();
  const disk = await diskService.getDisk(driveId, disk_id);
  const versionId = GenerateID.FileVersionID();

  const fileRecord: FileRecord = {
    id: newFileId,
    name: finalName,
    parent_folder_uuid,
    version_id: versionId,
    file_version: 1,
    prior_version: undefined,
    next_version: undefined,
    extension: extension,
    full_directory_path: finalPath,
    labels: [], // TODO: REDACT Handle labels insertion
    created_by: userId,
    created_at: now,
    disk_id: disk_id,
    disk_type: disk.disk_type,
    file_size: file_size,
    raw_url:
      params.raw_url ??
      internals.formatFileAssetPath(driveId, newFileId, extension),
    last_updated_date_ms: now,
    last_updated_by: userId,
    deleted: false,
    drive_id: driveId,
    expires_at: expires_at,
    has_sovereign_permissions: params.has_sovereign_permissions ?? false,
    upload_status: params.raw_url
      ? UploadStatus.COMPLETED
      : UploadStatus.QUEUED,
    notes: params.notes,
    shortcut_to: params.shortcut_to,
    external_id: params.external_id,
    external_payload: params.external_payload,
  };

  await dbHelpers.transaction("drive", driveId, (tx: Database) => {
    tx.prepare(
      `INSERT INTO files (id, name, parent_folder_id, version_id, extension, full_directory_path, created_by_user_id, created_at, disk_id, disk_type, file_size, raw_url, last_updated_at, last_updated_by_user_id, drive_id, upload_status, expires_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(
      fileRecord.id,
      fileRecord.name,
      fileRecord.parent_folder_uuid,
      fileRecord.version_id,
      fileRecord.extension,
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
      expires_at
    );
    // Create first version record
    tx.prepare(
      `INSERT INTO file_versions(version_id, file_id, name, file_version, extension, created_by_user_id, created_at, disk_id, disk_type, file_size, raw_url, notes)
           VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(
      versionId,
      newFileId,
      finalName,
      1,
      extension,
      userId,
      now,
      disk_id,
      disk.disk_type,
      file_size,
      fileRecord.raw_url,
      fileRecord.notes
    );
  });

  // TODO: DRIVE Generate a real upload response from a disk/storage service
  const uploadResponse: DiskUploadResponse = { url: "", fields: {} };

  return [fileRecord, uploadResponse];
}

/**
 * Creates a new folder.
 */
export async function createFolder(
  driveId: DriveID,
  userId: UserID,
  params: CreateFolderPayload
): Promise<FolderRecord> {
  const { parent_folder_uuid, disk_id, ...otherParams } = params;

  const parentFolder = (
    await db.queryDrive(driveId, "SELECT * FROM folders WHERE id = ?", [
      parent_folder_uuid,
    ])
  )[0];
  if (!parentFolder) throw new Error("Parent folder not found.");

  const [finalName, finalPath] = await internals.resolveNamingConflict(
    driveId,
    parentFolder.full_directory_path,
    params.name,
    true,
    params.file_conflict_resolution
  );

  if (!finalName) {
    throw new Error(
      "A folder with this name already exists and resolution strategy prevents creation."
    );
  }

  const folderId = await internals.ensureFolderStructure(
    driveId,
    finalPath,
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
 * Deletes a file or folder.
 */
export async function deleteResource(
  driveId: DriveID,
  resourceId: FileID | FolderID,
  permanent: boolean
): Promise<void> {
  // TODO: DRIVE This is a highly simplified version. The Rust code has complex recursive logic.
  // A full implementation would require a recursive function to handle folder contents.
  return dbHelpers.transaction("drive", driveId, async (tx: Database) => {
    if (permanent) {
      if (resourceId.startsWith(IDPrefixEnum.File)) {
        tx.prepare("DELETE FROM file_versions WHERE file_id = ?").run(
          resourceId
        );
        tx.prepare("DELETE FROM files WHERE id = ?").run(resourceId);
      } else {
        console.warn(
          "TODO: DRIVE Recursive folder deletion is not implemented."
        );
        tx.prepare("DELETE FROM folders WHERE id = ?").run(resourceId);
      }
    } else {
      // Move to trash
      const isFile = resourceId.startsWith(IDPrefixEnum.File);
      const tableName = isFile ? "files" : "folders";
      const resource: any = tx
        .prepare(
          `SELECT disk_id, parent_folder_id FROM ${tableName} WHERE id = ?`
        )
        .get(resourceId);
      if (!resource) throw new Error("Resource not found");

      const trashFolder: any = tx
        .prepare(`SELECT id FROM folders WHERE disk_id = ? AND name = '.trash'`)
        .get(resource.disk_id);
      if (!trashFolder)
        throw new Error("Trash folder not found for this disk.");

      tx.prepare(
        `UPDATE ${tableName} SET restore_trash_prior_folder_id = ?, parent_folder_id = ?, is_deleted = 1 WHERE id = ?`
      ).run(resource.parent_folder_id, trashFolder.id, resourceId);
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
  return dbHelpers.transaction("drive", driveId, (tx: Database) => {
    const file = tx
      .prepare("SELECT * FROM files WHERE id = ?")
      .get(fileId) as FileRecord;
    const destFolder = tx
      .prepare("SELECT * FROM folders WHERE id = ?")
      .get(destinationFolderId) as FolderRecord;
    if (!file || !destFolder) throw new Error("File or destination not found.");
    if (file.disk_id !== destFolder.disk_id)
      throw new Error("Cannot move between disks.");

    // This is a synchronous call inside a transaction, so we can't use the async version.
    // For a full migration, resolveNamingConflict might need a synchronous version or this logic must be moved.
    // TODO: DRIVE Properly implement resolveNamingConflict logic here synchronously.
    const finalName = file.name;
    const finalPath = `${destFolder.full_directory_path.replace(/\/$/, "")}/${finalName}`;

    tx.prepare(
      "UPDATE files SET name = ?, full_directory_path = ?, parent_folder_id = ? WHERE id = ?"
    ).run(finalName, finalPath, destinationFolderId, fileId);
    const updatedFile = tx
      .prepare("SELECT * FROM files WHERE id = ?")
      .get(fileId) as FileRecord;
    return updatedFile;
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
  return dbHelpers.transaction("drive", driveId, async (tx: Database) => {
    const folder = tx
      .prepare("SELECT * FROM folders WHERE id = ?")
      .get(folderId) as FolderRecord;
    const destFolder = tx
      .prepare("SELECT * FROM folders WHERE id = ?")
      .get(destinationFolderId) as FolderRecord;
    if (!folder || !destFolder)
      throw new Error("Folder or destination not found.");
    if (folder.disk_id !== destFolder.disk_id)
      throw new Error("Cannot move between disks.");

    // TODO: DRIVE Add circular reference check from Rust logic
    // TODO: DRIVE Synchronous version of resolveNamingConflict needed for transactions
    const finalName = folder.name;
    const finalPath = `${destFolder.full_directory_path.replace(/\/$/, "")}/${finalName}/`;

    const oldPath = folder.full_directory_path;
    tx.prepare(
      "UPDATE folders SET name = ?, full_directory_path = ?, parent_folder_id = ? WHERE id = ?"
    ).run(finalName, finalPath, destinationFolderId, folderId);

    // This recursive update needs to be handled carefully with transactions.
    await internals.updateSubfolderPaths(driveId, folderId, oldPath, finalPath);

    const updatedFolder = tx
      .prepare("SELECT * FROM folders WHERE id = ?")
      .get(folderId) as FolderRecord;
    return updatedFolder;
  });
}

/**
 * Restores a file or folder from the trash.
 */
export async function restoreFromTrash(
  driveId: DriveID,
  payload: RestoreTrashPayload
): Promise<RestoreTrashResponse> {
  return dbHelpers.transaction("drive", driveId, async (tx: Database) => {
    const isFile = payload.id.startsWith(IDPrefixEnum.File);
    const tableName = isFile ? "files" : "folders";

    const resource: any = tx
      .prepare(`SELECT * FROM ${tableName} WHERE id = ? AND is_deleted = 1`)
      .get(payload.id);
    if (!resource) {
      throw new Error("Resource not found in trash.");
    }

    let destinationFolderId = resource.restore_trash_prior_folder_id;
    if (!destinationFolderId) {
      throw new Error(
        "Resource is in trash but has no prior location to restore to."
      );
    }

    // TODO: DRIVE Full implementation should use moveFile/moveFolder to handle conflicts.
    // This simplified version just puts it back.
    tx.prepare(
      `UPDATE ${tableName} SET is_deleted = 0, restore_trash_prior_folder_id = NULL, parent_folder_id = ? WHERE id = ?`
    ).run(destinationFolderId, payload.id);

    const response: RestoreTrashResponse = {
      restored_folders: isFile ? [] : [payload.id as FolderID],
      restored_files: isFile ? [payload.id as FileID] : [],
    };

    return response;
  });
}

/**
 * Retrieves metadata for a specific folder.
 *
 * @param orgId The organization ID (also used as driveId for database access).
 * @param folderId The ID of the folder to retrieve.
 * @returns A promise that resolves to the folder's metadata, or null if not found.
 */
export async function getFolderMetadata(
  orgId: string,
  folderId: FolderID
): Promise<{
  id: FolderID;
  name: string;
  parent_folder_uuid: FolderID | undefined;
  full_directory_path: string;
  created_by_user_id: string;
  created_at: number;
  last_updated_at: number;
  last_updated_by_user_id: string;
  disk_id: string;
  disk_type: string;
  is_deleted: boolean;
  expires_at: number;
  drive_id: string;
  restore_trash_prior_folder_id: FolderID | undefined;
  has_sovereign_permissions: boolean;
  shortcut_to_folder_id: FolderID | undefined;
  notes: string | undefined;
  external_id: string | undefined;
  external_payload: string | undefined;
} | null> {
  const rows = await db.queryDrive(
    orgId,
    `SELECT
      id,
      name,
      parent_folder_id,
      full_directory_path,
      created_by_user_id,
      created_at,
      last_updated_at,
      last_updated_by_user_id,
      disk_id,
      disk_type,
      is_deleted,
      expires_at,
      drive_id,
      restore_trash_prior_folder_id,
      has_sovereign_permissions,
      shortcut_to_folder_id,
      notes,
      external_id,
      external_payload
    FROM folders
    WHERE id = ?`,
    [folderId]
  );

  if (rows.length === 0) {
    return null;
  }

  const row = rows[0];
  return {
    id: row.id as FolderID,
    name: row.name,
    parent_folder_uuid: row.parent_folder_id as FolderID | undefined,
    full_directory_path: row.full_directory_path,
    created_by_user_id: row.created_by_user_id,
    created_at: row.created_at,
    last_updated_at: row.last_updated_at,
    last_updated_by_user_id: row.last_updated_by_user_id,
    disk_id: row.disk_id,
    disk_type: row.disk_type,
    is_deleted: row.is_deleted === 1,
    expires_at: row.expires_at,
    drive_id: row.drive_id,
    restore_trash_prior_folder_id: row.restore_trash_prior_folder_id as
      | FolderID
      | undefined,
    has_sovereign_permissions: row.has_sovereign_permissions === 1,
    shortcut_to_folder_id: row.shortcut_to_folder_id as FolderID | undefined,
    notes: row.notes,
    external_id: row.external_id,
    external_payload: row.external_payload,
  };
}

/**
 * Retrieves metadata for a specific file.
 *
 * @param orgId The organization ID (also used as driveId for database access).
 * @param fileId The ID of the file to retrieve.
 * @returns A promise that resolves to the file's metadata, or null if not found.
 */
export async function getFileMetadata(
  orgId: string,
  fileId: FileID
): Promise<{
  id: FileID;
  name: string;
  parent_folder_uuid: FolderID;
  version_id: string;
  extension: string;
  full_directory_path: string;
  created_by_user_id: string;
  created_at: number;
  disk_id: string;
  disk_type: string;
  file_size: number;
  raw_url: string;
  last_updated_at: number;
  last_updated_by_user_id: string;
  is_deleted: boolean;
  drive_id: string;
  upload_status: string;
  expires_at: number;
  restore_trash_prior_folder_id: FolderID | undefined;
  has_sovereign_permissions: boolean;
  shortcut_to_file_id: FileID | undefined;
  notes: string | undefined;
  external_id: string | undefined;
  external_payload: string | undefined;
} | null> {
  const rows = await db.queryDrive(
    orgId,
    `SELECT
      id,
      name,
      parent_folder_id,
      version_id,
      extension,
      full_directory_path,
      created_by_user_id,
      created_at,
      disk_id,
      disk_type,
      file_size,
      raw_url,
      last_updated_at,
      last_updated_by_user_id,
      is_deleted,
      drive_id,
      upload_status,
      expires_at,
      restore_trash_prior_folder_id,
      has_sovereign_permissions,
      shortcut_to_file_id,
      notes,
      external_id,
      external_payload
    FROM files
    WHERE id = ?`,
    [fileId]
  );

  if (rows.length === 0) {
    return null;
  }

  const row = rows[0];
  return {
    id: row.id as FileID,
    name: row.name,
    parent_folder_uuid: row.parent_folder_id as FolderID,
    version_id: row.version_id,
    extension: row.extension,
    full_directory_path: row.full_directory_path,
    created_by_user_id: row.created_by_user_id,
    created_at: row.created_at,
    disk_id: row.disk_id,
    disk_type: row.disk_type,
    file_size: row.file_size,
    raw_url: row.raw_url,
    last_updated_at: row.last_updated_at,
    last_updated_by_user_id: row.last_updated_by_user_id,
    is_deleted: row.is_deleted === 1,
    drive_id: row.drive_id,
    upload_status: row.upload_status,
    expires_at: row.expires_at,
    restore_trash_prior_folder_id: row.restore_trash_prior_folder_id as
      | FolderID
      | undefined,
    has_sovereign_permissions: row.has_sovereign_permissions === 1,
    shortcut_to_file_id: row.shortcut_to_file_id as FileID | undefined,
    notes: row.notes,
    external_id: row.external_id,
    external_payload: row.external_payload,
  };
}
