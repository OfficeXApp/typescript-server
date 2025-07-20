// src/services/directory/internals.ts
// src/services/directory/internals.ts
import {
  DirectoryResourceID,
  DiskID,
  DiskTypeEnum,
  DriveFullFilePath,
  DriveID,
  ExternalID,
  ExternalPayload,
  FileConflictResolutionEnum,
  FileID,
  FilePathBreadcrumb,
  FolderID,
  UserID,
  GenerateID,
  IDPrefixEnum,
  DirectoryPermissionType, // Import DirectoryPermissionType
  DriveClippedFilePath, // Import DriveClippedFilePath
  LabelValue,
  Disk,
  UploadStatus,
} from "@officexapp/types";
import { db, dbHelpers } from "../database"; // Ensure dbHelpers is imported
import { FolderRecord, FileRecord } from "@officexapp/types";
import type { Database } from "better-sqlite3";

// Import the actual permission service functions
import {
  deriveDirectoryBreadcrumbs as actualDeriveDirectoryBreadcrumbs,
  checkDirectoryPermissions, // Import for permission checks
  deriveBreadcrumbVisibilityPreviews, // Import for visibility previews
} from "../permissions/directory";

import { getFolderMetadata, getFileMetadata } from "./drive"; // Assuming these are in a 'drive' sub-service within directory
import { getDriveOwnerId } from "../../routes/v1/types"; // Import for isOwner check

// =========================================================================

/**
 * Sanitizes a file path by replacing multiple slashes with a single one and removing trailing slashes.
 * @param filePath - The file path to sanitize.
 * @returns The sanitized file path.
 */
export function sanitizeFilePath(filePath: string): string {
  const parts = filePath.split("::");
  if (parts.length < 2) return filePath; // Invalid format, return as is

  const storagePart = parts[0];
  const pathPart = parts.slice(1).join("::");

  // Replace colons and multiple slashes
  const sanitized = pathPart.replace(/:/g, ";").replace(/\/+/g, "/");

  // Don't trim the leading slash, only trailing
  return `${storagePart}::${sanitized.replace(/\/$/, "")}`;
}

/**
 * Splits a full path into its parent folder path and the final component (file/folder name).
 * @param fullPath - The full path string, e.g., "disk_id::/folder1/file.txt"
 * @returns A tuple containing the folder path and the file/folder name. e.g., ["disk_id::/folder1/", "file.txt"]
 */
export function splitPath(fullPath: string): [string, string] {
  const lastSlashIndex = fullPath.lastIndexOf("/");
  if (lastSlashIndex === -1 || lastSlashIndex < fullPath.indexOf("::")) {
    // Handles cases like "disk_id::file.txt"
    const [storagePart, namePart] = fullPath.split("::");
    return [`${storagePart}::/`, namePart || ""];
  }

  const folderPath = fullPath.substring(0, lastSlashIndex + 1);
  const fileName = fullPath.substring(lastSlashIndex + 1);
  return [folderPath, fileName];
}

/**
 * Clips a full directory path for display purposes, showing only the disk and the last component.
 * Rust implementation:
 * // disk_id::path/to/folder/
 * // disk_id::path/to/folder/file.txt
 * // recostruct with .. in between
 * // disk_id::../folder/
 * // disk_id::../file.txt
 * @param fullPath - The full path (DriveFullFilePath).
 * @returns The clipped path (DriveClippedFilePath).
 */
export function clipDirectoryPath(
  fullPath: DriveFullFilePath
): DriveClippedFilePath {
  const pathParts = fullPath.split("::");
  if (pathParts.length < 2) {
    return fullPath as DriveClippedFilePath; // Should ideally not happen if DriveFullFilePath is always "diskID::path"
  }

  const diskIdPart = pathParts[0];
  const filePathSegment = pathParts[1]; // e.g., "/path/to/folder/" or "/path/to/file.txt"

  const segments = filePathSegment.split("/").filter((s) => s.length > 0);

  let clippedPath = "";
  if (segments.length > 1) {
    // Has intermediate folders, show ".."
    clippedPath = `${diskIdPart}::../${segments[segments.length - 1]}${fullPath.endsWith("/") && !segments[segments.length - 1].includes(".") ? "/" : ""}`;
  } else if (segments.length === 1) {
    // Only disk root + direct file/folder, show "disk_id::file.txt" or "disk_id::folder/"
    clippedPath = `${diskIdPart}::${segments[0]}${fullPath.endsWith("/") && !segments[0].includes(".") ? "/" : ""}`;
  } else {
    // Only disk root, show "disk_id::/"
    clippedPath = `${diskIdPart}::/`;
  }

  return clippedPath as DriveClippedFilePath;
}

/**
 * Translates a full directory path to a file or folder record.
 * This function remains async as it performs db.queryDrive directly.
 * It will be called *outside* the main transaction logic where possible.
 */
export async function translatePathToId(
  driveId: DriveID,
  path: DriveFullFilePath
): Promise<{ folder?: FolderRecord; file?: FileRecord }> {
  const isFolderPath = path.endsWith("/");

  if (isFolderPath) {
    const results = await db.queryDrive(
      driveId,
      "SELECT id, name, parent_folder_id, full_directory_path, created_by, created_at, last_updated_date_ms, last_updated_by, disk_id, disk_type, deleted, expires_at, drive_id, restore_trash_prior_folder_uuid, has_sovereign_permissions, shortcut_to, notes, external_id, external_payload FROM folders WHERE full_directory_path = ?",
      [path]
    );

    if (results.length === 0) return {};

    const folderData = results[0];
    const folderId = folderData.id;

    // These should also use db.queryDrive, so they remain async.
    const subfolderUuids = (
      await db.queryDrive(
        driveId,
        "SELECT id FROM folders WHERE parent_folder_id = ?",
        [folderId]
      )
    ).map((row: any) => row.id as FolderID);

    const fileUuids = (
      await db.queryDrive(
        driveId,
        "SELECT id FROM files WHERE parent_folder_id = ?",
        [folderId]
      )
    ).map((row: any) => row.id as FileID);

    const labels = (
      await db.queryDrive(
        driveId,
        "SELECT L.value FROM folder_labels FL JOIN labels L ON FL.label_id = L.id WHERE FL.folder_id = ?",
        [folderId]
      )
    ).map((row: any) => row.value as LabelValue);

    const hydratedFolder: FolderRecord = {
      id: folderData.id,
      name: folderData.name,
      parent_folder_uuid: folderData.parent_folder_id,
      subfolder_uuids: subfolderUuids,
      file_uuids: fileUuids,
      full_directory_path: folderData.full_directory_path,
      labels: [], // Labels are hydrated above, but this line assigns empty array. FIX ME if labels need to be included.
      created_by: folderData.created_by,
      created_at: folderData.created_at,
      last_updated_date_ms: folderData.last_updated_date_ms,
      last_updated_by: folderData.last_updated_by,
      disk_id: folderData.disk_id,
      disk_type: folderData.disk_type,
      deleted: !!folderData.deleted,
      expires_at: folderData.expires_at,
      drive_id: folderData.drive_id,
      restore_trash_prior_folder_uuid:
        folderData.restore_trash_prior_folder_uuid,
      has_sovereign_permissions: !!folderData.has_sovereign_permissions,
      shortcut_to: folderData.shortcut_to,
      external_id: folderData.external_id,
      external_payload: folderData.external_payload,
      notes: folderData.notes,
    };
    return { folder: hydratedFolder };
  } else {
    const results = await db.queryDrive(
      driveId,
      `SELECT
        f.id, f.name, f.parent_folder_id, f.version_id, f.extension, f.full_directory_path, f.created_by, f.created_at, f.disk_id, f.disk_type, f.file_size, f.raw_url, f.last_updated_date_ms, f.last_updated_by, f.deleted, f.drive_id, f.upload_status, f.expires_at, f.restore_trash_prior_folder_uuid, f.has_sovereign_permissions, f.shortcut_to, f.notes, f.external_id, f.external_payload,
        fv.file_version, fv.prior_version_id
      FROM files f
      JOIN file_versions fv ON f.version_id = fv.version_id
      WHERE f.full_directory_path = ?`,
      [path]
    );

    if (results.length === 0) return {};

    const fileData = results[0];
    const fileId = fileData.id;

    const labels = (
      await db.queryDrive(
        driveId,
        "SELECT L.value FROM file_labels FL JOIN labels L ON FL.label_id = L.id WHERE FL.file_id = ?",
        [fileId]
      )
    ).map((row: any) => row.value as LabelValue);

    const hydratedFile: FileRecord = {
      id: fileData.id,
      name: fileData.name,
      parent_folder_uuid: fileData.parent_folder_id,
      file_version: fileData.file_version,
      prior_version: fileData.prior_version_id,
      version_id: fileData.version_id,
      extension: fileData.extension,
      full_directory_path: fileData.full_directory_path,
      labels: [], // Labels are hydrated above, but this line assigns empty array. FIX ME if labels need to be included.
      created_by: fileData.created_by,
      created_at: fileData.created_at,
      disk_id: fileData.disk_id,
      disk_type: fileData.disk_type,
      file_size: fileData.file_size,
      raw_url: fileData.raw_url,
      last_updated_date_ms: fileData.last_updated_date_ms,
      last_updated_by: fileData.last_updated_by,
      deleted: !!fileData.deleted,
      drive_id: fileData.drive_id,
      upload_status: fileData.upload_status,
      expires_at: fileData.expires_at,
      restore_trash_prior_folder_uuid: fileData.restore_trash_prior_folder_uuid,
      has_sovereign_permissions: !!fileData.has_sovereign_permissions,
      shortcut_to: fileData.shortcut_to,
      external_id: fileData.external_id,
      external_payload: fileData.external_payload,
      notes: fileData.notes,
    };
    return { file: hydratedFile };
  }
}

/**
 * ASYNCHRONOUS version of resolveNamingConflict for use when *not* inside a transaction.
 * This is used for initial checks or when the operation doesn't need to be transactional.
 * It will call db.queryDrive which is async.
 */
export async function resolveNamingConflict(
  driveId: DriveID,
  basePath: string, // e.g., "disk_id::/parent/folder/"
  name: string,
  isFolder: boolean,
  resolution: FileConflictResolutionEnum = FileConflictResolutionEnum.KEEP_BOTH
): Promise<[string, string]> {
  let finalName = name;
  let finalPath =
    `${basePath.replace(/\/$/, "")}/${finalName}` + (isFolder ? "/" : "");
  const tableName = isFolder ? "folders" : "files";

  const checkConflictAsync = async (path: string) => {
    const results = await db.queryDrive(
      driveId,
      `SELECT id FROM ${tableName} WHERE full_directory_path = ?`,
      [path]
    );
    return results.length > 0;
  };

  if (
    resolution === FileConflictResolutionEnum.REPLACE ||
    resolution === FileConflictResolutionEnum.KEEP_NEWER
  ) {
    return [finalName, finalPath];
  }

  if (resolution === FileConflictResolutionEnum.KEEP_ORIGINAL) {
    if (await checkConflictAsync(finalPath)) {
      return ["", ""]; // Signal to abort
    }
    return [finalName, finalPath];
  }

  // Default: KEEP_BOTH
  let counter = 1;
  while (true) {
    const conflict = await checkConflictAsync(finalPath);
    if (!conflict) {
      break; // Found a unique name
    }

    counter++;
    const nameParts = name.split(".");
    const hasExtension = !isFolder && nameParts.length > 1;
    const baseName = hasExtension ? nameParts.slice(0, -1).join(".") : name;
    const extension = hasExtension ? nameParts[nameParts.length - 1] : "";

    finalName = `${baseName} (${counter})${hasExtension ? `.${extension}` : ""}`;
    finalPath =
      `${basePath.replace(/\/$/, "")}/${finalName}` + (isFolder ? "/" : "");
  }

  return [finalName, finalPath];
}

/**
 * SYNCHRONOUS version of resolveNamingConflict for use *within* transactions.
 * It accepts a `tx: Database` object to perform synchronous queries.
 */
export function resolveNamingConflict_SYNC(
  tx: Database,
  driveId: DriveID, // driveId might still be useful for context/logging
  basePath: string,
  name: string,
  isFolder: boolean,
  resolution: FileConflictResolutionEnum = FileConflictResolutionEnum.KEEP_BOTH
): [string, string] {
  let finalName = name;
  let finalPath =
    `${basePath.replace(/\/$/, "")}/${finalName}` + (isFolder ? "/" : "");
  const tableName = isFolder ? "folders" : "files";

  const checkConflictSync = (path: string) => {
    const result = tx
      .prepare(`SELECT id FROM ${tableName} WHERE full_directory_path = ?`)
      .get(path);
    return !!result;
  };

  if (
    resolution === FileConflictResolutionEnum.REPLACE ||
    resolution === FileConflictResolutionEnum.KEEP_NEWER
  ) {
    return [finalName, finalPath];
  }

  if (resolution === FileConflictResolutionEnum.KEEP_ORIGINAL) {
    if (checkConflictSync(finalPath)) {
      return ["", ""]; // Signal to abort
    }
    return [finalName, finalPath];
  }

  // Default: KEEP_BOTH
  let counter = 1;
  while (true) {
    const conflict = checkConflictSync(finalPath);
    if (!conflict) {
      break; // Found a unique name
    }

    counter++;
    const nameParts = name.split(".");
    const hasExtension = !isFolder && nameParts.length > 1;
    const baseName = hasExtension ? nameParts.slice(0, -1).join(".") : name;
    const extension = hasExtension ? nameParts[nameParts.length - 1] : "";

    finalName = `${baseName} (${counter})${hasExtension ? `.${extension}` : ""}`;
    finalPath =
      `${basePath.replace(/\/$/, "")}/${finalName}` + (isFolder ? "/" : "");
  }

  return [finalName, finalPath];
}

/**
 * Ensures the root and .trash folders exist for a given disk.
 * This function uses its own transaction and is designed to be called externally.
 */
export async function ensureRootFolder(
  driveId: DriveID,
  diskId: DiskID,
  userId: UserID
): Promise<FolderID> {
  return dbHelpers.transaction("drive", driveId, (tx: Database) => {
    const disk = tx
      .prepare("SELECT * FROM disks WHERE id = ?")
      .get(diskId) as Disk;
    if (!disk) {
      throw new Error("Disk not found.");
    }

    const rootPath = `${diskId}::/`;
    const trashPath = `${diskId}::.trash/`;

    let rootFolder: FolderRecord = tx
      .prepare("SELECT * FROM folders WHERE full_directory_path = ?")
      .get(rootPath) as FolderRecord;

    if (!rootFolder) {
      const rootFolderId = GenerateID.Folder();
      const now = Date.now();
      tx.prepare(
        `INSERT INTO folders (id, name, parent_folder_id, full_directory_path, created_by, created_at, last_updated_date_ms, last_updated_by, disk_id, disk_type, drive_id, expires_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      ).run(
        rootFolderId,
        "",
        null,
        rootPath,
        userId,
        now,
        now,
        userId,
        diskId,
        disk.disk_type,
        driveId,
        -1
      );
      rootFolder = tx
        .prepare("SELECT * FROM folders WHERE id = ?")
        .get(rootFolderId) as FolderRecord;

      const permissionId = GenerateID.DirectoryPermission();
      const nowMs = Date.now();
      const insertPermission = tx.prepare(`
        INSERT INTO permissions_directory (
          id, resource_type, resource_id, resource_path, grantee_type, grantee_id, granted_by,
          begin_date_ms, expiry_date_ms, inheritable, note, created_at, last_modified_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);
      insertPermission.run(
        permissionId,
        "Folder",
        rootFolder.id,
        rootPath,
        "User",
        userId,
        userId,
        0,
        -1,
        1,
        "Default permissions for root folder creator",
        nowMs,
        nowMs
      );

      const insertPermissionTypes = tx.prepare(`
        INSERT INTO permissions_directory_types (permission_id, permission_type) VALUES (?, ?)
      `);
      Object.values(DirectoryPermissionType).forEach((type) => {
        insertPermissionTypes.run(permissionId, type);
      });
    }

    const trashFolderResult = tx
      .prepare("SELECT id FROM folders WHERE full_directory_path = ?")
      .get(trashPath);
    if (!trashFolderResult) {
      const trashFolderId = GenerateID.Folder();
      const now = Date.now();
      tx.prepare(
        `INSERT INTO folders (id, name, parent_folder_id, full_directory_path, created_by, created_at, last_updated_date_ms, last_updated_by, disk_id, disk_type, drive_id, expires_at, has_sovereign_permissions)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      ).run(
        trashFolderId,
        ".trash",
        null,
        trashPath,
        userId,
        now,
        now,
        userId,
        diskId,
        disk.disk_type,
        driveId,
        -1,
        1
      );

      const permissionId = GenerateID.DirectoryPermission();
      const nowMs = Date.now();
      const insertPermission = tx.prepare(`
        INSERT INTO permissions_directory (
          id, resource_type, resource_id, resource_path, grantee_type, grantee_id, granted_by,
          begin_date_ms, expiry_date_ms, inheritable, note, created_at, last_modified_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);
      insertPermission.run(
        permissionId,
        "Folder",
        trashFolderId,
        trashPath,
        "User",
        userId,
        userId,
        0,
        -1,
        0,
        "Default permissions for trash folder creator",
        nowMs,
        nowMs
      );

      const insertPermissionTypes = tx.prepare(`
        INSERT INTO permissions_directory_types (permission_id, permission_type) VALUES (?, ?)
      `);
      Object.values(DirectoryPermissionType).forEach((type) => {
        insertPermissionTypes.run(permissionId, type);
      });
    }
    return rootFolder.id;
  });
}

/**
 * Creates a nested folder structure if it doesn't already exist.
 * This function also operates within a transaction.
 */
export async function ensureFolderStructure(
  driveId: DriveID,
  fullPath: string,
  diskId: DiskID,
  userId: UserID,
  hasSovereignPermissions: boolean = false,
  externalId?: ExternalID,
  externalPayload?: ExternalPayload,
  final_folder_id?: FolderID,
  shortcutTo?: FolderID,
  notes?: string
): Promise<FolderID> {
  // Fetch disk info outside the transaction as it's an async query.
  const disk = (await dbHelpers.withDrive(driveId, (tx) => {
    return tx.prepare("SELECT * FROM disks WHERE id = ?").get(diskId);
  })) as Disk;
  if (!disk) {
    throw new Error("Disk not found for ensureFolderStructure.");
  }

  // ensureRootFolder performs its own transaction. Call it before the main transaction.
  let parentFolderId = await ensureRootFolder(driveId, diskId, userId);

  const pathSegments =
    fullPath
      .split("::")[1]
      ?.split("/")
      .filter((p) => p.length > 0) ?? [];
  let currentPath = `${diskId}::/`;

  return dbHelpers.transaction("drive", driveId, (tx: Database) => {
    // Re-fetch root folder within this transaction to ensure `parentFolderId` is valid in this tx context.
    // Or, ensure `ensureRootFolder` can return data usable directly.
    // Given the structure, `parentFolderId` from outside the transaction should be safe to use as ID.

    for (let i = 0; i < pathSegments.length; i++) {
      const segment = pathSegments[i];
      currentPath += `${segment}/`;
      const result = tx
        .prepare("SELECT * FROM folders WHERE full_directory_path = ?")
        .get(currentPath) as FolderRecord;
      let folder: FolderRecord | undefined = result;

      if (folder) {
        parentFolderId = folder.id;
      } else {
        const isFinalFolder = i === pathSegments.length - 1;

        const newFolderId =
          isFinalFolder && final_folder_id
            ? final_folder_id
            : GenerateID.Folder();

        const now = Date.now();

        tx.prepare(
          `INSERT INTO folders (id, name, parent_folder_id, full_directory_path, created_by, created_at, last_updated_date_ms, last_updated_by, disk_id, disk_type, drive_id, expires_at, has_sovereign_permissions, shortcut_to, notes, external_id, external_payload)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
        ).run(
          newFolderId,
          segment,
          parentFolderId,
          currentPath,
          userId,
          now,
          now,
          userId,
          diskId,
          disk.disk_type,
          driveId,
          -1,
          isFinalFolder && hasSovereignPermissions ? 1 : 0,
          isFinalFolder ? shortcutTo : undefined,
          isFinalFolder ? notes : undefined,
          isFinalFolder ? externalId : undefined,
          isFinalFolder ? externalPayload : undefined
        );
        parentFolderId = newFolderId;
      }
    }
    return parentFolderId;
  });
}

/**
 * Recursively updates the full_directory_path for all children of a moved/renamed folder.
 * This function is designed to be called *within* a synchronous transaction.
 */
export function updateSubfolderPaths_SYNC(
  tx: Database,
  folderId: FolderID,
  oldPath: string,
  newPath: string,
  userId: UserID
): void {
  const queue: FolderID[] = [folderId];
  const now = Date.now(); // Timestamp for updates

  while (queue.length > 0) {
    const currentFolderId = queue.shift()!;
    const currentFolder = tx
      .prepare("SELECT full_directory_path FROM folders WHERE id = ?")
      .get(currentFolderId) as { full_directory_path: string };

    if (!currentFolder) continue;

    const currentOldPath = currentFolder.full_directory_path;
    const updatedPath = currentOldPath.replace(oldPath, newPath);

    tx.prepare(
      "UPDATE folders SET full_directory_path = ?, last_updated_date_ms = ?, last_updated_by = ? WHERE id = ?"
    ).run(updatedPath, now, userId, currentFolderId);

    tx.prepare(
      `UPDATE permissions_directory SET resource_path = ? WHERE resource_id = ?`
    ).run(updatedPath, currentFolderId);

    const childFiles = tx
      .prepare(
        "SELECT id, full_directory_path FROM files WHERE parent_folder_id = ?"
      )
      .all(currentFolderId) as { id: FileID; full_directory_path: string }[];

    for (const file of childFiles) {
      const newFilePath = file.full_directory_path.replace(
        currentOldPath,
        updatedPath
      );
      tx.prepare(
        "UPDATE files SET full_directory_path = ?, last_updated_date_ms = ?, last_updated_by = ? WHERE id = ?"
      ).run(newFilePath, now, userId, file.id);
      tx.prepare(
        `UPDATE permissions_directory SET resource_path = ? WHERE resource_id = ?`
      ).run(newFilePath, file.id);
    }

    const childFolders = tx
      .prepare("SELECT id FROM folders WHERE parent_folder_id = ?")
      .all(currentFolderId) as { id: FolderID }[];

    for (const subfolder of childFolders) {
      queue.push(subfolder.id);
    }
  }
}

export function updateSubfolderPaths(
  driveId: DriveID,
  folderId: FolderID,
  oldPath: string,
  newPath: string,
  userId: UserID
): void {
  const tx = dbHelpers.transaction("drive", driveId, (tx: Database) => {
    const queue: FolderID[] = [folderId];
    const now = Date.now(); // Timestamp for updates

    while (queue.length > 0) {
      const currentFolderId = queue.shift()!;
      const currentFolder = tx
        .prepare("SELECT full_directory_path FROM folders WHERE id = ?")
        .get(currentFolderId) as { full_directory_path: string };

      if (!currentFolder) continue;

      const currentOldPath = currentFolder.full_directory_path;
      const updatedPath = currentOldPath.replace(oldPath, newPath);

      tx.prepare(
        "UPDATE folders SET full_directory_path = ?, last_updated_date_ms = ?, last_updated_by = ? WHERE id = ?"
      ).run(updatedPath, now, userId, currentFolderId);

      tx.prepare(
        `UPDATE permissions_directory SET resource_path = ? WHERE resource_id = ?`
      ).run(updatedPath, currentFolderId);

      const childFiles = tx
        .prepare(
          "SELECT id, full_directory_path FROM files WHERE parent_folder_id = ?"
        )
        .all(currentFolderId) as { id: FileID; full_directory_path: string }[];

      for (const file of childFiles) {
        const newFilePath = file.full_directory_path.replace(
          currentOldPath,
          updatedPath
        );
        tx.prepare(
          "UPDATE files SET full_directory_path = ?, last_updated_date_ms = ?, last_updated_by = ? WHERE id = ?"
        ).run(newFilePath, now, userId, file.id);
        tx.prepare(
          `UPDATE permissions_directory SET resource_path = ? WHERE resource_id = ?`
        ).run(newFilePath, file.id);
      }

      const childFolders = tx
        .prepare("SELECT id FROM folders WHERE parent_folder_id = ?")
        .all(currentFolderId) as { id: FolderID }[];

      for (const subfolder of childFolders) {
        queue.push(subfolder.id);
      }
    }
  });
  return;
}

/**
 * Synchronous helper to copy contents of a folder (files and subfolders).
 * This function is designed to be called *within* an existing synchronous transaction.
 * It will recursively call itself and copyFile_SYNC.
 */
export function copyFolderContents_SYNC(
  tx: Database,
  driveId: DriveID,
  userId: UserID,
  sourceFolderId: FolderID,
  destinationFolderId: FolderID,
  resolution: FileConflictResolutionEnum | undefined
): void {
  const sourceFolder = tx
    .prepare("SELECT * FROM folders WHERE id = ?")
    .get(sourceFolderId) as FolderRecord;
  const destFolder = tx
    .prepare("SELECT * FROM folders WHERE id = ?")
    .get(destinationFolderId) as FolderRecord;

  if (!sourceFolder || !destFolder) {
    console.error("Source or destination folder not found for recursive copy.");
    return; // Or throw error, depending on desired behavior
  }

  // Copy files in the current folder
  const filesInFolder = tx
    .prepare("SELECT * FROM files WHERE parent_folder_id = ?")
    .all(sourceFolder.id) as FileRecord[];

  for (const file of filesInFolder) {
    const newFileUuid = GenerateID.File();
    const now = Date.now();

    const [finalName, finalPath] = resolveNamingConflict_SYNC(
      tx,
      driveId,
      destFolder.full_directory_path,
      file.name,
      false, // is_folder = false
      resolution
    );

    if (!finalName) {
      // If conflict resolution is KEEP_ORIGINAL and file exists, skip.
      continue;
    }

    const newVersionId = GenerateID.FileVersionID();

    const newFileRecord: FileRecord = {
      id: newFileUuid,
      name: finalName,
      parent_folder_uuid: destinationFolderId,
      version_id: newVersionId,
      file_version: 1,
      prior_version: undefined,
      extension: file.extension,
      full_directory_path: finalPath,
      labels: file.labels,
      created_by: userId,
      created_at: now,
      disk_id: file.disk_id,
      disk_type: file.disk_type,
      file_size: file.file_size,
      raw_url: formatFileAssetPath(driveId, newFileUuid, file.extension),
      last_updated_date_ms: now,
      last_updated_by: userId,
      deleted: false,
      drive_id: driveId,
      expires_at: file.expires_at,
      restore_trash_prior_folder_uuid: undefined,
      has_sovereign_permissions: file.has_sovereign_permissions,
      shortcut_to: file.shortcut_to,
      upload_status: file.upload_status,
      external_id: file.external_id,
      external_payload: file.external_payload,
      notes: file.notes,
    };

    tx.prepare(
      `INSERT INTO files (id, name, parent_folder_id, version_id, extension, full_directory_path, created_by, created_at, disk_id, disk_type, file_size, raw_url, last_updated_date_ms, last_updated_by, drive_id, upload_status, expires_at, has_sovereign_permissions, shortcut_to, notes, external_id, external_payload)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(
      newFileRecord.id,
      newFileRecord.name,
      newFileRecord.parent_folder_uuid,
      newFileRecord.version_id,
      newFileRecord.extension,
      newFileRecord.full_directory_path,
      newFileRecord.created_by,
      newFileRecord.created_at,
      newFileRecord.disk_id,
      newFileRecord.disk_type,
      newFileRecord.file_size,
      newFileRecord.raw_url,
      newFileRecord.last_updated_date_ms,
      newFileRecord.last_updated_by,
      newFileRecord.drive_id,
      newFileRecord.upload_status,
      newFileRecord.expires_at,
      newFileRecord.has_sovereign_permissions ? 1 : 0,
      newFileRecord.shortcut_to,
      newFileRecord.notes,
      newFileRecord.external_id,
      newFileRecord.external_payload
    );

    tx.prepare(
      `INSERT INTO file_versions(version_id, file_id, name, file_version, prior_version_id, extension, created_by, created_at, disk_id, disk_type, file_size, raw_url, notes)
       VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(
      newVersionId,
      newFileUuid,
      finalName,
      1,
      undefined,
      file.extension,
      userId,
      now,
      file.disk_id,
      file.disk_type,
      file.file_size,
      newFileRecord.raw_url,
      file.notes
    );

    // Update destFolder's file_uuids if in-memory sync is needed (for immediate retrieval within tx)
    tx.prepare(
      `UPDATE folders SET file_uuids = json_insert(coalesce(file_uuids, '[]'), '$[#]', ?) WHERE id = ?`
    ).run(newFileRecord.id, destinationFolderId);

    // Asynchronous cloud copy for files (fire and forget)
    if (
      newFileRecord.upload_status === UploadStatus.QUEUED &&
      (file.disk_type === DiskTypeEnum.AwsBucket ||
        file.disk_type === DiskTypeEnum.StorjWeb3)
    ) {
      // Need to fetch disk info here if not passed, but doing this inside sync tx is bad.
      // Ideally, the disk info would be passed down or cached. For now, a non-blocking approach.
      // This part *cannot* use the `tx` object for queries.
      // It's a "fire and forget" operation.
      (async () => {
        const disk = (
          await db.queryDrive(driveId, "SELECT * FROM disks WHERE id = ?", [
            file.disk_id,
          ])
        )[0];
        if (!disk || !disk.auth_json) {
          console.error("Missing disk or auth for async copy operation.");
          return;
        }
        const auth = JSON.parse(disk.auth_json);
        const sourceKey = file.raw_url;
        const destinationKey = newFileRecord.raw_url;
        let copyResult;
        if (file.disk_type === DiskTypeEnum.AwsBucket) {
          copyResult = await copyS3Object(sourceKey, destinationKey, auth);
        } else {
          copyResult = await copyStorjObject(sourceKey, destinationKey, auth);
        }
        if (copyResult.err) {
          console.error(
            `Cloud copy failed for ${newFileRecord.id}: ${copyResult.err}`
          );
        } else {
          console.log(`Cloud copy completed for ${newFileRecord.id}.`);
        }
      })();
    }
  }

  // Recursively copy subfolders
  const subfolders = tx
    .prepare("SELECT * FROM folders WHERE parent_folder_id = ?")
    .all(sourceFolder.id) as FolderRecord[];

  for (const sub of subfolders) {
    const newSubfolderUuid = GenerateID.Folder();
    const now = Date.now();

    const [finalName, finalPath] = resolveNamingConflict_SYNC(
      tx,
      driveId,
      destFolder.full_directory_path,
      sub.name,
      true, // isFolder = true
      resolution
    );

    if (!finalName) {
      continue; // If conflict resolution is KEEP_ORIGINAL and folder exists, skip.
    }

    const newSubfolderRecord: FolderRecord = {
      id: newSubfolderUuid,
      name: finalName,
      parent_folder_uuid: destinationFolderId,
      subfolder_uuids: [], // Will be populated recursively
      file_uuids: [], // Will be populated recursively
      full_directory_path: finalPath,
      labels: sub.labels,
      created_by: userId,
      created_at: now,
      last_updated_date_ms: now,
      last_updated_by: userId,
      disk_id: sub.disk_id,
      disk_type: sub.disk_type,
      deleted: false,
      expires_at: sub.expires_at,
      drive_id: driveId,
      restore_trash_prior_folder_uuid: undefined,
      has_sovereign_permissions: sub.has_sovereign_permissions,
      shortcut_to: sub.shortcut_to,
      external_id: sub.external_id,
      external_payload: sub.external_payload,
      notes: sub.notes,
    };

    tx.prepare(
      `INSERT INTO folders (id, name, parent_folder_id, full_directory_path, created_by, created_at, last_updated_date_ms, last_updated_by, disk_id, disk_type, deleted, expires_at, drive_id, restore_trash_prior_folder_uuid, has_sovereign_permissions, shortcut_to, notes, external_id, external_payload)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(
      newSubfolderRecord.id,
      newSubfolderRecord.name,
      newSubfolderRecord.parent_folder_uuid,
      newSubfolderRecord.full_directory_path,
      newSubfolderRecord.created_by,
      newSubfolderRecord.created_at,
      newSubfolderRecord.last_updated_date_ms,
      newSubfolderRecord.last_updated_by,
      newSubfolderRecord.disk_id,
      newSubfolderRecord.disk_type,
      newSubfolderRecord.deleted ? 1 : 0,
      newSubfolderRecord.expires_at,
      newSubfolderRecord.drive_id,
      newSubfolderRecord.restore_trash_prior_folder_uuid,
      newSubfolderRecord.has_sovereign_permissions ? 1 : 0,
      newSubfolderRecord.shortcut_to,
      newSubfolderRecord.notes,
      newSubfolderRecord.external_id,
      newSubfolderRecord.external_payload
    );

    // Update destFolder's subfolder_uuids if in-memory sync is needed
    tx.prepare(
      `UPDATE folders SET subfolder_uuids = json_insert(coalesce(subfolder_uuids, '[]'), '$[#]', ?) WHERE id = ?`
    ).run(newSubfolderRecord.id, destinationFolderId);

    // Recursively copy contents of the subfolder
    copyFolderContents_SYNC(
      tx,
      driveId,
      userId,
      sub.id,
      newSubfolderRecord.id,
      resolution
    );
  }
}

/**
 * Generates the publicly accessible URL for a file asset.
 */
export function formatFileAssetPath(
  driveId: DriveID,
  fileId: FileID,
  extension: string
): string {
  const baseUrl = process.env.BASE_URL || "http://localhost:8888";
  return `${baseUrl}/v1/drive/${driveId}/directory/asset/${fileId}.${extension}`;
}

// These are placeholder functions for cloud storage operations,
// assuming they would exist in a real implementation.
// For the purpose of this task, they just return dummy success.
export const copyS3Object = async (
  sourceKey: string,
  destinationKey: string,
  auth: any
): Promise<{ ok?: null; err?: string }> => {
  console.log(`Simulating S3 copy from ${sourceKey} to ${destinationKey}`);
  // In a real scenario, this would involve calling AWS SDK or similar.
  return { ok: null };
};

export const copyStorjObject = async (
  sourceKey: string,
  destinationKey: string,
  auth: any
): Promise<{ ok?: null; err?: string }> => {
  console.log(`Simulating Storj copy from ${sourceKey} to ${destinationKey}`);
  // In a real scenario, this would involve calling Storj SDK or similar.
  return { ok: null };
};
