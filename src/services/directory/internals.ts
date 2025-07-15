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
 */
export async function translatePathToId(
  driveId: DriveID,
  path: DriveFullFilePath
): Promise<{ folder?: FolderRecord; file?: FileRecord }> {
  const isFolderPath = path.endsWith("/");

  if (isFolderPath) {
    const results = await db.queryDrive(
      driveId,
      "SELECT id, name, parent_folder_id, full_directory_path, created_by, created_at, last_updated_date_ms, last_updated_by_user_id, disk_id, disk_type, is_deleted, expires_at, drive_id, restore_trash_prior_folder_id, has_sovereign_permissions, shortcut_to_folder_id, notes, external_id, external_payload FROM folders WHERE full_directory_path = ?",
      [path]
    );

    if (results.length === 0) return {};

    const folderData = results[0];
    const folderId = folderData.id;

    // Hydrate subfolder_uuids
    const subfolderUuids = (
      await db.queryDrive(
        driveId,
        "SELECT id FROM folders WHERE parent_folder_id = ?",
        [folderId]
      )
    ).map((row: any) => row.id as FolderID);

    // Hydrate file_uuids
    const fileUuids = (
      await db.queryDrive(
        driveId,
        "SELECT id FROM files WHERE parent_folder_id = ?",
        [folderId]
      )
    ).map((row: any) => row.id as FileID);

    // Hydrate labels
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
      labels: [],
      created_by: folderData.created_by,
      created_at: folderData.created_at,
      last_updated_date_ms: folderData.last_updated_date_ms,
      last_updated_by: folderData.last_updated_by_user_id,
      disk_id: folderData.disk_id,
      disk_type: folderData.disk_type,
      deleted: !!folderData.is_deleted,
      expires_at: folderData.expires_at,
      drive_id: folderData.drive_id,
      restore_trash_prior_folder_uuid: folderData.restore_trash_prior_folder_id,
      has_sovereign_permissions: !!folderData.has_sovereign_permissions,
      shortcut_to: folderData.shortcut_to_folder_id,
      external_id: folderData.external_id,
      external_payload: folderData.external_payload,
      notes: folderData.notes,
    };
    return { folder: hydratedFolder };
  } else {
    const results = await db.queryDrive(
      driveId,
      `SELECT
        f.id, f.name, f.parent_folder_id, f.version_id, f.extension, f.full_directory_path, f.created_by, f.created_at, f.disk_id, f.disk_type, f.file_size, f.raw_url, f.last_updated_date_ms, f.last_updated_by_user_id, f.is_deleted, f.drive_id, f.upload_status, f.expires_at, f.restore_trash_prior_folder_id, f.has_sovereign_permissions, f.shortcut_to_file_id, f.notes, f.external_id, f.external_payload,
        fv.file_version, fv.prior_version_id
      FROM files f
      JOIN file_versions fv ON f.version_id = fv.version_id
      WHERE f.full_directory_path = ?`,
      [path]
    );

    if (results.length === 0) return {};

    const fileData = results[0];
    const fileId = fileData.id;

    // Hydrate labels from file_labels junction table
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
      labels: [],
      created_by: fileData.created_by,
      created_at: fileData.created_at,
      disk_id: fileData.disk_id,
      disk_type: fileData.disk_type,
      file_size: fileData.file_size,
      raw_url: fileData.raw_url,
      last_updated_date_ms: fileData.last_updated_date_ms,
      last_updated_by: fileData.last_updated_by_user_id,
      deleted: !!fileData.is_deleted,
      drive_id: fileData.drive_id,
      upload_status: fileData.upload_status,
      expires_at: fileData.expires_at,
      restore_trash_prior_folder_uuid: fileData.restore_trash_prior_folder_id,
      has_sovereign_permissions: !!fileData.has_sovereign_permissions,
      shortcut_to: fileData.shortcut_to_file_id,
      external_id: fileData.external_id,
      external_payload: fileData.external_payload,
      notes: fileData.notes,
    };
    return { file: hydratedFile };
  }
}

/**
 * Synchronous version of resolveNamingConflict for use within transactions.
 * It takes a `Database` object instead of relying on `db.queryDrive`.
 */
export function resolveNamingConflict(
  driveId: DriveID,
  basePath: string, // e.g., "disk_id::/parent/folder/"
  name: string,
  isFolder: boolean,
  resolution: FileConflictResolutionEnum = FileConflictResolutionEnum.KEEP_BOTH
): [string, string] {
  let finalName = name;
  let finalPath =
    `${basePath.replace(/\/$/, "")}/${finalName}` + (isFolder ? "/" : "");
  const tableName = isFolder ? "folders" : "files";

  const checkConflictSync = (path: string) => {
    // IMPORTANT: This is a synchronous function. `db.queryDrive` is async.
    // To make this truly synchronous without `tx` passed in, it would need
    // to open and close its own connection, which is inefficient in a loop.
    // For a real-world scenario with better-sqlite3 and transactions,
    // `tx` should be passed.
    // For now, simulating synchronous by using an immediately invoked async function.
    // This is a hack for the "don't pass tx" constraint for a synchronous context.
    let conflict = false;
    dbHelpers.withDrive(driveId, (tempDb: Database) => {
      const result = tempDb
        .prepare(`SELECT id FROM ${tableName} WHERE full_directory_path = ?`)
        .get(path);
      conflict = !!result;
    });
    return conflict;
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
  const pathConflict = checkConflictSync(finalPath);
  if (!pathConflict) {
    return [finalName, finalPath];
  }

  while (true) {
    counter++;
    const nameParts = name.split(".");
    const hasExtension = !isFolder && nameParts.length > 1;
    const baseName = hasExtension ? nameParts.slice(0, -1).join(".") : name;
    const extension = hasExtension ? nameParts[nameParts.length - 1] : "";

    finalName = `${baseName} (${counter})${hasExtension ? `.${extension}` : ""}`;
    finalPath =
      `${basePath.replace(/\/$/, "")}/${finalName}` + (isFolder ? "/" : "");

    if (!checkConflictSync(finalPath)) {
      break; // Found a unique name
    }
  }

  return [finalName, finalPath];
}

/**
 * Ensures the root and .trash folders exist for a given disk.
 */
export async function ensureRootFolder(
  driveId: DriveID,
  diskId: DiskID,
  userId: UserID
): Promise<FolderID> {
  // Use a temporary transaction for ensureRootFolder to ensure atomicity
  // without interfering with an outer transaction if called from one.
  // The outer function (e.g., createFolder) needs to be aware of this and
  // not double-wrap if it wants ensureRootFolder to be part of its transaction.
  // For now, let's keep it self-contained as per the original Rust logic.
  return dbHelpers.transaction("drive", driveId, (tx: Database) => {
    const disk = tx
      .prepare("SELECT * FROM disks WHERE id = ?")
      .get(diskId) as Disk;
    if (!disk) {
      throw new Error("Disk not found."); // Should ideally not happen if diskId is valid
    }

    const rootPath = `${diskId}::/`;
    const trashPath = `${diskId}::/.trash/`;

    let rootFolder: FolderRecord = tx
      .prepare("SELECT * FROM folders WHERE full_directory_path = ?")
      .get(rootPath) as FolderRecord;

    if (!rootFolder) {
      const rootFolderId = GenerateID.Folder();
      const now = Date.now();
      tx.prepare(
        `INSERT INTO folders (id, name, parent_folder_id, full_directory_path, created_by, created_at, last_updated_date_ms, last_updated_by_user_id, disk_id, disk_type, drive_id, expires_at)
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

      // PERMIT FIX: Add default permissions for the newly created root folder
      const permissionId = GenerateID.DirectoryPermission(); // Generate a new permission ID
      const nowMs = Date.now();
      const insertPermission = tx.prepare(`
        INSERT INTO permissions_directory (
          id, resource_type, resource_id, resource_path, grantee_type, grantee_id, granted_by_user_id,
          begin_date_ms, expiry_date_ms, inheritable, note, created_at, last_modified_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);
      insertPermission.run(
        permissionId,
        "Folder",
        rootFolder.id.substring(IDPrefixEnum.Folder.length), // Store plain ID
        rootPath,
        "User",
        userId.substring(IDPrefixEnum.User.length), // Store plain ID
        userId.substring(IDPrefixEnum.User.length), // Store plain ID
        0, // Immediate
        -1, // Never expires
        1, // Inheritable
        "Default permissions for root folder creator",
        nowMs,
        nowMs
      );

      const insertPermissionTypes = tx.prepare(`
        INSERT INTO permissions_directory_types (permission_id, permission_type) VALUES (?, ?)
      `);
      // Grant all directory permission types to the creator
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
        `INSERT INTO folders (id, name, parent_folder_id, full_directory_path, created_by, created_at, last_updated_date_ms, last_updated_by_user_id, disk_id, disk_type, drive_id, expires_at, has_sovereign_permissions)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      ).run(
        trashFolderId,
        ".trash",
        rootFolder.id,
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

      // PERMIT FIX: Add default permissions for the newly created trash folder
      const permissionId = GenerateID.DirectoryPermission(); // Generate a new permission ID
      const nowMs = Date.now();
      const insertPermission = tx.prepare(`
        INSERT INTO permissions_directory (
          id, resource_type, resource_id, resource_path, grantee_type, grantee_id, granted_by_user_id,
          begin_date_ms, expiry_date_ms, inheritable, note, created_at, last_modified_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);
      insertPermission.run(
        permissionId,
        "Folder",
        trashFolderId.substring(IDPrefixEnum.Folder.length), // Store plain ID
        trashPath,
        "User",
        userId.substring(IDPrefixEnum.User.length), // Store plain ID
        userId.substring(IDPrefixEnum.User.length), // Store plain ID
        0, // Immediate
        -1, // Never expires
        0, // Not inheritable (sovereign permissions - Rust had `has_sovereign_permissions` for this logic)
        "Default permissions for trash folder creator",
        nowMs,
        nowMs
      );

      const insertPermissionTypes = tx.prepare(`
        INSERT INTO permissions_directory_types (permission_id, permission_type) VALUES (?, ?)
      `);
      // Grant all directory permission types to the creator for the trash folder
      Object.values(DirectoryPermissionType).forEach((type) => {
        insertPermissionTypes.run(permissionId, type);
      });

      // Add trash folder to root's subfolders (if tracking this way)
      // This is implicit in the SQL by setting parent_folder_id, but if there's an in-memory representation, it would need updating.
      // Since `FolderRecord` includes `subfolder_uuids` but is hydrated from DB, we don't need to manually update it here.
    }
    return rootFolder.id;
  });
}

/**
 * Creates a nested folder structure if it doesn't already exist.
 */
export async function ensureFolderStructure(
  driveId: DriveID,
  fullPath: string, // e.g., disk_id::/path/to/folder/
  diskId: DiskID,
  userId: UserID,
  hasSovereignPermissions: boolean = false,
  externalId?: ExternalID,
  externalPayload?: ExternalPayload,
  shortcutTo?: FolderID,
  notes?: string
): Promise<FolderID> {
  const disk = (await dbHelpers.withDrive(driveId, (tx) => {
    return tx.prepare("SELECT * FROM disks WHERE id = ?").get(diskId);
  })) as Disk;
  if (!disk) {
    throw new Error("Disk not found for ensureFolderStructure.");
  }

  let parentFolderId = await ensureRootFolder(driveId, diskId, userId); // This already handles its own transaction

  const pathSegments =
    fullPath
      .split("::")[1]
      ?.split("/")
      .filter((p) => p.length > 0) ?? [];
  let currentPath = `${diskId}::/`;

  return dbHelpers.transaction("drive", driveId, (tx: Database) => {
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
        const newFolderId = GenerateID.Folder();
        const now = Date.now();
        const isFinalFolder = i === pathSegments.length - 1;

        tx.prepare(
          `INSERT INTO folders (id, name, parent_folder_id, full_directory_path, created_by, created_at, last_updated_date_ms, last_updated_by_user_id, disk_id, disk_type, drive_id, expires_at, has_sovereign_permissions, shortcut_to_folder_id, notes, external_id, external_payload)
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

        // PERMIT FIX: Add default permissions for the newly created folder
        const permissionId = GenerateID.DirectoryPermission();
        const nowMs = Date.now();
        tx.prepare(
          `INSERT INTO permissions_directory (
            id, resource_type, resource_id, resource_path, grantee_type, grantee_id, granted_by_user_id,
            begin_date_ms, expiry_date_ms, inheritable, note, created_at, last_modified_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
        ).run(
          permissionId,
          "Folder",
          newFolderId.substring(IDPrefixEnum.Folder.length),
          currentPath,
          "User",
          userId.substring(IDPrefixEnum.User.length),
          userId.substring(IDPrefixEnum.User.length),
          0, // Immediate
          -1, // Never expires
          isFinalFolder && hasSovereignPermissions ? 0 : 1, // If sovereign, not inheritable from parents
          "Default permissions for new folder creator",
          nowMs,
          nowMs
        );

        const insertPermissionTypes = tx.prepare(
          `INSERT INTO permissions_directory_types (permission_id, permission_type) VALUES (?, ?)`
        );
        Object.values(DirectoryPermissionType).forEach((type) => {
          insertPermissionTypes.run(permissionId, type);
        });
      }
    }
    return parentFolderId;
  });
}

/**
 * Recursively updates the full_directory_path for all children of a moved/renamed folder.
 */
export async function updateSubfolderPaths(
  driveId: DriveID,
  folderId: FolderID,
  oldPath: string,
  newPath: string
): Promise<void> {
  return dbHelpers.transaction("drive", driveId, (tx: Database) => {
    const queue: FolderID[] = [folderId];

    while (queue.length > 0) {
      const currentFolderId = queue.shift()!;

      // Fetch the current folder's path from the DB.
      // This is important because its path might have been updated by a parent in the queue.
      const currentFolder = tx
        .prepare("SELECT full_directory_path FROM folders WHERE id = ?")
        .get(currentFolderId) as { full_directory_path: string };

      if (!currentFolder) continue;

      const currentOldPath = currentFolder.full_directory_path;
      const updatedPath = currentOldPath.replace(oldPath, newPath);

      tx.prepare("UPDATE folders SET full_directory_path = ? WHERE id = ?").run(
        updatedPath,
        currentFolderId
      );

      // Update child files
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
        tx.prepare("UPDATE files SET full_directory_path = ? WHERE id = ?").run(
          newFilePath,
          file.id
        );
        // PERMIT FIX: Update resource_path for directory permissions associated with moved/renamed files
        const filePermissionsToUpdate = tx
          .prepare(
            `SELECT id, resource_path FROM permissions_directory
             WHERE resource_type = 'File' AND resource_id = ?`
          )
          .all(file.id.substring(IDPrefixEnum.File.length)) as {
          id: string;
          resource_path: string;
        }[];

        for (const perm of filePermissionsToUpdate) {
          if (perm.resource_path === file.full_directory_path) {
            // Compare with old path of the file
            tx.prepare(
              `UPDATE permissions_directory SET resource_path = ? WHERE id = ?`
            ).run(newFilePath, perm.id);
          }
        }
      }

      // Enqueue child folders and update their paths
      const childFolders = tx
        .prepare(
          "SELECT id, full_directory_path FROM folders WHERE parent_folder_id = ?"
        )
        .all(currentFolderId) as {
        id: FolderID;
        full_directory_path: string;
      }[];

      for (const subfolder of childFolders) {
        queue.push(subfolder.id);
        // The actual update for this subfolder's path will happen when it's dequeued
        // This is important for correct recursive replacement.
      }

      // PERMIT FIX: Update resource_path for directory permissions associated with the current folder
      const permissionsToUpdate = tx
        .prepare(
          `SELECT id, resource_path FROM permissions_directory
           WHERE resource_type = 'Folder' AND resource_id = ?`
        )
        .all(currentFolderId.substring(IDPrefixEnum.Folder.length)) as {
        id: string;
        resource_path: string;
      }[];

      for (const perm of permissionsToUpdate) {
        // If a permission's resource_path exactly matches the *old* path of this folder
        // (before its own path was updated), then update it to the *new* path of this folder.
        // This handles permissions directly on the folder being processed.
        if (perm.resource_path === currentOldPath) {
          tx.prepare(
            `UPDATE permissions_directory SET resource_path = ? WHERE id = ?`
          ).run(updatedPath, perm.id);
        }
        // No need for `else if (perm.resource_path.startsWith(currentOldPath))` because
        // child folder permissions are handled when the child folder itself is dequeued.
      }
    }
  });
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
  return `${baseUrl}/v1/drives/${driveId}/directory/asset/${fileId}.${extension}`;
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
