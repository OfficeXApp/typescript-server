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
} from "@officexapp/types";
import { db, dbHelpers } from "../database";
import { FolderRecord, FileRecord } from "@officexapp/types";
import type { Database } from "better-sqlite3";

// =========================================================================
// TODO: DRIVE Service Placeholders
// These are mock implementations. Replace with actual service logic.
// =========================================================================

const permissionsService = {
  // TODO: DRIVE Replace with actual implementation
  deriveDirectoryBreadcrumbs: async (
    _driveId: DriveID,
    _userId: UserID,
    _resource: { file?: FileID; folder?: FolderID }
  ): Promise<FilePathBreadcrumb[]> => {
    return [];
  },
  // TODO: DRIVE Replace with actual implementation
  castFolderFE: async (
    _driveId: DriveID,
    _userId: UserID,
    folder: FolderRecord
  ): Promise<any> => {
    return {
      ...folder,
      clipped_directory_path: "mock/path",
      permission_previews: [],
    };
  },
  // TODO: DRIVE Replace with actual implementation
  castFileFE: async (
    _driveId: DriveID,
    _userId: UserID,
    file: FileRecord
  ): Promise<any> => {
    return {
      ...file,
      clipped_directory_path: "mock/path",
      permission_previews: [],
    };
  },
};

const diskService = {
  // TODO: DRIVE Replace with actual implementation
  getDisk: async (
    _driveId: DriveID,
    diskId: DiskID
  ): Promise<{ disk_type: DiskTypeEnum; root_folder_id: FolderID }> => {
    console.warn("TODO: DRIVE diskService.getDisk is a mock");
    return {
      disk_type: DiskTypeEnum.IcpCanister,
      root_folder_id: `FolderID_root_${diskId}`,
    };
  },
};

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
 * Translates a full directory path to a file or folder record.
 */
export async function translatePathToId(
  driveId: DriveID,
  path: DriveFullFilePath
): Promise<{ folder?: FolderRecord; file?: FileRecord }> {
  const isFolderPath = path.endsWith("/");

  if (isFolderPath) {
    const result = await db.queryDrive(
      driveId,
      "SELECT * FROM folders WHERE full_directory_path = ?",
      [path]
    );
    // TODO: DRIVE Hydrate labels, subfolder_uuids, file_uuids from other tables if needed for the caller
    return { folder: result[0] as FolderRecord };
  } else {
    const result = await db.queryDrive(
      driveId,
      "SELECT * FROM files WHERE full_directory_path = ?",
      [path]
    );
    // TODO: DRIVE Hydrate labels from file_labels junction table if needed
    return { file: result[0] as FileRecord };
  }
}

/**
 * Resolves naming conflicts based on the specified resolution strategy.
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

  const checkConflict = async (path: string) => {
    const result = await db.queryDrive(
      driveId,
      `SELECT id FROM ${tableName} WHERE full_directory_path = ?`,
      [path]
    );
    return result.length > 0;
  };

  if (
    resolution === FileConflictResolutionEnum.REPLACE ||
    resolution === FileConflictResolutionEnum.KEEP_NEWER
  ) {
    return [finalName, finalPath];
  }

  if (resolution === FileConflictResolutionEnum.KEEP_ORIGINAL) {
    if (await checkConflict(finalPath)) {
      return ["", ""]; // Signal to abort
    }
    return [finalName, finalPath];
  }

  // Default: KEEP_BOTH
  let counter = 1;
  const pathConflict = await checkConflict(finalPath);
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

    if (!(await checkConflict(finalPath))) {
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
  const disk = await diskService.getDisk(driveId, diskId);
  const rootPath = `${diskId}::/`;
  const trashPath = `${diskId}::/.trash/`;

  return dbHelpers.transaction("drive", driveId, (tx: Database) => {
    let rootFolder: FolderRecord = tx
      .prepare("SELECT * FROM folders WHERE full_directory_path = ?")
      .get(rootPath) as FolderRecord;

    if (!rootFolder) {
      const rootFolderId = GenerateID.Folder();
      const now = Date.now();
      tx.prepare(
        `INSERT INTO folders (id, name, parent_folder_id, full_directory_path, created_by_user_id, created_at, last_updated_at, last_updated_by_user_id, disk_id, disk_type, drive_id, expires_at)
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
    }

    const trashFolderResult = tx
      .prepare("SELECT id FROM folders WHERE full_directory_path = ?")
      .get(trashPath);
    if (!trashFolderResult) {
      const trashFolderId = GenerateID.Folder();
      const now = Date.now();
      tx.prepare(
        `INSERT INTO folders (id, name, parent_folder_id, full_directory_path, created_by_user_id, created_at, last_updated_at, last_updated_by_user_id, disk_id, disk_type, drive_id, expires_at, has_sovereign_permissions)
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
  const disk = await diskService.getDisk(driveId, diskId);
  let parentFolderId = await ensureRootFolder(driveId, diskId, userId);

  const pathSegments =
    fullPath
      .split("::")[1]
      ?.split("/")
      .filter((p) => p.length > 0) ?? [];
  let currentPath = `${diskId}::/`;

  for (let i = 0; i < pathSegments.length; i++) {
    const segment = pathSegments[i];
    currentPath += `${segment}/`;
    const result = await db.queryDrive(
      driveId,
      "SELECT * FROM folders WHERE full_directory_path = ?",
      [currentPath]
    );
    let folder: FolderRecord | undefined = result[0] as FolderRecord;

    if (folder) {
      parentFolderId = folder.id;
    } else {
      const newFolderId = GenerateID.Folder();
      const now = Date.now();
      const isFinalFolder = i === pathSegments.length - 1;

      await db.queryDrive(
        driveId,
        `INSERT INTO folders (id, name, parent_folder_id, full_directory_path, created_by_user_id, created_at, last_updated_at, last_updated_by_user_id, disk_id, disk_type, drive_id, expires_at, has_sovereign_permissions, shortcut_to_folder_id, notes, external_id, external_payload)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
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
          isFinalFolder ? externalPayload : undefined,
        ]
      );
      parentFolderId = newFolderId;
    }
  }
  return parentFolderId;
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
  return dbHelpers.transaction("drive", driveId, async (tx: Database) => {
    const folder = tx
      .prepare("SELECT * FROM folders WHERE id = ?")
      .get(folderId) as FolderRecord | undefined;
    if (!folder) return;

    // Note: The schema doesn't store subfolder_uuids and file_uuids directly in the `folders` table.
    // This was a misinterpretation of the Rust code. We need to query for children.
    const childFiles = tx
      .prepare("SELECT * FROM files WHERE parent_folder_id = ?")
      .all(folderId) as FileRecord[];
    const childFolders = tx
      .prepare("SELECT * FROM folders WHERE parent_folder_id = ?")
      .all(folderId) as FolderRecord[];

    for (const file of childFiles) {
      const newFilePath = file.full_directory_path.replace(oldPath, newPath);
      tx.prepare("UPDATE files SET full_directory_path = ? WHERE id = ?").run(
        newFilePath,
        file.id
      );
    }

    for (const subfolder of childFolders) {
      const newSubfolderPath = subfolder.full_directory_path.replace(
        oldPath,
        newPath
      );
      tx.prepare("UPDATE folders SET full_directory_path = ? WHERE id = ?").run(
        newSubfolderPath,
        subfolder.id
      );
      // Recursive call needs to be outside the transaction or handled carefully.
      // For simplicity here, we'll call it, but a better pattern might be needed for deep recursion.
      await updateSubfolderPaths(
        driveId,
        subfolder.id,
        subfolder.full_directory_path,
        newSubfolderPath
      );
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
  const baseUrl = process.env.BASE_URL || "http://localhost:3000";
  return `${baseUrl}/v1/drives/${driveId}/directory/asset/${fileId}.${extension}`;
}
