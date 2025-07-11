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
} from "@officexapp/types";
import { db } from "../database";
import { FolderRecord, FileRecord } from "@officexapp/types";
import { permissionsService } from "../permissions"; // TODO: Implement permissions service
import { diskService } from "../disk"; // TODO: Implement disk service

/**
 * Sanitizes a file path by replacing multiple slashes with a single one and removing trailing slashes.
 * @param filePath - The file path to sanitize.
 * @returns The sanitized file path.
 */
export function sanitizeFilePath(filePath: string): string {
  const [storagePart, ...pathParts] = filePath.split("::");
  if (!storagePart) {
    return filePath;
  }
  const pathPart = pathParts.join("::");

  // Replace colons and multiple slashes
  const sanitized = pathPart.replace(/:/g, ";").replace(/\/+/g, "/");

  // Don't trim the leading slash, only trailing
  return `${storagePart}::${sanitized.replace(/\/$/, "")}`;
}

/**
 * Splits a full path into its parent folder path and the final component (file/folder name).
 * @param fullPath - The full path string.
 * @returns A tuple containing the folder path and the file/folder name.
 */
export function splitPath(fullPath: string): [string, string] {
  const parts = fullPath.rsplit("/", 1);
  if (parts.length === 2) {
    return [`${parts[0]}/`, parts[1]]; // [folder, filename]
  }
  // Handle root-level files/folders
  const [storagePart, namePart] = fullPath.split("::");
  if (storagePart && namePart) {
    return [`${storagePart}::/`, namePart];
  }
  return ["", fullPath]; // Should not happen with valid paths
}

/**
 * Translates a full directory path to a file or folder record.
 * @param driveId - The ID of the drive.
 * @param path - The full directory path.
 * @returns An object containing the folder or file record if found.
 */
export async function translatePathToId(
  driveId: DriveID,
  path: DriveFullFilePath
): Promise<{ folder?: FolderRecord; file?: FileRecord }> {
  // A path ending in '/' is always a folder
  const isFolderPath = path.endsWith("/");

  if (isFolderPath) {
    const result = await db.queryDrive(
      driveId,
      "SELECT * FROM folders WHERE full_directory_path = ?",
      [path]
    );
    // TODO: Hydrate labels, subfolder_uuids, file_uuids from other tables
    return { folder: result[0] as FolderRecord };
  } else {
    const result = await db.queryDrive(
      driveId,
      "SELECT * FROM files WHERE full_directory_path = ?",
      [path]
    );
    // TODO: Hydrate labels from file_labels junction table
    return { file: result[0] as FileRecord };
  }
}

/**
 * Resolves naming conflicts based on the specified resolution strategy.
 * @param driveId - The ID of the drive.
 * @param basePath - The base path of the parent directory.
 * @param name - The original name of the item.
 * @param isFolder - True if the item is a folder.
 * @param resolution - The conflict resolution strategy.
 * @returns A tuple of the final name and final full path.
 */
export async function resolveNamingConflict(
  driveId: DriveID,
  basePath: string,
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
    // These strategies will overwrite, so the initial name is fine. The caller will handle deletion.
    return [finalName, finalPath];
  }

  if (resolution === FileConflictResolutionEnum.KEEP_ORIGINAL) {
    // If a conflict exists, signal the caller to abort by returning empty strings.
    if (await checkConflict(finalPath)) {
      return ["", ""];
    }
    return [finalName, finalPath];
  }

  // Default to KEEP_BOTH
  let counter = 1;
  while (await checkConflict(finalPath)) {
    counter++;
    const nameParts = name.rsplit(".", 1);
    const hasExtension = !isFolder && nameParts.length === 2;
    const baseName = hasExtension ? nameParts[0] : name;
    const extension = hasExtension ? nameParts[1] : "";

    finalName = `${baseName} (${counter})${hasExtension ? `.${extension}` : ""}`;
    finalPath =
      `${basePath.replace(/\/$/, "")}/${finalName}` + (isFolder ? "/" : "");
  }

  return [finalName, finalPath];
}

/**
 * Ensures the root and .trash folders exist for a given disk.
 * @param driveId - The ID of the drive.
 * @param diskId - The ID of the disk.
 * @param userId - The ID of the user creating the folders.
 * @returns The FolderID of the root folder.
 */
export async function ensureRootFolder(
  driveId: DriveID,
  diskId: DiskID,
  userId: UserID
): Promise<FolderID> {
  const disk = await diskService.getDisk(driveId, diskId); // Assumes a diskService
  const rootPath = `${diskId}::/`;
  const trashPath = `${diskId}::/.trash/`;

  return db.transaction("drive", driveId, async (tx) => {
    // Check for root folder
    let rootFolderResult = await tx
      .prepare("SELECT * FROM folders WHERE full_directory_path = ?")
      .all(rootPath);
    let rootFolder: FolderRecord = rootFolderResult[0] as FolderRecord;

    if (!rootFolder) {
      const rootFolderId = GenerateID.Folder();
      const now = Date.now();
      await tx
        .prepare(
          `INSERT INTO folders (id, name, parent_folder_id, full_directory_path, created_by_user_id, created_at, last_updated_at, last_updated_by_user_id, disk_id, disk_type, drive_id, expires_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
        )
        .run(
          rootFolderId,
          "", // Root folder name is empty
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
      rootFolderResult = await tx
        .prepare("SELECT * FROM folders WHERE id = ?")
        .all(rootFolderId);
      rootFolder = rootFolderResult[0] as FolderRecord;
    }

    // Check for trash folder
    const trashFolderResult = await tx
      .prepare("SELECT id FROM folders WHERE full_directory_path = ?")
      .all(trashPath);
    if (trashFolderResult.length === 0) {
      const trashFolderId = GenerateID.Folder();
      const now = Date.now();
      await tx
        .prepare(
          `INSERT INTO folders (id, name, parent_folder_id, full_directory_path, created_by_user_id, created_at, last_updated_at, last_updated_by_user_id, disk_id, disk_type, drive_id, expires_at, has_sovereign_permissions)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
        )
        .run(
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
          1 // has_sovereign_permissions = true
        );
    }
    return rootFolder.id;
  });
}

/**
 * Creates a nested folder structure if it doesn't already exist.
 * @returns The FolderID of the final folder in the path.
 */
export async function ensureFolderStructure(
  driveId: DriveID,
  fullPath: string,
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

  for (const segment of pathSegments) {
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
          hasSovereignPermissions ? 1 : 0,
          shortcutTo,
          notes,
          externalId,
          externalPayload,
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
  return db.transaction("drive", driveId, async (tx) => {
    const { files = [], subfolder_uuids = [] } =
      ((await tx
        .prepare("SELECT file_uuids, subfolder_uuids FROM folders WHERE id = ?")
        .get(folderId)) as any) ?? {};

    // Update child files
    for (const fileId of files) {
      const file = (await tx
        .prepare("SELECT * FROM files WHERE id = ?")
        .get(fileId)) as FileRecord;
      if (file) {
        const newFilePath = file.full_directory_path.replace(oldPath, newPath);
        await tx
          .prepare("UPDATE files SET full_directory_path = ? WHERE id = ?")
          .run(newFilePath, fileId);
      }
    }

    // Recursively update child folders
    for (const subfolderId of subfolder_uuids) {
      const subfolder = (await tx
        .prepare("SELECT * FROM folders WHERE id = ?")
        .get(subfolderId)) as FolderRecord;
      if (subfolder) {
        const newSubfolderPath = subfolder.full_directory_path.replace(
          oldPath,
          newPath
        );
        await tx
          .prepare("UPDATE folders SET full_directory_path = ? WHERE id = ?")
          .run(newSubfolderPath, subfolderId);
        // Recursive call
        await updateSubfolderPaths(
          driveId,
          subfolderId,
          subfolder.full_directory_path,
          newSubfolderPath
        );
      }
    }
  });
}

/**
 * Generates the publicly accessible URL for a file asset.
 * @param driveId - The ID of the drive.
 * @param fileId - The ID of the file.
 * @param extension - The file extension.
 * @returns The formatted URL string.
 */
export function formatFileAssetPath(
  driveId: DriveID,
  fileId: FileID,
  extension: string
): string {
  // TODO: This should be configured via environment variables
  const baseUrl = process.env.BASE_URL || "http://localhost:3000";
  return `${baseUrl}/v1/drives/${driveId}/directory/asset/${fileId}.${extension}`;
}
