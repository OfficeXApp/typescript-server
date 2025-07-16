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
  DirectoryResourceID, // Import DirectoryResourceID
  DirectoryPermissionType, // Import DirectoryPermissionType
  FilePathBreadcrumb,
  DriveClippedFilePath, // Import FilePathBreadcrumb
  DiskTypeEnum,
  DiskID,
  FileVersionID,
} from "@officexapp/types";
import { db, dbHelpers } from "../database";
import * as internals from "./internals";
import type { Database } from "better-sqlite3";

// Import actual permission services
import {
  deriveDirectoryBreadcrumbs, // Renamed to avoid conflict
  checkDirectoryPermissions, // Import checkDirectoryPermissions
} from "../permissions/directory";
import { getDriveOwnerId } from "../../routes/v1/types"; // This is needed for permission checks
import { generate_s3_upload_url } from "../disks/aws_s3"; // Assuming these are correctly implemented

async function get_disk_from_db(
  driveId: DriveID,
  diskId: DiskID
): Promise<any> {
  const [disk] = await db.queryDrive(
    driveId,
    "SELECT * FROM disks WHERE id = ?",
    [diskId]
  );
  return disk;
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

  // PERMIT: Add permission check for parent folder CREATE permission
  const parentFolderResourceId: DirectoryResourceID =
    `${parent_folder_uuid}` as DirectoryResourceID;
  const hasCreatePermission = (
    await checkDirectoryPermissions(parentFolderResourceId, userId, driveId)
  ).includes(DirectoryPermissionType.UPLOAD); // Rust uses Upload for create file

  const isOwner = (await getDriveOwnerId(driveId)) === userId;

  console.log(
    `Requesting user ${userId} has create permission: ${hasCreatePermission}. meanwhile the owner of drive ${driveId} is ${isOwner}`
  );

  if (!isOwner && !hasCreatePermission) {
    throw new Error(
      `Permission denied: User ${userId} cannot create files in folder ${parent_folder_uuid}.`
    );
  }

  const parentFolder = (
    await db.queryDrive(driveId, "SELECT * FROM folders WHERE id = ?", [
      parent_folder_uuid,
    ])
  )[0] as FolderRecord;
  if (!parentFolder) throw new Error("Parent folder not found.");

  const disk = await get_disk_from_db(driveId, disk_id);
  if (!disk) throw new Error("Disk not found.");

  if (
    disk.disk_type !== DiskTypeEnum.AwsBucket &&
    disk.disk_type !== DiskTypeEnum.StorjWeb3 &&
    disk.disk_type !== DiskTypeEnum.IcpCanister
  ) {
    throw new Error(
      "Only S3 buckets, Storj & ICP Canisters are supported for file uploads"
    );
  }

  // Handle naming conflicts
  const [finalName, finalPath] = await internals.resolveNamingConflict(
    driveId,
    parentFolder.full_directory_path,
    params.name,
    false,
    file_conflict_resolution
  );

  if (
    !finalName &&
    file_conflict_resolution === FileConflictResolutionEnum.KEEP_ORIGINAL
  ) {
    const existingFile = (
      await db.queryDrive(
        driveId,
        "SELECT * FROM files WHERE full_directory_path = ?",
        [finalPath]
      )
    )[0] as FileRecord;
    if (existingFile) {
      // For KEEP_ORIGINAL, if a file exists, we return an error as per Rust logic.
      // If no file exists, proceed with creation.
      throw new Error("File already exists and resolution is KEEP_ORIGINAL");
    }
  }

  const newFileId = params.id || GenerateID.File();
  const extension = finalName.split(".").pop() || "";
  const now = Date.now();
  const versionId = GenerateID.FileVersionID();

  // Determine file_version and prior_version based on conflict resolution
  let fileVersion = 1;
  let priorVersion: string | undefined = undefined;
  let fileIdToUse = newFileId;

  if (
    file_conflict_resolution === FileConflictResolutionEnum.REPLACE ||
    file_conflict_resolution === FileConflictResolutionEnum.KEEP_NEWER
  ) {
    const existingFile = (
      await db.queryDrive(
        driveId,
        "SELECT * FROM files WHERE full_directory_path = ?",
        [finalPath]
      )
    )[0] as FileRecord;

    if (existingFile) {
      const existingFileVersion = (
        await db.queryDrive(
          driveId,
          "SELECT * FROM file_versions WHERE version_id = ?",
          [existingFile.version_id]
        )
      )[0];

      fileVersion = (existingFileVersion?.file_version || 0) + 1;
      priorVersion = existingFile.version_id;
      fileIdToUse = existingFile.id; // Use existing file ID for new version

      if (
        file_conflict_resolution === FileConflictResolutionEnum.KEEP_NEWER &&
        existingFile.last_updated_date_ms > now
      ) {
        // If existing file is newer, return it and generate upload URL for it
        let uploadResponse: DiskUploadResponse = { url: "", fields: {} };
        if (disk.disk_type === DiskTypeEnum.AwsBucket) {
          const awsAuth = JSON.parse(disk.auth_json);
          const result = await generate_s3_upload_url(
            existingFile.id,
            existingFile.extension,
            awsAuth,
            driveId,
            BigInt(file_size),
            BigInt(24 * 60 * 60),
            disk_id,
            existingFile.name
          );
          if (result.ok) uploadResponse = result.ok;
          else throw new Error(result.err);
        } else if (disk.disk_type === DiskTypeEnum.StorjWeb3) {
          const storjAuth = JSON.parse(disk.auth_json);
          const result = await generate_s3_upload_url(
            existingFile.id,
            existingFile.extension,
            storjAuth,
            driveId,
            BigInt(file_size),
            BigInt(24 * 60 * 60),
            disk_id,
            existingFile.name
          );
          if (result.ok) uploadResponse = result.ok;
          else throw new Error(result.err);
        } else if (disk.disk_type === DiskTypeEnum.IcpCanister) {
          // ICP Canister handles uploads differently, no presigned URL needed
          uploadResponse = { url: "", fields: {} };
        } else {
          throw new Error(
            `Unsupported disk type for upload URL generation: ${disk.disk_type}`
          );
        }
        return [existingFile, uploadResponse];
      }
    }
  }

  const fileRecord: FileRecord = {
    id: fileIdToUse,
    name: finalName,
    parent_folder_uuid,
    version_id: versionId,
    file_version: fileVersion,
    prior_version: priorVersion,
    extension: extension,
    full_directory_path: finalPath,
    labels: params.labels || [],
    created_by: userId,
    created_at: now,
    disk_id: disk_id,
    disk_type: disk.disk_type,
    file_size: file_size,
    raw_url:
      params.raw_url ??
      internals.formatFileAssetPath(driveId, fileIdToUse, extension),
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
    // If replacing an existing file, update the old version's next_version
    if (priorVersion) {
      tx.prepare(
        `UPDATE file_versions SET next_version_id = ? WHERE version_id = ?`
      ).run(versionId, priorVersion);
    }
    // Update the main file record
    // Add 'deleted' and 'restore_trash_prior_folder_uuid' to the column list and values
    tx.prepare(
      `
    INSERT OR REPLACE INTO files (
      id, name, parent_folder_id, version_id, extension, full_directory_path, created_by, created_at,
      disk_id, disk_type, file_size, raw_url, last_updated_date_ms, last_updated_by, deleted,
      drive_id, upload_status, expires_at, restore_trash_prior_folder_uuid,
      has_sovereign_permissions, shortcut_to, notes, external_id, external_payload
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `
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
      fileRecord.deleted ? 1 : 0, // Value for 'deleted' (boolean to integer)
      driveId,
      fileRecord.upload_status,
      fileRecord.expires_at,
      fileRecord.restore_trash_prior_folder_uuid, // Value for 'restore_trash_prior_folder_uuid'
      fileRecord.has_sovereign_permissions ? 1 : 0,
      fileRecord.shortcut_to,
      fileRecord.notes,
      fileRecord.external_id,
      fileRecord.external_payload
    );

    // Insert new version record
    tx.prepare(
      `
        INSERT INTO file_versions (version_id, file_id, name, file_version, prior_version_id, extension, created_by, created_at, disk_id, disk_type, file_size, raw_url, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `
    ).run(
      versionId,
      fileRecord.id,
      fileRecord.name,
      fileRecord.file_version,
      fileRecord.prior_version,
      fileRecord.extension,
      userId,
      now,
      fileRecord.disk_id,
      fileRecord.disk_type,
      fileRecord.file_size,
      fileRecord.raw_url,
      fileRecord.notes
    );

    // Update parent folder's file_uuids list if this is a new file for the folder
    // Note: This is simpler than Rust's update_folder_file_uuids as we're not tracking old values.
    // It's assumed the `files` table's parent_folder_id handles the relationship.
    // If the Rust logic means `FolderRecord.file_uuids` in memory, we need to manually update it too.
    // Given we are using SQLite, we query the list dynamically rather than maintaining it in a cached struct.
  });

  let uploadResponse: DiskUploadResponse = { url: "", fields: {} };
  if (fileRecord.upload_status === UploadStatus.QUEUED) {
    if (disk.disk_type === DiskTypeEnum.AwsBucket) {
      const awsAuth = JSON.parse(disk.auth_json);
      const result = await generate_s3_upload_url(
        fileRecord.id,
        fileRecord.extension,
        awsAuth,
        driveId,
        BigInt(file_size),
        BigInt(24 * 60 * 60),
        disk_id,
        fileRecord.name
      );
      if (result.ok) uploadResponse = result.ok;
      else throw new Error(result.err);
    } else if (disk.disk_type === DiskTypeEnum.StorjWeb3) {
      const storjAuth = JSON.parse(disk.auth_json);
      const result = await generate_s3_upload_url(
        fileRecord.id,
        fileRecord.extension,
        storjAuth,
        driveId,
        BigInt(file_size),
        BigInt(24 * 60 * 60),
        disk_id,
        fileRecord.name
      );
      if (result.ok) uploadResponse = result.ok;
      else throw new Error(result.err);
    } else if (disk.disk_type === DiskTypeEnum.IcpCanister) {
      // ICP Canister handles uploads differently, no presigned URL needed
      uploadResponse = { url: "", fields: {} };
    } else {
      throw new Error(
        `Unsupported disk type for upload URL generation: ${disk.disk_type}`
      );
    }
  }

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

  // PERMIT: Add permission check for parent folder CREATE permission
  const parentFolderResourceId: DirectoryResourceID =
    `${parent_folder_uuid}` as DirectoryResourceID;
  const hasCreatePermission = (
    await checkDirectoryPermissions(parentFolderResourceId, userId, driveId)
  ).includes(DirectoryPermissionType.UPLOAD); // Rust uses Upload for create folder

  const isOwner = (await getDriveOwnerId(driveId)) === userId;

  if (!isOwner && !hasCreatePermission) {
    throw new Error(
      `Permission denied: User ${userId} cannot create folders in this location.`
    );
  }

  const parentFolder = (
    await db.queryDrive(driveId, "SELECT * FROM folders WHERE id = ?", [
      parent_folder_uuid,
    ])
  )[0];
  if (!parentFolder) throw new Error("Parent folder not found.");

  // Handle naming conflicts
  const [finalName, finalPath] = await internals.resolveNamingConflict(
    driveId,
    parentFolder.full_directory_path,
    params.name,
    true, // isFolder = true
    params.file_conflict_resolution
  );

  if (!finalName) {
    // If finalName is empty, it means KEEP_ORIGINAL was chosen and a conflict exists.
    throw new Error(
      "A folder with this name already exists and resolution strategy prevents creation."
    );
  }

  // Ensure disk type is set correctly
  const disk = await get_disk_from_db(driveId, disk_id);
  if (!disk) {
    throw new Error("Disk not found.");
  }
  const diskType = disk.disk_type;

  const folderId = await internals.ensureFolderStructure(
    driveId,
    finalPath, // Use finalPath after conflict resolution
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
  permanent: boolean,
  userId: UserID // PERMIT: Add userId for permission check
): Promise<void> {
  // Determine if it's a file or folder for permission check
  const isFile = resourceId.startsWith(IDPrefixEnum.File);
  const resourceType = isFile ? "File" : "Folder";
  const tableName = isFile ? "files" : "folders";

  // Fetch the resource to get its parent_folder_id for permission check
  const resource: any = (
    await db.queryDrive(driveId, `SELECT * FROM ${tableName} WHERE id = ?`, [
      resourceId,
    ])
  )[0];

  if (!resource) {
    throw new Error(`${resourceType} not found.`);
  }

  // Prevent deletion of root and .trash folders (Rust logic)
  if (
    !isFile && // Only for folders
    (resource.parent_folder_id === null || resource.name === ".trash")
  ) {
    throw new Error("Cannot delete root or .trash folders.");
  }

  // If resource is already in trash, only allow permanent deletion (Rust logic)
  if (resource.restore_trash_prior_folder_uuid && !permanent) {
    throw new Error("Cannot move to trash: item is already in trash.");
  }

  const parentFolderId = resource.parent_folder_id;
  const parentFolderResourceId: DirectoryResourceID =
    `${parentFolderId}` as DirectoryResourceID;

  // PERMIT: Add permission check for DELETE permission on the parent folder
  const hasDeletePermission = (
    await checkDirectoryPermissions(parentFolderResourceId, userId, driveId)
  ).includes(DirectoryPermissionType.DELETE);

  const isOwner = (await getDriveOwnerId(driveId)) === userId;

  if (!isOwner && !hasDeletePermission) {
    throw new Error(
      `Permission denied: User ${userId} cannot delete ${resourceType.toLowerCase()} ${resourceId}.`
    );
  }

  return dbHelpers.transaction("drive", driveId, async (tx: Database) => {
    if (permanent) {
      if (isFile) {
        tx.prepare("DELETE FROM file_versions WHERE file_id = ?").run(
          resourceId
        );
        tx.prepare("DELETE FROM files WHERE id = ?").run(resourceId);
        // Also remove from parent's file_uuids if we were maintaining it in memory (Rust)
        // With SQLite, this implicit association through parent_folder_id in the files table is enough.
      } else {
        // Recursive folder deletion for permanent mode
        // Need to find all children (files and folders) and delete them as well.
        console.warn("Recursive folder deletion is not implemented.");
        // --- START RECURSIVE FOLDER DELETION IMPLEMENTATION ---
        const foldersToDelete: FolderID[] = [resourceId];
        const deletedFiles: FileID[] = [];
        const deletedFolders: FolderID[] = [];

        let folderIdx = 0;
        while (folderIdx < foldersToDelete.length) {
          const currentFolderId = foldersToDelete[folderIdx++];

          // Get subfolders and files of the current folder
          const subfolders = tx
            .prepare("SELECT id FROM folders WHERE parent_folder_id = ?")
            .all(currentFolderId) as { id: FolderID }[];
          const filesInFolder = tx
            .prepare("SELECT id FROM files WHERE parent_folder_id = ?")
            .all(currentFolderId) as { id: FileID }[];

          // Add subfolders to the queue for processing
          for (const sub of subfolders) {
            foldersToDelete.push(sub.id);
          }

          // Delete files in the current folder
          for (const file of filesInFolder) {
            tx.prepare("DELETE FROM file_versions WHERE file_id = ?").run(
              file.id
            );
            tx.prepare("DELETE FROM files WHERE id = ?").run(file.id);
            if (deletedFiles.length < 2000) {
              // Limit collected IDs as in Rust
              deletedFiles.push(file.id);
            }
          }
          if (deletedFolders.length < 2000) {
            deletedFolders.push(currentFolderId);
          }
        }

        // Finally, delete the folders themselves, starting from deepest
        // Process in reverse order to delete children before parents
        for (let i = foldersToDelete.length - 1; i >= 0; i--) {
          const folderIdToDelete = foldersToDelete[i];
          tx.prepare("DELETE FROM folders WHERE id = ?").run(folderIdToDelete);
        }
        // --- END RECURSIVE FOLDER DELETION IMPLEMENTATION ---
      }
    } else {
      // Move to trash (soft delete)
      const disk_rec = (
        await db.queryDrive(driveId, "SELECT * FROM disks WHERE id = ?", [
          resource.disk_id,
        ])
      )[0];
      if (!disk_rec) throw new Error("Disk not found for resource.");

      const trashFolderId = disk_rec.trash_folder_id; // Get trash folder ID from disk record

      // Update parent_folder_id to trash folder's ID
      tx.prepare(
        `UPDATE ${tableName} SET deleted = 1, restore_trash_prior_folder_uuid = ?, parent_folder_id = ? WHERE id = ?`
      ).run(resource.parent_folder_id, trashFolderId, resourceId);

      // If it's a folder, also update all its children to be in trash (by setting their restore_trash_prior_folder_uuid)
      // This is the Rust logic for delete_folder non-permanent path.
      if (!isFile) {
        const stack: FolderID[] = [resourceId];
        while (stack.length > 0) {
          const currentFolderId = stack.pop()!;
          // Update current folder's restore_trash_prior_folder_uuid to its actual parent before being moved
          tx.prepare(
            `UPDATE folders SET restore_trash_prior_folder_uuid = parent_folder_id WHERE id = ?`
          ).run(currentFolderId);

          const subfolders = tx
            .prepare("SELECT id FROM folders WHERE parent_folder_id = ?")
            .all(currentFolderId) as { id: FolderID }[];
          for (const sub of subfolders) {
            stack.push(sub.id);
          }

          const files = tx
            .prepare("SELECT id FROM files WHERE parent_folder_id = ?")
            .all(currentFolderId) as { id: FileID }[];
          for (const file_in_folder of files) {
            tx.prepare(
              `UPDATE files SET restore_trash_prior_folder_uuid = ? WHERE id = ?`
            ).run(currentFolderId, file_in_folder.id);
          }
        }
        // Finally, the move logic would update the full paths for all items in the moved folder.
        // Rust's `move_folder` function is then called within `delete_folder`.
        // We will call `moveFolder` here, adapting it for this use case.
        // It needs to be the synchronous version or a simplified version for transactions.
        // For now, given the prompt, we'll do the direct UPDATE, but a full solution would use moveFolder.
        const movedFolder = await moveFolderTransaction(
          tx,
          driveId,
          userId,
          resourceId,
          trashFolderId,
          FileConflictResolutionEnum.KEEP_BOTH
        );
      }
    }
  });
}

/**
 * Copies a file to a new destination folder.
 */
export async function copyFile(
  driveId: DriveID,
  userId: UserID, // PERMIT: Add userId for permission check
  fileId: FileID,
  destinationFolderId: FolderID,
  resolution: FileConflictResolutionEnum | undefined,
  newCopyId?: FileID
): Promise<FileRecord> {
  return dbHelpers.transaction("drive", driveId, async (tx: Database) => {
    const sourceFile = tx
      .prepare("SELECT * FROM files WHERE id = ?")
      .get(fileId) as FileRecord;
    const destFolder = tx
      .prepare("SELECT * FROM folders WHERE id = ?")
      .get(destinationFolderId) as FolderRecord;

    if (!sourceFile || !destFolder)
      throw new Error("Source file or destination folder not found.");
    if (sourceFile.disk_id !== destFolder.disk_id)
      throw new Error("Cannot copy between different disks.");

    // PERMIT: Check for VIEW permission on source file and UPLOAD/EDIT/MANAGE on destination folder
    const sourceFileResourceId: DirectoryResourceID =
      `${fileId}` as DirectoryResourceID;
    const destFolderResourceId: DirectoryResourceID =
      `${destinationFolderId}` as DirectoryResourceID;

    const hasViewSourcePermission = (
      await checkDirectoryPermissions(sourceFileResourceId, userId, driveId)
    ).includes(DirectoryPermissionType.VIEW);

    const hasCreateDestPermission = (
      await checkDirectoryPermissions(destFolderResourceId, userId, driveId)
    ).includes(DirectoryPermissionType.UPLOAD);

    const isOwner = (await getDriveOwnerId(driveId)) === userId;

    if (!isOwner && (!hasViewSourcePermission || !hasCreateDestPermission)) {
      throw new Error(
        `Permission denied: User ${userId} cannot copy file ${fileId} to folder ${destinationFolderId}.`
      );
    }

    // Resolve naming conflict for the new file
    const [finalName, finalPath] = await internals.resolveNamingConflict(
      driveId,
      destFolder.full_directory_path,
      sourceFile.name,
      false, // is_folder = false
      resolution
    );

    if (!finalName) {
      // If finalName is empty, it means KEEP_ORIGINAL was chosen and a conflict exists.
      // In copy_file, Rust returns the existing file.
      const existingFile = (
        await db.queryDrive(
          driveId,
          "SELECT * FROM files WHERE full_directory_path = ?",
          [finalPath]
        )
      )[0] as FileRecord;
      if (existingFile) return existingFile;
      throw new Error(
        "File already exists and resolution strategy prevents creation."
      );
    }

    const newFileUuid = newCopyId || GenerateID.File();
    const now = Date.now();
    const newVersionId = GenerateID.FileVersionID();

    // Create new file record
    const newFileRecord: FileRecord = {
      id: newFileUuid,
      name: finalName,
      parent_folder_uuid: destinationFolderId,
      version_id: newVersionId,
      file_version: 1, // New copy always starts at version 1
      prior_version: undefined,
      extension: sourceFile.extension,
      full_directory_path: finalPath,
      labels: sourceFile.labels, // Keep original labels
      created_by: userId, // New creator
      created_at: now, // New creation time
      disk_id: sourceFile.disk_id,
      disk_type: sourceFile.disk_type,
      file_size: sourceFile.file_size,
      raw_url: internals.formatFileAssetPath(
        driveId,
        newFileUuid,
        sourceFile.extension
      ), // New asset path
      last_updated_date_ms: now,
      last_updated_by: userId,
      deleted: false,
      drive_id: driveId,
      expires_at: sourceFile.expires_at,
      restore_trash_prior_folder_uuid: undefined,
      has_sovereign_permissions: sourceFile.has_sovereign_permissions,
      shortcut_to: sourceFile.shortcut_to,
      upload_status: sourceFile.upload_status, // Keep original upload status
      external_id: sourceFile.external_id,
      external_payload: sourceFile.external_payload,
      notes: sourceFile.notes,
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

    // Insert new version record
    tx.prepare(
      `INSERT INTO file_versions(version_id, file_id, name, file_version, prior_version_id, extension, created_by, created_at, disk_id, disk_type, file_size, raw_url, notes)
       VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(
      newVersionId,
      newFileUuid,
      finalName,
      1,
      undefined, // No prior version for a new copy
      sourceFile.extension,
      userId,
      now,
      sourceFile.disk_id,
      sourceFile.disk_type,
      sourceFile.file_size,
      newFileRecord.raw_url,
      sourceFile.notes
    );

    // Update destination folder's file_uuids.
    // In SQLite, we rely on the `files` table having `parent_folder_id`.
    // If we need to update the `subfolder_uuids` or `file_uuids` arrays directly on the `folders` table,
    // that logic would go here. For now, we assume the relational queries handle it.

    // If this is an S3 or Storj bucket, perform copy operation (asynchronous)
    if (
      sourceFile.disk_type === DiskTypeEnum.AwsBucket ||
      sourceFile.disk_type === DiskTypeEnum.StorjWeb3
    ) {
      const disk = await get_disk_from_db(driveId, sourceFile.disk_id);
      if (!disk || !disk.auth_json) {
        console.error("Missing disk or auth for copy operation.");
      } else {
        const auth = JSON.parse(disk.auth_json);
        const sourceKey = sourceFile.raw_url; // Assuming raw_url is the actual S3 key
        const destinationKey = newFileRecord.raw_url;

        // Fire and forget (Rust uses `ic_cdk::spawn`)
        (async () => {
          let copyResult;
          if (sourceFile.disk_type === DiskTypeEnum.AwsBucket) {
            copyResult = await internals.copyS3Object(
              sourceKey,
              destinationKey,
              auth
            );
          } else {
            copyResult = await internals.copyStorjObject(
              sourceKey,
              destinationKey,
              auth
            ); // Assuming similar function exists
          }
          if (copyResult.err) {
            console.error(`Cloud copy failed: ${copyResult.err}`);
          } else {
            console.log("Cloud copy completed successfully.");
          }
        })();
      }
    }

    const copiedFile = tx
      .prepare("SELECT * FROM files WHERE id = ?")
      .get(newFileUuid) as FileRecord;

    return copiedFile;
  });
}

/**
 * Copies a folder to a new destination folder recursively.
 */
export async function copyFolder(
  driveId: DriveID,
  userId: UserID, // PERMIT: Add userId for permission check
  folderId: FolderID,
  destinationFolderId: FolderID,
  resolution: FileConflictResolutionEnum | undefined,
  newCopyId?: FolderID
): Promise<FolderRecord> {
  return dbHelpers.transaction("drive", driveId, async (tx: Database) => {
    const sourceFolder = tx
      .prepare("SELECT * FROM folders WHERE id = ?")
      .get(folderId) as FolderRecord;
    const destFolder = tx
      .prepare("SELECT * FROM folders WHERE id = ?")
      .get(destinationFolderId) as FolderRecord;

    if (!sourceFolder || !destFolder)
      throw new Error("Source folder or destination folder not found.");
    if (sourceFolder.disk_id !== destFolder.disk_id)
      throw new Error("Cannot copy between different disks.");

    // PERMIT: Check for VIEW permission on source folder and UPLOAD/EDIT/MANAGE on destination folder
    const sourceFolderResourceId: DirectoryResourceID =
      `${folderId}` as DirectoryResourceID;
    const destFolderResourceId: DirectoryResourceID =
      `${destinationFolderId}` as DirectoryResourceID;

    const hasViewSourcePermission = (
      await checkDirectoryPermissions(sourceFolderResourceId, userId, driveId)
    ).includes(DirectoryPermissionType.VIEW);

    const hasCreateDestPermission = (
      await checkDirectoryPermissions(destFolderResourceId, userId, driveId)
    ).includes(DirectoryPermissionType.UPLOAD);

    const isOwner = (await getDriveOwnerId(driveId)) === userId;

    if (!isOwner && (!hasViewSourcePermission || !hasCreateDestPermission)) {
      throw new Error(
        `Permission denied: User ${userId} cannot copy folder ${folderId} to folder ${destinationFolderId}.`
      );
    }

    // Resolve naming conflict for the new folder
    const [finalName, finalPath] = await internals.resolveNamingConflict(
      driveId,
      destFolder.full_directory_path,
      sourceFolder.name,
      true, // isFolder = true
      resolution
    );

    // If finalName is empty, it means KEEP_ORIGINAL was chosen and a conflict exists.
    // In copy_folder, Rust returns the existing folder.
    if (!finalName) {
      const existingFolder = (
        await db.queryDrive(
          driveId,
          "SELECT * FROM folders WHERE full_directory_path = ?",
          [finalPath]
        )
      )[0] as FolderRecord;
      if (existingFolder) return existingFolder;
      throw new Error(
        "Folder already exists and resolution strategy prevents creation."
      );
    }

    const newFolderUuid = newCopyId || GenerateID.Folder();
    const now = Date.now();

    // Create new folder record (shallow copy initially)
    const newFolderRecord: FolderRecord = {
      id: newFolderUuid,
      name: finalName,
      parent_folder_uuid: destinationFolderId,
      subfolder_uuids: [], // Will be populated recursively
      file_uuids: [], // Will be populated recursively
      full_directory_path: finalPath,
      labels: sourceFolder.labels,
      created_by: userId,
      created_at: now,
      last_updated_date_ms: now,
      last_updated_by: userId,
      disk_id: sourceFolder.disk_id,
      disk_type: sourceFolder.disk_type,
      deleted: false,
      expires_at: sourceFolder.expires_at,
      drive_id: driveId,
      restore_trash_prior_folder_uuid: undefined,
      has_sovereign_permissions: sourceFolder.has_sovereign_permissions,
      shortcut_to: sourceFolder.shortcut_to,
      external_id: sourceFolder.external_id,
      external_payload: sourceFolder.external_payload,
      notes: sourceFolder.notes,
    };

    tx.prepare(
      `INSERT INTO folders (id, name, parent_folder_id, full_directory_path, created_by, created_at, last_updated_date_ms, last_updated_by, disk_id, disk_type, deleted, expires_at, drive_id, restore_trash_prior_folder_uuid, has_sovereign_permissions, shortcut_to, notes, external_id, external_payload)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(
      newFolderRecord.id,
      newFolderRecord.name,
      newFolderRecord.parent_folder_uuid,
      newFolderRecord.full_directory_path,
      newFolderRecord.created_by,
      newFolderRecord.created_at,
      newFolderRecord.last_updated_date_ms,
      newFolderRecord.last_updated_by,
      newFolderRecord.disk_id,
      newFolderRecord.disk_type,
      newFolderRecord.deleted ? 1 : 0,
      newFolderRecord.expires_at,
      newFolderRecord.drive_id,
      newFolderRecord.restore_trash_prior_folder_uuid,
      newFolderRecord.has_sovereign_permissions ? 1 : 0,
      newFolderRecord.shortcut_to,
      newFolderRecord.notes,
      newFolderRecord.external_id,
      newFolderRecord.external_payload
    );

    // Recursively copy subfolders and files
    // NOTE: This will recursively call copyFolder and copyFile, which will re-enter the transaction.
    // Better-sqlite3 allows nested transactions (they become savepoints), but it's important
    // that the `tx` object is passed down.
    const subfolders = tx
      .prepare("SELECT id FROM folders WHERE parent_folder_id = ?")
      .all(sourceFolder.id) as { id: FolderID }[];

    for (const sub of subfolders) {
      const copiedSubfolder = await copyFolder(
        driveId,
        userId,
        sub.id,
        newFolderRecord.id,
        resolution
      );
      // Update newFolderRecord.subfolder_uuids if in-memory sync is needed
      tx.prepare(
        `UPDATE folders SET subfolder_uuids = json_insert(coalesce(subfolder_uuids, '[]'), '$[#]', ?) WHERE id = ?`
      ).run(copiedSubfolder.id, newFolderRecord.id);
    }

    const filesInFolder = tx
      .prepare("SELECT id FROM files WHERE parent_folder_id = ?")
      .all(sourceFolder.id) as { id: FileID }[];

    for (const file_of_folder of filesInFolder) {
      const copiedFile = await copyFile(
        driveId,
        userId,
        file_of_folder.id,
        newFolderRecord.id,
        resolution
      );
      // Update newFolderRecord.file_uuids if in-memory sync is needed
      tx.prepare(
        `UPDATE folders SET file_uuids = json_insert(coalesce(file_uuids, '[]'), '$[#]', ?) WHERE id = ?`
      ).run(copiedFile.id, newFolderRecord.id);
    }

    const copiedFolder = tx
      .prepare("SELECT * FROM folders WHERE id = ?")
      .get(newFolderUuid) as FolderRecord;

    return copiedFolder;
  });
}

/**
 * Moves a file to a new destination folder.
 */
export async function moveFile(
  driveId: DriveID,
  userId: UserID, // PERMIT: Add userId for permission check
  fileId: FileID,
  destinationFolderId: FolderID,
  resolution: FileConflictResolutionEnum
): Promise<FileRecord> {
  return dbHelpers.transaction("drive", driveId, async (tx: Database) => {
    const file = tx
      .prepare("SELECT * FROM files WHERE id = ?")
      .get(fileId) as FileRecord;
    const destFolder = tx
      .prepare("SELECT * FROM folders WHERE id = ?")
      .get(destinationFolderId) as FolderRecord;

    if (!file || !destFolder) throw new Error("File or destination not found.");
    if (file.disk_id !== destFolder.disk_id)
      throw new Error("Cannot move between different disks.");

    // PERMIT: Check EDIT permission on the source file and UPLOAD/CREATE on the destination folder
    const sourceFileResourceId: DirectoryResourceID =
      `${fileId}` as DirectoryResourceID;
    const destFolderResourceId: DirectoryResourceID =
      `${destinationFolderId}` as DirectoryResourceID;

    const hasEditSourcePermission = (
      await checkDirectoryPermissions(sourceFileResourceId, userId, driveId)
    ).includes(DirectoryPermissionType.EDIT);

    const hasCreateDestPermission = (
      await checkDirectoryPermissions(destFolderResourceId, userId, driveId)
    ).includes(DirectoryPermissionType.UPLOAD);

    const isOwner = (await getDriveOwnerId(driveId)) === userId;

    if (!isOwner && (!hasEditSourcePermission || !hasCreateDestPermission)) {
      throw new Error(
        `Permission denied: User ${userId} cannot move file ${fileId} to folder ${destinationFolderId}.`
      );
    }

    // Synchronous version of resolveNamingConflict for transactions
    const [finalName, finalPath] = internals.resolveNamingConflict(
      driveId,
      destFolder.full_directory_path,
      file.name,
      false, // is_folder = false
      resolution
    );

    if (!finalName) {
      // If finalName is empty, it means KEEP_ORIGINAL was chosen and a conflict exists.
      // Rust's move_file returns the original file in this case.
      return file;
    }

    // Remove file from old parent's list (if tracking is by arrays)
    // SQL handles this by updating the parent_folder_id.

    // Update file's metadata and parent
    tx.prepare(
      "UPDATE files SET name = ?, full_directory_path = ?, parent_folder_id = ?, last_updated_date_ms = ?, last_updated_by = ? WHERE id = ?"
    ).run(
      finalName,
      finalPath,
      destinationFolderId,
      Date.now(),
      userId,
      fileId
    );

    // Update resource_path for associated directory permissions
    const permissionsToUpdate = tx
      .prepare(
        `SELECT id FROM permissions_directory WHERE resource_type = 'File' AND resource_id = ?`
      )
      .all(fileId.substring(IDPrefixEnum.File.length)) as { id: string }[];
    for (const perm of permissionsToUpdate) {
      tx.prepare(
        `UPDATE permissions_directory SET resource_path = ? WHERE id = ?`
      ).run(finalPath, perm.id);
    }

    const updatedFile = tx
      .prepare("SELECT * FROM files WHERE id = ?")
      .get(fileId) as FileRecord;
    return updatedFile;
  });
}

/**
 * Synchronous transaction helper for moveFolder.
 * This is needed because `moveFolder` can be called from `deleteResource` within a transaction.
 */
async function moveFolderTransaction(
  tx: Database,
  driveId: DriveID,
  userId: UserID,
  folderId: FolderID,
  destinationFolderId: FolderID,
  resolution: FileConflictResolutionEnum
): Promise<FolderRecord> {
  const folder = tx
    .prepare("SELECT * FROM folders WHERE id = ?")
    .get(folderId) as FolderRecord;
  const destFolder = tx
    .prepare("SELECT * FROM folders WHERE id = ?")
    .get(destinationFolderId) as FolderRecord;

  if (!folder || !destFolder)
    throw new Error("Folder or destination not found.");
  if (folder.disk_id !== destFolder.disk_id)
    throw new Error("Cannot move between different disks.");

  // Circular reference check (Rust logic)
  let currentParentId: FolderID | null | undefined = destinationFolderId;
  while (currentParentId) {
    if (currentParentId === folderId) {
      throw new Error("Cannot move folder into itself or its subdirectories.");
    }
    const parentFolder = tx
      .prepare("SELECT parent_folder_id FROM folders WHERE id = ?")
      .get(currentParentId) as {
      parent_folder_id: FolderID | null | undefined;
    };
    currentParentId = parentFolder?.parent_folder_id;
  }

  // PERMIT: Check EDIT permission on the source folder and UPLOAD/CREATE on the destination folder
  const sourceFolderResourceId: DirectoryResourceID =
    `${folderId}` as DirectoryResourceID;
  const destFolderResourceId: DirectoryResourceID =
    `${destinationFolderId}` as DirectoryResourceID;

  const hasEditSourcePermission = (
    await checkDirectoryPermissions(sourceFolderResourceId, userId, driveId)
  ).includes(DirectoryPermissionType.EDIT);

  const hasCreateDestPermission = (
    await checkDirectoryPermissions(destFolderResourceId, userId, driveId)
  ).includes(DirectoryPermissionType.UPLOAD);

  const isOwner = (await getDriveOwnerId(driveId)) === userId;

  if (!isOwner && (!hasEditSourcePermission || !hasCreateDestPermission)) {
    throw new Error(
      `Permission denied: User ${userId} cannot move folder ${folderId} to folder ${destinationFolderId}.`
    );
  }

  const oldPath = folder.full_directory_path;

  // Synchronous version of resolveNamingConflict needed for transactions
  const [finalName, finalPath] = internals.resolveNamingConflict(
    driveId,
    destFolder.full_directory_path,
    folder.name,
    true, // is_folder = true
    resolution
  );

  if (!finalName) {
    // If empty strings returned, keep original folder
    return folder;
  }

  // Update folder's metadata and parent
  tx.prepare(
    "UPDATE folders SET name = ?, full_directory_path = ?, parent_folder_id = ?, last_updated_date_ms = ?, last_updated_by = ? WHERE id = ?"
  ).run(
    finalName,
    finalPath,
    destinationFolderId,
    Date.now(),
    userId,
    folderId
  );

  // Update subfolder and file paths recursively
  await internals.updateSubfolderPaths(driveId, folderId, oldPath, finalPath);

  // Update resource_path for directory permissions associated with the moved folder
  const permissionsToUpdate = tx
    .prepare(
      `SELECT id FROM permissions_directory WHERE resource_type = 'Folder' AND resource_id = ?`
    )
    .all(folderId.substring(IDPrefixEnum.Folder.length)) as { id: string }[];
  for (const perm of permissionsToUpdate) {
    tx.prepare(
      `UPDATE permissions_directory SET resource_path = ? WHERE id = ?`
    ).run(finalPath, perm.id);
  }

  const updatedFolder = tx
    .prepare("SELECT * FROM folders WHERE id = ?")
    .get(folderId) as FolderRecord;
  return updatedFolder;
}

/**
 * Moves a folder to a new destination folder.
 */
export async function moveFolder(
  driveId: DriveID,
  userId: UserID, // PERMIT: Add userId for permission check
  folderId: FolderID,
  destinationFolderId: FolderID,
  resolution: FileConflictResolutionEnum
): Promise<FolderRecord> {
  return dbHelpers.transaction("drive", driveId, async (tx: Database) => {
    // Re-use the synchronous transaction helper
    return moveFolderTransaction(
      tx,
      driveId,
      userId,
      folderId,
      destinationFolderId,
      resolution
    );
  });
}

/**
 * Restores a file or folder from the trash.
 */
export async function restoreFromTrash(
  driveId: DriveID,
  payload: RestoreTrashPayload,
  userId: UserID // PERMIT: Add userId for permission check
): Promise<RestoreTrashResponse> {
  return dbHelpers.transaction("drive", driveId, async (tx: Database) => {
    const isFile = payload.id.startsWith(IDPrefixEnum.File);
    const tableName = isFile ? "files" : "folders";

    const resource: any = tx
      .prepare(`SELECT * FROM ${tableName} WHERE id = ? AND deleted = 1`)
      .get(payload.id);
    if (!resource) {
      throw new Error("Resource not found in trash.");
    }

    // Verify resource is actually in trash (redundant check, but matches Rust)
    if (resource.restore_trash_prior_folder_uuid === null) {
      throw new Error(`${isFile ? "File" : "Folder"} is not in trash.`);
    }

    let targetDestinationFolder: FolderRecord | null = null;
    let finalDestinationFolderId: FolderID;

    if (payload.restore_to_folder_path) {
      // User provided a specific path to restore to
      const translation = await internals.translatePathToId(
        driveId,
        payload.restore_to_folder_path
      );
      if (translation.folder) {
        targetDestinationFolder = translation.folder;
      } else {
        // If path doesn't exist, create the folder structure
        const disk = await get_disk_from_db(driveId, resource.disk_id);
        if (!disk) throw new Error("Disk not found for resource.");

        const createdFolderId = await internals.ensureFolderStructure(
          driveId,
          payload.restore_to_folder_path,
          resource.disk_id,
          userId,
          false // default to non-sovereign for created path
        );
        targetDestinationFolder = (
          await db.queryDrive(driveId, "SELECT * FROM folders WHERE id = ?", [
            createdFolderId,
          ])
        )[0] as FolderRecord;
      }
    } else {
      // Restore to original location
      targetDestinationFolder = tx
        .prepare("SELECT * FROM folders WHERE id = ?")
        .get(resource.restore_trash_prior_folder_uuid) as FolderRecord;

      // If original folder not found, restore to root of the disk
      if (!targetDestinationFolder) {
        const disk = await get_disk_from_db(driveId, resource.disk_id);
        if (!disk) throw new Error("Disk not found.");
        targetDestinationFolder = tx
          .prepare("SELECT * FROM folders WHERE id = ?")
          .get(disk.root_folder) as FolderRecord;
        if (!targetDestinationFolder)
          throw new Error("Root folder not found for disk.");
      }
    }

    if (!targetDestinationFolder) {
      throw new Error("Failed to determine restore destination.");
    }

    // Verify target folder is not in trash (Rust logic)
    if (targetDestinationFolder.restore_trash_prior_folder_uuid !== null) {
      throw new Error(
        `Cannot restore to a folder that is in trash. Please first restore ${targetDestinationFolder.full_directory_path}.`
      );
    }

    finalDestinationFolderId = targetDestinationFolder.id;

    // PERMIT: Check UPLOAD/EDIT/MANAGE permission on the target destination folder
    const targetFolderResourceId: DirectoryResourceID =
      `${finalDestinationFolderId}` as DirectoryResourceID;

    const hasPermissionToRestore = (
      await checkDirectoryPermissions(targetFolderResourceId, userId, driveId)
    ).includes(DirectoryPermissionType.UPLOAD); // Rust uses Upload for creating in a folder

    const isOwner = (await getDriveOwnerId(driveId)) === userId;

    if (!isOwner && !hasPermissionToRestore) {
      throw new Error(
        `Permission denied: User ${userId} cannot restore resource ${payload.id} to folder ${finalDestinationFolderId}.`
      );
    }

    // Use moveFile/moveFolder to handle conflicts and update paths correctly
    let restoredFileId: FileID | undefined;
    let restoredFolderId: FolderID | undefined;
    let restoredResource: FileRecord | FolderRecord;

    if (isFile) {
      restoredResource = await moveFile(
        driveId,
        userId,
        payload.id as FileID,
        finalDestinationFolderId,
        payload.file_conflict_resolution || FileConflictResolutionEnum.KEEP_BOTH // Default
      );
      restoredFileId = restoredResource.id as FileID;
    } else {
      restoredResource = await moveFolderTransaction(
        tx, // Pass the current transaction object
        driveId,
        userId,
        payload.id as FolderID,
        finalDestinationFolderId,
        payload.file_conflict_resolution || FileConflictResolutionEnum.KEEP_BOTH // Default
      );
      restoredFolderId = restoredResource.id as FolderID;
    }

    // Clear restore_trash_prior_folder_uuid and set deleted to 0
    tx.prepare(
      `UPDATE ${tableName} SET deleted = 0, restore_trash_prior_folder_uuid = NULL WHERE id = ?`
    ).run(payload.id);

    // If restoring a folder, recursively clear trash flags for its contents
    if (!isFile) {
      const foldersToProcess: FolderID[] = [payload.id as FolderID];
      let idx = 0;
      while (idx < foldersToProcess.length) {
        const currentFolderId = foldersToProcess[idx++];
        tx.prepare(
          `UPDATE folders SET deleted = 0, restore_trash_prior_folder_uuid = NULL WHERE id = ?`
        ).run(currentFolderId);
        tx.prepare(
          `UPDATE files SET deleted = 0, restore_trash_prior_folder_uuid = NULL WHERE parent_folder_id = ?`
        ).run(currentFolderId);

        const subfolders = tx
          .prepare("SELECT id FROM folders WHERE parent_folder_id = ?")
          .all(currentFolderId) as { id: FolderID }[];
        for (const sub of subfolders) {
          foldersToProcess.push(sub.id);
        }
      }
    }

    const response: RestoreTrashResponse = {
      restored_folders: restoredFolderId ? [restoredFolderId] : [],
      restored_files: restoredFileId ? [restoredFileId] : [],
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
  created_by: string;
  created_at: number;

  disk_id: string;
  disk_type: DiskTypeEnum;
  deleted: boolean;
  expires_at: number;
  drive_id: string;
  restore_trash_prior_folder_uuid: FolderID | undefined;
  has_sovereign_permissions: boolean;
  shortcut_to: FolderID | undefined;
  notes: string | undefined;
  external_id: string | undefined;
  external_payload: string | undefined;
  subfolder_uuids: FolderID[];
  file_uuids: FileID[];
  labels: string[];
  last_updated_date_ms: number;
  last_updated_by: string;
} | null> {
  const rows = await db.queryDrive(
    orgId,
    `SELECT
      id,
      name,
      parent_folder_id,
      full_directory_path,
      created_by,
      created_at,
      last_updated_date_ms,
      last_updated_by,
      disk_id,
      disk_type,
      deleted,
      expires_at,
      drive_id,
      restore_trash_prior_folder_uuid,
      has_sovereign_permissions,
      shortcut_to,
      notes,
      external_id,
      external_payload,
      subfolder_uuids,
      file_uuids,
      created_by,
      last_updated_date_ms,
      last_updated_by,
      deleted
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
    created_by: row.created_by,
    created_at: row.created_at,

    disk_id: row.disk_id,
    disk_type: row.disk_type,
    deleted: row.deleted === 1,
    expires_at: row.expires_at,
    drive_id: row.drive_id,
    restore_trash_prior_folder_uuid: row.restore_trash_prior_folder_uuid as
      | FolderID
      | undefined,
    has_sovereign_permissions: row.has_sovereign_permissions === 1,
    shortcut_to: row.shortcut_to as FolderID | undefined,
    notes: row.notes,
    external_id: row.external_id,
    external_payload: row.external_payload,
    subfolder_uuids: row.subfolder_uuids as FolderID[],
    file_uuids: row.file_uuids as FileID[],
    labels: [],
    last_updated_date_ms: row.last_updated_date_ms,
    last_updated_by: row.last_updated_by,
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
  version_id: FileVersionID;
  extension: string;
  full_directory_path: string;
  created_at: number;
  disk_id: string;
  disk_type: DiskTypeEnum;
  file_size: number;
  raw_url: string;
  drive_id: string;
  upload_status: UploadStatus;
  expires_at: number;
  restore_trash_prior_folder_uuid: FolderID | undefined;
  has_sovereign_permissions: boolean;
  shortcut_to: FileID | undefined;
  notes: string | undefined;
  external_id: string | undefined;
  external_payload: string | undefined;
  file_version: number;
  labels: string[];
  created_by: string;
  last_updated_date_ms: number;
  last_updated_by: string;
  deleted: boolean;
} | null> {
  const rows = await db.queryDrive(
    orgId,
    `SELECT
      f.id,
      f.name,
      f.parent_folder_id,
      f.version_id,
      f.extension,
      f.full_directory_path,
      f.created_by,
      f.created_at,
      f.disk_id,
      f.disk_type,
      f.file_size,
      f.raw_url,
      f.last_updated_date_ms,
      f.last_updated_by,
      f.deleted,
      f.drive_id,
      f.upload_status,
      f.expires_at,
      f.restore_trash_prior_folder_uuid,
      f.has_sovereign_permissions,
      f.shortcut_to,
      f.notes,
      f.external_id,
      f.external_payload,
      fv.file_version -- Select file_version from the joined table
    FROM files AS f
    JOIN file_versions AS fv ON f.version_id = fv.version_id -- Join file_versions table
    WHERE f.id = ?`,
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
    created_by: row.created_by,
    created_at: row.created_at,
    disk_id: row.disk_id,
    disk_type: row.disk_type,
    file_size: row.file_size,
    raw_url: row.raw_url,
    deleted: row.deleted === 1,
    drive_id: row.drive_id,
    upload_status: row.upload_status,
    expires_at: row.expires_at,
    restore_trash_prior_folder_uuid: row.restore_trash_prior_folder_uuid as
      | FolderID
      | undefined,
    has_sovereign_permissions: row.has_sovereign_permissions === 1,
    shortcut_to: row.shortcut_to as FileID | undefined,
    notes: row.notes,
    external_id: row.external_id,
    external_payload: row.external_payload,
    file_version: row.file_version, // This line is now correct
    labels: [],
    last_updated_date_ms: row.last_updated_date_ms,
    last_updated_by: row.last_updated_by,
  };
}
