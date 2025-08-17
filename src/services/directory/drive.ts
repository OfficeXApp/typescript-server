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
  Disk,
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
import { claimUUID } from "../external";

async function get_disk_from_db(
  driveId: DriveID,
  diskId: DiskID
): Promise<Disk> {
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

  let effective_expires_at = expires_at;

  // check if there is an existing file by id or path
  let hasEditPermission = false;
  if (params.id) {
    const existingFile = await getFileMetadata(driveId, params.id);
    if (existingFile) {
      hasEditPermission = (
        await checkDirectoryPermissions(
          `${existingFile.id}` as DirectoryResourceID,
          userId,
          driveId
        )
      ).includes(DirectoryPermissionType.EDIT);
    }
  }

  // PERMIT: Add permission check for parent folder CREATE permission
  const parentFolderResourceId: DirectoryResourceID =
    `${parent_folder_uuid}` as DirectoryResourceID;
  const hasCreatePermission = (
    await checkDirectoryPermissions(parentFolderResourceId, userId, driveId)
  ).includes(DirectoryPermissionType.UPLOAD); // Rust uses Upload for create file

  const isOwner = (await getDriveOwnerId(driveId)) === userId;

  if (!isOwner && !hasCreatePermission && !hasEditPermission) {
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

  if (disk.autoexpire_ms) {
    effective_expires_at = Date.now() + disk.autoexpire_ms;
  }

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
          const awsAuth = JSON.parse(disk.auth_json || "");
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
          const storjAuth = JSON.parse(disk.auth_json || "");
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

  let fileRecord: FileRecord;

  if (params.id && hasEditPermission) {
    const existingRecord = await getFileMetadata(driveId, params.id);
    if (!existingRecord) throw new Error("Existing file not found.");
    fileRecord = {
      ...existingRecord,
      name: finalName,
      parent_folder_uuid,
      version_id: versionId,
      file_version: fileVersion,
      prior_version: priorVersion,
      extension,
      full_directory_path: finalPath,
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
      expires_at: effective_expires_at,
      has_sovereign_permissions: params.has_sovereign_permissions ?? false,
      upload_status: params.raw_url
        ? UploadStatus.COMPLETED
        : UploadStatus.QUEUED,
      notes: params.notes || "",
      external_id: params.external_id || "",
      external_payload: params.external_payload || "",
    };
    await dbHelpers.transaction("drive", driveId, (tx: Database) => {
      // update prior version
      tx.prepare(
        `UPDATE file_versions SET next_version_id = ? WHERE version_id = ?`
      ).run(versionId, priorVersion);
      // create file
      tx.prepare(
        `UPDATE files SET name = ?, parent_folder_id = ?, version_id = ?, extension = ?, full_directory_path = ?, created_by = ?, created_at = ?, disk_id = ?, disk_type = ?, file_size = ?, raw_url = ?, last_updated_date_ms = ?, last_updated_by = ?, deleted = ?, drive_id = ?, expires_at = ?, has_sovereign_permissions = ?, notes = ?, external_id = ?, external_payload = ? WHERE id = ?`
      ).run(
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
        Date.now(),
        userId,
        fileRecord.deleted ? 1 : 0,
        fileRecord.drive_id,
        fileRecord.expires_at,
        fileRecord.has_sovereign_permissions ? 1 : 0,
        fileRecord.notes,
        fileRecord.external_id,
        fileRecord.external_payload,
        fileRecord.id
      );
      // insert file_versions
      tx.prepare(
        `INSERT INTO file_versions (version_id, file_id, name, file_version, prior_version_id, extension, created_by, created_at, disk_id, disk_type, file_size, raw_url, notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      ).run(
        versionId,
        fileRecord.id,
        fileRecord.name,
        fileRecord.file_version,
        fileRecord.prior_version,
        fileRecord.extension,
        userId,
        Date.now(),
        fileRecord.disk_id,
        fileRecord.disk_type,
        fileRecord.file_size,
        fileRecord.raw_url,
        fileRecord.notes
      );
    });
  } else {
    fileRecord = {
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
      expires_at: effective_expires_at,
      has_sovereign_permissions: params.has_sovereign_permissions ?? false,
      upload_status: params.raw_url
        ? UploadStatus.COMPLETED
        : UploadStatus.QUEUED,
      notes: params.notes,
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
        has_sovereign_permissions, notes, external_id, external_payload
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        fileRecord.restore_trash_prior_folder_uuid ?? "", // Value for 'restore_trash_prior_folder_uuid'
        fileRecord.has_sovereign_permissions ? 1 : 0,
        fileRecord.notes ?? "",
        fileRecord.external_id ?? "",
        fileRecord.external_payload ?? ""
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

      // Update parent folder's file_uuids list
      const parentFolder = tx
        .prepare("SELECT file_uuids FROM folders WHERE id = ?")
        .get(parent_folder_uuid) as { file_uuids: string | null };
      if (parentFolder) {
        const fileUuids = parentFolder.file_uuids
          ? JSON.parse(parentFolder.file_uuids)
          : [];
        if (!fileUuids.includes(fileRecord.id)) {
          fileUuids.push(fileRecord.id);
          tx.prepare("UPDATE folders SET file_uuids = ? WHERE id = ?").run(
            JSON.stringify(fileUuids),
            parent_folder_uuid
          );
        }
      }
    });
  }

  // @ts-ignore
  if (!fileRecord) {
    throw new Error("Failed to create file record.");
  }

  let uploadResponse: DiskUploadResponse = { url: "", fields: {} };
  if (fileRecord.upload_status === UploadStatus.QUEUED) {
    if (disk.disk_type === DiskTypeEnum.AwsBucket) {
      const awsAuth = JSON.parse(disk.auth_json || "");
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
      const storjAuth = JSON.parse(disk.auth_json || "");
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
    finalPath,
    disk_id,
    userId,
    otherParams.has_sovereign_permissions,
    otherParams.external_id,
    otherParams.external_payload,
    params.id,
    otherParams.shortcut_to,
    otherParams.notes
  );

  const [folder] = await db.queryDrive(
    driveId,
    "SELECT * FROM folders WHERE id = ?",
    [folderId]
  );

  // Update parent folder's subfolder_uuids list
  dbHelpers.transaction("drive", driveId, (tx: Database) => {
    const parent = tx
      .prepare("SELECT subfolder_uuids FROM folders WHERE id = ?")
      .get(parent_folder_uuid) as { subfolder_uuids: string | null };
    if (parent) {
      const subfolderUuids = parent.subfolder_uuids
        ? JSON.parse(parent.subfolder_uuids)
        : [];
      if (!subfolderUuids.includes(folder.id)) {
        subfolderUuids.push(folder.id);
        tx.prepare("UPDATE folders SET subfolder_uuids = ? WHERE id = ?").run(
          JSON.stringify(subfolderUuids),
          parent_folder_uuid
        );
      }
    }
  });

  return folder as FolderRecord;
}

/**
 * Deletes a file or folder.
 */
export async function deleteResource(
  driveId: DriveID,
  resourceId: FileID | FolderID,
  permanent: boolean,
  userId: UserID
): Promise<void> {
  const isFile = resourceId.startsWith(IDPrefixEnum.File);
  const tableName = isFile ? "files" : "folders";

  // --- 1. ASYNC PREPARATION (outside transaction) ---
  const resource: any = (
    await db.queryDrive(driveId, `SELECT * FROM ${tableName} WHERE id = ?`, [
      resourceId,
    ])
  )[0];

  if (!resource) {
    throw new Error(`${isFile ? "File" : "Folder"} not found.`);
  }

  const parentFolderId = resource.parent_folder_id;
  if (!parentFolderId) {
    throw new Error("Cannot delete root resource.");
  }

  const isOwner = (await getDriveOwnerId(driveId)) === userId;
  if (!isOwner) {
    const permissions = await checkDirectoryPermissions(
      `${parentFolderId}` as DirectoryResourceID,
      userId,
      driveId
    );
    if (!permissions.includes(DirectoryPermissionType.DELETE)) {
      throw new Error(
        `Permission denied to delete in folder ${parentFolderId}.`
      );
    }
  }

  let trashFolder: FolderRecord | null = null;
  if (!permanent) {
    const disk = (
      await db.queryDrive(driveId, "SELECT * FROM disks WHERE id = ?", [
        resource.disk_id,
      ])
    )[0];
    if (!disk || !disk.trash_folder)
      throw new Error("Trash folder configuration missing.");
    trashFolder = (
      await db.queryDrive(driveId, "SELECT * FROM folders WHERE id = ?", [
        disk.trash_folder,
      ])
    )[0];
    if (!trashFolder) throw new Error("Trash folder not found.");
  }

  // --- 2. SYNCHRONOUS TRANSACTION ---
  dbHelpers.transaction("drive", driveId, (tx: Database) => {
    if (permanent) {
      // --- PERMANENT DELETE LOGIC ---
      if (isFile) {
        tx.prepare("DELETE FROM file_versions WHERE file_id = ?").run(
          resourceId
        );
        tx.prepare("DELETE FROM files WHERE id = ?").run(resourceId);
      } else {
        const foldersToDelete: FolderID[] = [resourceId];
        let folderIdx = 0;
        while (folderIdx < foldersToDelete.length) {
          const currentFolderId = foldersToDelete[folderIdx++];
          const subfolders = tx
            .prepare("SELECT id FROM folders WHERE parent_folder_id = ?")
            .all(currentFolderId) as { id: FolderID }[];
          foldersToDelete.push(...subfolders.map((s) => s.id));
        }

        for (let i = foldersToDelete.length - 1; i >= 0; i--) {
          const folderIdToDelete = foldersToDelete[i];
          tx.prepare(
            "DELETE FROM file_versions WHERE file_id IN (SELECT id FROM files WHERE parent_folder_id = ?)"
          ).run(folderIdToDelete);
          tx.prepare("DELETE FROM files WHERE parent_folder_id = ?").run(
            folderIdToDelete
          );
          tx.prepare("DELETE FROM folders WHERE id = ?").run(folderIdToDelete);
        }
      }
    } else {
      // --- NON-PERMANENT (MOVE TO TRASH) LOGIC ---
      if (!trashFolder) return;

      const [finalName, finalPath] = internals.resolveNamingConflict_SYNC(
        tx,
        driveId,
        trashFolder.full_directory_path,
        resource.name,
        !isFile,
        FileConflictResolutionEnum.KEEP_BOTH
      );

      tx.prepare(
        `UPDATE ${tableName} 
         SET deleted = 1, 
             restore_trash_prior_folder_uuid = parent_folder_id,
             parent_folder_id = ?,
             name = ?,
             full_directory_path = ?
         WHERE id = ?`
      ).run(trashFolder.id, finalName, finalPath, resourceId);

      if (!isFile) {
        internals.updateSubfolderPaths_SYNC(
          tx,
          resourceId,
          resource.full_directory_path,
          finalPath,
          userId
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
  userId: UserID,
  fileId: FileID,
  destinationFolderId: FolderID,
  resolution: FileConflictResolutionEnum | undefined,
  newCopyId?: FileID
): Promise<FileRecord> {
  // ASYNC PRE-CHECKS AND DATA FETCHING
  const sourceFile = (
    await db.queryDrive(driveId, "SELECT * FROM files WHERE id = ?", [fileId])
  )[0] as FileRecord;
  const destFolder = (
    await db.queryDrive(driveId, "SELECT * FROM folders WHERE id = ?", [
      destinationFolderId,
    ])
  )[0] as FolderRecord;

  if (!sourceFile || !destFolder)
    throw new Error("Source file or destination folder not found.");
  if (sourceFile.disk_id !== destFolder.disk_id)
    throw new Error("Cannot copy between different disks.");

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

  // Resolve naming conflict for the new file (ASYNC before transaction)
  const [finalName, finalPath] = await internals.resolveNamingConflict(
    driveId,
    destFolder.full_directory_path,
    sourceFile.name,
    false, // is_folder = false
    resolution
  );

  if (!finalName) {
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

  // Create new file record (data prepared for sync insert)
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

  // SYNCHRONOUS TRANSACTION
  await dbHelpers.transaction("drive", driveId, (tx: Database) => {
    claimUUID(tx, newFileRecord.id);
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
      sourceFile.extension,
      userId,
      now,
      sourceFile.disk_id,
      sourceFile.disk_type,
      sourceFile.file_size,
      newFileRecord.raw_url,
      sourceFile.notes
    );
  });

  // ASYNC CLOUD OPERATION (OUTSIDE TRANSACTION)
  if (
    newFileRecord.upload_status === UploadStatus.QUEUED &&
    (sourceFile.disk_type === DiskTypeEnum.AwsBucket ||
      sourceFile.disk_type === DiskTypeEnum.StorjWeb3)
  ) {
    const disk = await get_disk_from_db(driveId, sourceFile.disk_id);
    if (!disk || !disk.auth_json) {
      console.error("Missing disk or auth for copy operation.");
    } else {
      const auth = JSON.parse(disk.auth_json);
      const sourceKey = sourceFile.raw_url;
      const destinationKey = newFileRecord.raw_url;

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
          );
        }
        if (copyResult.err) {
          console.error(`Cloud copy failed: ${copyResult.err}`);
        } else {
          console.log("Cloud copy completed successfully.");
        }
      })();
    }
  }

  // Fetch the newly copied file after the transaction
  const copiedFile = (
    await db.queryDrive(driveId, "SELECT * FROM files WHERE id = ?", [
      newFileUuid,
    ])
  )[0] as FileRecord;

  return copiedFile;
}

/**
 * Copies a folder to a new destination folder recursively.
 * This function will orchestrate the transaction, calling a synchronous helper.
 */
export async function copyFolder(
  driveId: DriveID,
  userId: UserID,
  folderId: FolderID,
  destinationFolderId: FolderID,
  resolution: FileConflictResolutionEnum | undefined,
  newCopyId?: FolderID
): Promise<FolderRecord> {
  // ASYNC PRE-CHECKS AND DATA FETCHING
  const sourceFolder = (
    await db.queryDrive(driveId, "SELECT * FROM folders WHERE id = ?", [
      folderId,
    ])
  )[0] as FolderRecord;
  const destFolder = (
    await db.queryDrive(driveId, "SELECT * FROM folders WHERE id = ?", [
      destinationFolderId,
    ])
  )[0] as FolderRecord;

  if (!sourceFolder || !destFolder)
    throw new Error("Source folder or destination folder not found.");
  if (sourceFolder.disk_id !== destFolder.disk_id)
    throw new Error("Cannot copy between different disks.");

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

  // Resolve naming conflict for the new folder (ASYNC before transaction)
  const [finalName, finalPath] = await internals.resolveNamingConflict(
    driveId,
    destFolder.full_directory_path,
    sourceFolder.name,
    true, // isFolder = true
    resolution
  );

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

  // SYNCHRONOUS TRANSACTION for the main folder creation and recursive calls
  return dbHelpers.transaction("drive", driveId, (tx: Database) => {
    claimUUID(tx, newFolderUuid);

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

    // Recursively copy subfolders and files using synchronous helpers
    // These functions must also accept `tx: Database` and not perform async operations inside.
    internals.copyFolderContents_SYNC(
      tx,
      driveId,
      userId,
      sourceFolder.id,
      newFolderRecord.id,
      resolution
    );

    const copiedFolder = tx
      .prepare("SELECT * FROM folders WHERE id = ?")
      .get(newFolderUuid) as FolderRecord;

    return copiedFolder; // This is the return value of the synchronous transaction
  });
}

/**
 * Moves a file to a new destination folder.
 * This function will orchestrate the transaction, calling a synchronous helper.
 */
export async function moveFile(
  driveId: DriveID,
  userId: UserID,
  fileId: FileID,
  destinationFolderId: FolderID,
  resolution: FileConflictResolutionEnum
): Promise<FileRecord> {
  // ASYNC PRE-CHECKS AND DATA FETCHING
  const file = (
    await db.queryDrive(driveId, "SELECT * FROM files WHERE id = ?", [fileId])
  )[0] as FileRecord;
  const destFolder = (
    await db.queryDrive(driveId, "SELECT * FROM folders WHERE id = ?", [
      destinationFolderId,
    ])
  )[0] as FolderRecord;

  if (!file || !destFolder) throw new Error("File or destination not found.");
  if (file.disk_id !== destFolder.disk_id)
    throw new Error("Cannot move between different disks.");

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
  // This will be called inside the synchronous transaction, so it must accept `tx`
  // We cannot call async resolveNamingConflict here because its result might change if there are parallel operations.
  // The conflict resolution needs to be part of the atomic transaction.
  // We will call the synchronous version inside the transaction.

  // SYNCHRONOUS TRANSACTION
  return dbHelpers.transaction("drive", driveId, (tx: Database) => {
    const [finalName, finalPath] = internals.resolveNamingConflict_SYNC(
      tx, // Pass the transaction object
      driveId,
      destFolder.full_directory_path,
      file.name,
      false, // is_folder = false
      resolution
    );

    if (!finalName) {
      return file; // Rust's move_file returns the original file in this case.
    }

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

    const permissionsToUpdate = tx
      .prepare(
        `SELECT id FROM permissions_directory WHERE resource_type = 'File' AND resource_id = ?`
      )
      .all(fileId) as { id: string }[];
    for (const perm of permissionsToUpdate) {
      tx.prepare(
        `UPDATE permissions_directory SET resource_path = ? WHERE id = ?`
      ).run(finalPath, perm.id);
    }

    const updatedFile = tx
      .prepare("SELECT * FROM files WHERE id = ?")
      .get(fileId) as FileRecord;
    return updatedFile; // This is the return value of the synchronous transaction
  });
}

/**
 * Synchronous transaction helper for moveFolder.
 * This is designed to be called *within* an existing transaction.
 */
function moveFolderTransaction_SYNC(
  tx: Database,
  driveId: DriveID,
  userId: UserID,
  folderId: FolderID,
  destinationFolderId: FolderID,
  resolution: FileConflictResolutionEnum
): FolderRecord {
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

  // Circular reference check (Synchronous)
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

  // Permission checks would normally be ASYNC, but this function is meant to be called INSIDE
  // a transaction. The outer `moveFolder` should handle permissions.
  // If `moveFolderTransaction_SYNC` is called directly, you'd need async checks here,
  // making it impossible to be a pure sync function.
  // For `restoreFromTrash`, the permission check is done higher up, before calling this.

  const oldPath = folder.full_directory_path;

  const [finalName, finalPath] = internals.resolveNamingConflict_SYNC(
    tx, // Pass the transaction object
    driveId,
    destFolder.full_directory_path,
    folder.name,
    true, // is_folder = true
    resolution
  );

  if (!finalName) {
    return folder; // If empty strings returned, keep original folder
  }

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

  // Update subfolder and file paths recursively (synchronous)
  internals.updateSubfolderPaths_SYNC(tx, folderId, oldPath, finalPath, userId);

  // Update resource_path for directory permissions associated with the moved folder
  const permissionsToUpdate = tx
    .prepare(
      `SELECT id FROM permissions_directory WHERE resource_type = 'Folder' AND resource_id = ?`
    )
    .all(folderId) as { id: string }[];
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
  userId: UserID,
  folderId: FolderID,
  destinationFolderId: FolderID,
  resolution: FileConflictResolutionEnum
): Promise<FolderRecord> {
  // ASYNC PRE-CHECKS AND DATA FETCHING (permissions are async)
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

  // SYNCHRONOUS TRANSACTION
  return dbHelpers.transaction("drive", driveId, (tx: Database) => {
    // Call the synchronous helper within the transaction
    return moveFolderTransaction_SYNC(
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
  userId: UserID
): Promise<RestoreTrashResponse> {
  const isFile = payload.id.startsWith(IDPrefixEnum.File);
  const tableName = isFile ? "files" : "folders";

  // --- ASYNC PRE-TRANSACTION CHECKS AND DATA FETCHING ---
  const resource: any = (
    await db.queryDrive(
      driveId,
      `SELECT * FROM ${tableName} WHERE id = ? AND deleted = 1`,
      [payload.id]
    )
  )[0];

  if (!resource) {
    throw new Error("Resource not found in trash.");
  }

  if (resource.restore_trash_prior_folder_uuid === null) {
    throw new Error(`${isFile ? "File" : "Folder"} is not in trash.`);
  }

  let targetDestinationFolder: FolderRecord | null = null;
  let finalDestinationFolderId: FolderID;
  let createdNewPath = false; // Flag to check if we created a new path

  if (payload.restore_to_folder_path) {
    const translation = await internals.translatePathToId(
      driveId,
      payload.restore_to_folder_path
    );
    if (translation.folder) {
      targetDestinationFolder = translation.folder;
    } else {
      // If path doesn't exist, create the folder structure (ASYNC ensureFolderStructure)
      const disk = await get_disk_from_db(driveId, resource.disk_id);
      if (!disk) throw new Error("Disk not found for resource.");

      const createdFolderId = await internals.ensureFolderStructure(
        // This uses its own transaction
        driveId,
        payload.restore_to_folder_path,
        resource.disk_id,
        userId,
        false
      );
      targetDestinationFolder = (
        await db.queryDrive(driveId, "SELECT * FROM folders WHERE id = ?", [
          createdFolderId,
        ])
      )[0] as FolderRecord;
      createdNewPath = true; // Mark that a new path was created
    }
  } else {
    // Restore to original location
    targetDestinationFolder = (
      await db.queryDrive(driveId, "SELECT * FROM folders WHERE id = ?", [
        resource.restore_trash_prior_folder_uuid,
      ])
    )[0] as FolderRecord;

    if (!targetDestinationFolder) {
      const disk = await get_disk_from_db(driveId, resource.disk_id);
      if (!disk) throw new Error("Disk not found.");
      targetDestinationFolder = (
        await db.queryDrive(driveId, "SELECT * FROM folders WHERE id = ?", [
          disk.root_folder,
        ])
      )[0] as FolderRecord;
      if (!targetDestinationFolder)
        throw new Error("Root folder not found for disk.");
    }
  }

  if (!targetDestinationFolder) {
    throw new Error("Failed to determine restore destination.");
  }

  if (targetDestinationFolder.deleted) {
    // Use .deleted directly as it's a boolean from query result
    throw new Error(
      `Cannot restore to a folder that is in trash. Please first restore ${targetDestinationFolder.full_directory_path}.`
    );
  }

  finalDestinationFolderId = targetDestinationFolder.id;

  const targetFolderResourceId: DirectoryResourceID =
    `${finalDestinationFolderId}` as DirectoryResourceID;

  const hasPermissionToRestore = (
    await checkDirectoryPermissions(targetFolderResourceId, userId, driveId)
  ).includes(DirectoryPermissionType.UPLOAD);

  const isOwner = (await getDriveOwnerId(driveId)) === userId;

  if (!isOwner && !hasPermissionToRestore) {
    throw new Error(
      `Permission denied: User ${userId} cannot restore resource ${payload.id} to folder ${finalDestinationFolderId}.`
    );
  }

  // --- SYNCHRONOUS TRANSACTION ---
  const response = await dbHelpers.transaction(
    "drive",
    driveId,
    (tx: Database) => {
      let restoredFileId: FileID | undefined;
      let restoredFolderId: FolderID | undefined;
      let restoredResource: FileRecord | FolderRecord;

      // Inside the transaction, we must use the synchronous versions of moveFile/moveFolderTransaction
      if (isFile) {
        // Direct call to synchronous logic of moveFile
        // We need to re-fetch the file within the transaction context for consistency if needed,
        // but the `file` object from outside is fine for its ID/initial properties.
        const fileFromTx = tx
          .prepare("SELECT * FROM files WHERE id = ?")
          .get(payload.id) as FileRecord;
        if (!fileFromTx)
          throw new Error("File not found for restore within transaction.");

        const [finalName, finalPath] = internals.resolveNamingConflict_SYNC(
          tx,
          driveId,
          targetDestinationFolder.full_directory_path,
          fileFromTx.name,
          false,
          payload.file_conflict_resolution ||
            FileConflictResolutionEnum.KEEP_BOTH
        );

        if (!finalName) {
          // This case means KEEP_ORIGINAL was chosen and a conflict exists, so return original file.
          // For restore, if conflict, it implies we don't move it. Rust returns it in old state.
          restoredResource = fileFromTx;
        } else {
          tx.prepare(
            "UPDATE files SET name = ?, full_directory_path = ?, parent_folder_id = ?, last_updated_date_ms = ?, last_updated_by = ?, deleted = 0, restore_trash_prior_folder_uuid = NULL WHERE id = ?"
          ).run(
            finalName,
            finalPath,
            finalDestinationFolderId,
            Date.now(),
            userId,
            payload.id
          );

          const permissionsToUpdate = tx
            .prepare(
              `SELECT id FROM permissions_directory WHERE resource_type = 'File' AND resource_id = ?`
            )
            .all(payload.id) as { id: string }[];
          for (const perm of permissionsToUpdate) {
            tx.prepare(
              `UPDATE permissions_directory SET resource_path = ? WHERE id = ?`
            ).run(finalPath, perm.id);
          }
          restoredResource = tx
            .prepare("SELECT * FROM files WHERE id = ?")
            .get(payload.id) as FileRecord;
        }
        restoredFileId = restoredResource.id as FileID;
      } else {
        // Call the synchronous move folder logic, passing the current transaction
        // moveFolderTransaction_SYNC needs to be updated to be truly synchronous with its permission checks
        // as it's now internal to a transaction and shouldn't call async db.queryDrive
        restoredResource = moveFolderTransaction_SYNC(
          tx,
          driveId,
          userId,
          payload.id as FolderID,
          finalDestinationFolderId,
          payload.file_conflict_resolution ||
            FileConflictResolutionEnum.KEEP_BOTH
        );

        // After move, explicitly clear deleted and restore_trash_prior_folder_uuid for the moved folder and its contents
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
        restoredFolderId = restoredResource.id as FolderID;
      }

      return {
        restored_folders: restoredFolderId ? [restoredFolderId] : [],
        restored_files: restoredFileId ? [restoredFileId] : [],
      };
    }
  );
  return response;
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
    file_uuids: (row.file_uuids as FileID[]) || [],
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

export async function requestFileOverwritePresignedUrl(
  fileRecord: FileRecord,
  driveId: DriveID
) {
  const disk = await get_disk_from_db(driveId, fileRecord.disk_id);
  if (!disk) throw new Error("Disk not found.");

  let uploadResponse: DiskUploadResponse = { url: "", fields: {} };

  if (disk.disk_type === DiskTypeEnum.AwsBucket) {
    const awsAuth = JSON.parse(disk.auth_json || "");
    const result = await generate_s3_upload_url(
      fileRecord.id,
      fileRecord.extension,
      awsAuth,
      driveId,
      BigInt(fileRecord.file_size),
      BigInt(24 * 60 * 60),
      fileRecord.disk_id,
      fileRecord.name
    );
    if (result.ok) uploadResponse = result.ok;
    else throw new Error(result.err);
  } else if (disk.disk_type === DiskTypeEnum.StorjWeb3) {
    const storjAuth = JSON.parse(disk.auth_json || "");
    const result = await generate_s3_upload_url(
      fileRecord.id,
      fileRecord.extension,
      storjAuth,
      driveId,
      BigInt(fileRecord.file_size),
      BigInt(24 * 60 * 60),
      fileRecord.disk_id,
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

  return uploadResponse;
}
