// src/services/permissions/directory.ts

import {
  DirectoryPermission,
  DirectoryPermissionFE,
  DirectoryPermissionType,
  IDPrefixEnum,
  DirectoryResourceID,
  UserID,
  GranteeID,
  FolderID,
  FileID,
  SystemPermissionType,
  SystemResourceID, // Corrected import
  PermissionMetadata,
  PermissionMetadataTypeEnum,
  PermissionMetadataContent,
  DriveClippedFilePath,
  LabelValue,
  GroupID,
  FilePathBreadcrumb,
  SystemTableValueEnum,
  BreadcrumbVisibilityPreviewEnum,
  DriveID,
} from "@officexapp/types";
import { db } from "../../services/database";
import { isUserInGroup } from "../groups"; // Import helpers from groups service
import { getFolderMetadata, getFileMetadata } from "../directory/drive";
import { checkSystemPermissions, redactLabelValue } from "./system";

import { getDriveOwnerId } from "../../routes/v1/types";

// Constants
export const PUBLIC_GRANTEE_ID_STRING = "PUBLIC";

// Helper to extract the UUID part from a DirectoryResourceID
// NO CHANGE NEEDED, AS PER YOUR CLARIFICATION, IDPrefixEnum.File and IDPrefixEnum.Folder
// are assumed to be present in the `id` field from the database.
function extractPlainDirectoryResourceId(id: DirectoryResourceID): string {
  // If the ID is already a plain UUID string from the DB (e.g., from `resource_id` column
  // if you changed the schema to store plain UUIDs), then this would trim the prefix.
  // Given your clarification that `id`s are already prefixed, this function might be redundant
  // or needs to be used carefully. For now, assuming `resource_id` in DB is the full prefixed ID.
  return id;
}

// Helper to parse DirectoryResourceID from string
export function parseDirectoryResourceIDString(
  idStr: string
): DirectoryResourceID | undefined {
  if (idStr.startsWith(IDPrefixEnum.File)) {
    return idStr as DirectoryResourceID;
  } else if (idStr.startsWith(IDPrefixEnum.Folder)) {
    return idStr as DirectoryResourceID;
  }
  return undefined;
}

// Helper to parse PermissionGranteeID from string
export function parsePermissionGranteeIDString(idStr: string): GranteeID {
  if (idStr === PUBLIC_GRANTEE_ID_STRING) {
    return PUBLIC_GRANTEE_ID_STRING;
  }
  if (idStr.startsWith(IDPrefixEnum.User)) {
    return idStr as UserID;
  }
  if (idStr.startsWith(IDPrefixEnum.Group)) {
    return idStr as GroupID;
  }
  if (idStr.startsWith(IDPrefixEnum.PlaceholderPermissionGrantee)) {
    return idStr as `PlaceholderPermissionGranteeID_${string}`;
  }
  throw new Error(`Invalid GranteeID format: ${idStr}`);
}

// Utility to convert raw DB row to DirectoryPermission
export function mapDbRowToDirectoryPermission(row: any): DirectoryPermission {
  let grantedTo: GranteeID;
  // Assuming row.grantee_id already contains the full prefixed ID or "PUBLIC"
  const granteeIdString = row.grantee_id;

  switch (row.grantee_type) {
    case "Public":
      grantedTo = PUBLIC_GRANTEE_ID_STRING;
      break;
    case "User":
      grantedTo = granteeIdString as UserID;
      break;
    case "Group":
      grantedTo = granteeIdString as GroupID;
      break;
    case "Placeholder":
      grantedTo = granteeIdString as `PlaceholderPermissionGranteeID_${string}`;
      break;
    default:
      console.warn(
        `Unknown grantee_type: ${row.grantee_type}. Defaulting to Public.`
      );
      grantedTo = PUBLIC_GRANTEE_ID_STRING;
      break;
  }

  // resource_id from DB should already be the full prefixed ID
  const resourceIdWithPrefix: DirectoryResourceID =
    row.resource_id as DirectoryResourceID;

  // Reconstruct metadata if present
  let metadata: PermissionMetadata | undefined;
  if (
    row.metadata_type &&
    row.metadata_content !== null &&
    row.metadata_content !== undefined
  ) {
    let content: PermissionMetadataContent;
    switch (row.metadata_type) {
      case PermissionMetadataTypeEnum.LABELS:
        content = { Labels: row.metadata_content };
        break;
      case PermissionMetadataTypeEnum.DIRECTORY_PASSWORD:
        content = { DirectoryPassword: row.metadata_content };
        break;
      default:
        console.warn(
          `Unknown metadata_type: ${row.metadata_type}. Skipping metadata.`
        );
        content = { Labels: "" }; // Fallback to a default or handle error
        break;
    }
    metadata = {
      metadata_type: row.metadata_type as PermissionMetadataTypeEnum,
      content: content,
    };
  }

  return {
    id: row.id,
    resource_id: resourceIdWithPrefix,
    resource_path: row.resource_path,
    granted_to: grantedTo,
    granted_by: row.granted_by as UserID, // Already prefixed
    permission_types: (row.permission_types_list || "")
      .split(",")
      .filter(Boolean)
      .map((typeStr: string) => typeStr.trim() as DirectoryPermissionType),
    begin_date_ms: row.begin_date_ms,
    expiry_date_ms: row.expiry_date_ms,
    inheritable: row.inheritable === 1,
    note: row.note,
    created_at: row.created_at,
    last_modified_at: row.last_modified_at,
    redeem_code: row.redeem_code,
    from_placeholder_grantee: row.from_placeholder_grantee,
    metadata: metadata,
    labels: [], // Labels explicitly ignored and blank array
    external_id: undefined, // External ID explicitly ignored
    external_payload: undefined, // External Payload explicitly ignored
  };
}

// Utility to convert DirectoryPermission to DirectoryPermissionFE
export async function castToDirectoryPermissionFE(
  permission: DirectoryPermission,
  currentUserId: UserID,
  orgId: string
): Promise<DirectoryPermissionFE> {
  const isOwner = (await getDriveOwnerId(orgId)) === currentUserId;

  // Get resource_name
  let resourceName: string | undefined;
  // Use permission.resource_id directly as it's already prefixed
  const resourceId = permission.resource_id;

  if (resourceId.startsWith(IDPrefixEnum.File)) {
    const fileMetadata = await getFileMetadata(orgId, resourceId as FileID);
    resourceName = fileMetadata?.name;
  } else if (resourceId.startsWith(IDPrefixEnum.Folder)) {
    const folderMetadata = await getFolderMetadata(
      orgId,
      resourceId as FolderID
    );
    resourceName = folderMetadata?.name;
  }

  // Get grantee_name and grantee_avatar
  let granteeName: string | undefined;
  let granteeAvatar: string | undefined;
  if (permission.granted_to === PUBLIC_GRANTEE_ID_STRING) {
    granteeName = "PUBLIC";
  } else if (permission.granted_to.startsWith(IDPrefixEnum.User)) {
    const userId = permission.granted_to as UserID;
    const contactRows = await db.queryDrive(
      orgId,
      "SELECT name, avatar FROM contacts WHERE id = ?",
      [userId] // Use prefixed ID directly
    );
    if (contactRows.length > 0) {
      granteeName = contactRows[0].name;
      granteeAvatar = contactRows[0].avatar;
    } else {
      granteeName = `Unknown User (${userId})`; // Fallback if contact not found
    }
  } else if (permission.granted_to.startsWith(IDPrefixEnum.Group)) {
    const groupId = permission.granted_to as GroupID;
    const groupRows = await db.queryDrive(
      orgId,
      "SELECT name, avatar FROM groups WHERE id = ?",
      [groupId] // Use prefixed ID directly
    );
    if (groupRows.length > 0) {
      granteeName = groupRows[0].name;
      granteeAvatar = groupRows[0].avatar;
    } else {
      granteeName = `Unknown Group (${groupId})`; // Fallback if group not found
    }
  } else if (
    permission.granted_to.startsWith(IDPrefixEnum.PlaceholderPermissionGrantee)
  ) {
    granteeName = "Awaiting Anon";
  }

  // Get granter_name
  const granterId = permission.granted_by;
  const granterRows = await db.queryDrive(
    orgId,
    "SELECT name FROM contacts WHERE id = ?",
    [granterId] // Use prefixed ID directly
  );
  const granterName =
    granterRows.length > 0
      ? granterRows[0].name
      : `Granter: ${permission.granted_by}`;

  // Get permission previews for the current user on this permission record
  const recordPermissions = await checkSystemPermissions(
    permission.id as SystemResourceID, // SystemPermissionID is a SystemResourceID
    currentUserId,
    orgId
  );
  const tablePermissions = await checkSystemPermissions(
    `TABLE_${SystemTableValueEnum.PERMISSIONS}` as SystemResourceID,
    currentUserId,
    orgId
  );
  const permissionPreviews = Array.from(
    new Set([...recordPermissions, ...tablePermissions])
  );

  // Clip resource_path
  const fullPath = permission.resource_path;
  let clippedPath: DriveClippedFilePath;
  const pathParts = fullPath.split("::/");
  if (pathParts.length > 1) {
    const diskId = pathParts[0];
    const filePath = pathParts[1];
    const fileFolderParts = filePath.split("/");
    if (fileFolderParts.length > 1) {
      clippedPath =
        `${diskId}::../${fileFolderParts[fileFolderParts.length - 1]}` as DriveClippedFilePath;
    } else {
      clippedPath = `${diskId}::${filePath}` as DriveClippedFilePath;
    }
  } else {
    clippedPath = fullPath as DriveClippedFilePath;
  }

  const castedPermission: DirectoryPermissionFE = {
    id: permission.id,
    resource_id: permission.resource_id,
    resource_path: clippedPath,
    granted_to: permission.granted_to, // Keep as GranteeID (string union)
    granted_by: permission.granted_by,
    permission_types: permission.permission_types,
    begin_date_ms: permission.begin_date_ms,
    expiry_date_ms: permission.expiry_date_ms,
    inheritable: permission.inheritable,
    note: permission.note,
    created_at: permission.created_at,
    last_modified_at: permission.last_modified_at,
    from_placeholder_grantee: permission.from_placeholder_grantee,
    labels: [], // Explicitly empty as requested
    redeem_code: permission.redeem_code,
    metadata: permission.metadata,
    external_id: undefined, // Explicitly undefined as requested
    external_payload: undefined, // Explicitly undefined as requested
    resource_name: resourceName,
    grantee_name: granteeName,
    grantee_avatar: granteeAvatar,
    granter_name: granterName,
    permission_previews: permissionPreviews,
  };

  return redactDirectoryPermissionFE(
    castedPermission,
    currentUserId,
    isOwner,
    orgId
  );
}

// Function to redact DirectoryPermissionFE
export async function redactDirectoryPermissionFE(
  permissionFe: DirectoryPermissionFE,
  userId: UserID,
  isOwner: boolean,
  orgId: DriveID
): Promise<DirectoryPermissionFE> {
  const redacted = { ...permissionFe };

  if (!isOwner) {
    redacted.resource_path = "" as DriveClippedFilePath;
  }

  // Labels are already set to empty array in mapDbRowToDirectoryPermission
  // and are explicitly ignored per request.
  redacted.labels = [];

  redacted.external_id = undefined;
  redacted.external_payload = undefined;

  return redacted;
}

/**
 * Checks if a user has any permission (e.g., View, Edit, Manage) on a specific directory resource.
 * This function handles inheritance.
 * @param resourceId The ID of the DirectoryResource (File or Folder) to check.
 * @param requesterUserId The ID of the user attempting to access the resource.
 * @param orgId The ID of the organization/drive.
 * @returns A Promise that resolves to true if the user has any permission, false otherwise.
 */
export async function canUserAccessDirectoryPermission(
  resourceId: DirectoryResourceID, // Corrected to take DirectoryResourceID
  requesterUserId: UserID,
  orgId: string
): Promise<boolean> {
  const isOwner = (await getDriveOwnerId(orgId)) === requesterUserId;

  if (isOwner) {
    return true;
  }

  // Leverage the existing checkDirectoryPermissions function to get all applicable permissions
  const permissions = await checkDirectoryPermissions(
    resourceId,
    requesterUserId, // Pass the user as the grantee
    orgId
  );

  // If the user has any permission type (i.e., the array is not empty), return true.
  return permissions.length > 0; // If any permission is found, access is granted.
}

export async function checkDirectoryResourcePermissions(
  resourceId: DirectoryResourceID,
  granteeId: GranteeID,
  isParentForInheritance: boolean,
  orgId: string
): Promise<DirectoryPermissionType[]> {
  const permissionsSet = new Set<DirectoryPermissionType>();
  const currentTime = Date.now();

  let dbResourceId: string;
  let dbResourceType: "File" | "Folder";
  if (resourceId.startsWith(IDPrefixEnum.File)) {
    dbResourceId = resourceId; // Use the full prefixed ID
    dbResourceType = "File";
  } else if (resourceId.startsWith(IDPrefixEnum.Folder)) {
    dbResourceId = resourceId; // Use the full prefixed ID
    dbResourceType = "Folder";
  } else {
    throw new Error(`Invalid DirectoryResourceID format: ${resourceId}`);
  }

  // SQL query: filter by resource and join with types table
  // Assuming 'id' column in permissions_directory and 'grantee_id' store full prefixed IDs.
  const rows = await db.queryDrive(
    orgId,
    `SELECT
      pd.id,
      pd.resource_type,
      pd.resource_id,
      pd.resource_path,
      pd.grantee_type,
      pd.grantee_id,
      pd.granted_by,
      GROUP_CONCAT(pdt.permission_type) AS permission_types_list,
      pd.begin_date_ms,
      pd.expiry_date_ms,
      pd.inheritable,
      pd.note,
      pd.created_at,
      pd.last_modified_at,
      pd.redeem_code,
      pd.from_placeholder_grantee,
      pd.metadata_type,
      pd.metadata_content
      FROM permissions_directory pd
      JOIN permissions_directory_types pdt ON pd.id = pdt.permission_id
      WHERE pd.resource_type = ? AND pd.resource_id = ?
      GROUP BY pd.id`,
    [dbResourceType, dbResourceId]
  );

  for (const row of rows) {
    const permission: DirectoryPermission = mapDbRowToDirectoryPermission(row);

    if (
      permission.expiry_date_ms > 0 &&
      permission.expiry_date_ms <= currentTime
    ) {
      continue;
    }
    if (
      permission.begin_date_ms > 0 &&
      permission.begin_date_ms > currentTime
    ) {
      continue;
    }

    if (!permission.inheritable && isParentForInheritance) {
      continue;
    }

    let applies = false;
    const permissionGrantedTo = permission.granted_to;

    if (permissionGrantedTo === PUBLIC_GRANTEE_ID_STRING) {
      applies = true;
    } else if (
      granteeId.startsWith(IDPrefixEnum.User) &&
      permissionGrantedTo.startsWith(IDPrefixEnum.User)
    ) {
      applies = granteeId === permissionGrantedTo;
    } else if (
      granteeId.startsWith(IDPrefixEnum.Group) &&
      permissionGrantedTo.startsWith(IDPrefixEnum.Group)
    ) {
      applies = granteeId === permissionGrantedTo;
    } else if (
      granteeId.startsWith(IDPrefixEnum.User) &&
      permissionGrantedTo.startsWith(IDPrefixEnum.Group)
    ) {
      applies = await isUserInGroup(
        granteeId as UserID,
        permissionGrantedTo as GroupID,
        orgId
      );
    } else if (
      granteeId.startsWith(IDPrefixEnum.PlaceholderPermissionGrantee) &&
      permissionGrantedTo.startsWith(IDPrefixEnum.PlaceholderPermissionGrantee)
    ) {
      applies = granteeId === permissionGrantedTo;
    }

    if (applies) {
      permission.permission_types.forEach((type) => permissionsSet.add(type));
    }
  }

  return Array.from(permissionsSet);
}

// Function to check what kind of permission a specific user has on a specific resource
export async function checkDirectoryPermissions(
  resourceId: DirectoryResourceID,
  granteeId: GranteeID,
  orgId: string
): Promise<DirectoryPermissionType[]> {
  const isOwner = (await getDriveOwnerId(orgId)) === granteeId;

  if (isOwner) {
    return [
      DirectoryPermissionType.VIEW,
      DirectoryPermissionType.EDIT,
      DirectoryPermissionType.UPLOAD,
      DirectoryPermissionType.DELETE,
      DirectoryPermissionType.INVITE,
      DirectoryPermissionType.MANAGE,
    ];
  }

  const resourcesToCheck = await getInheritedResourcesList(resourceId, orgId);

  const allPermissions = new Set<DirectoryPermissionType>();
  for (const resource of resourcesToCheck) {
    const isParent = resource !== resourceId;
    const resourcePermissions = await checkDirectoryResourcePermissions(
      resource,
      granteeId,
      isParent,
      orgId
    );
    resourcePermissions.forEach((p) => allPermissions.add(p));
  }

  return Array.from(allPermissions);
}

// Check if a user has manage permission on a directory resource
export async function hasDirectoryManagePermission(
  userId: UserID,
  resourceId: DirectoryResourceID,
  orgId: string
): Promise<boolean> {
  const permissions = await checkDirectoryPermissions(
    resourceId,
    userId,
    orgId
  );
  return permissions.includes(DirectoryPermissionType.INVITE);
}

// Add a helper function to get permissions for a resource
export async function previewDirectoryPermissions(
  resourceId: DirectoryResourceID,
  userId: UserID,
  orgId: string
): Promise<Array<{ permission_id: string; grant_type: string }>> {
  const resourcePermissions: Array<{
    permission_id: string;
    grant_type: string;
  }> = [];

  let dbResourceId: string;
  let dbResourceType: "File" | "Folder";
  if (resourceId.startsWith(IDPrefixEnum.File)) {
    dbResourceId = resourceId;
    dbResourceType = "File";
  } else if (resourceId.startsWith(IDPrefixEnum.Folder)) {
    dbResourceId = resourceId;
    dbResourceType = "Folder";
  } else {
    throw new Error(`Invalid DirectoryResourceID format: ${resourceId}`);
  }

  // SQL query: filter by resource and join with types table
  const rows = await db.queryDrive(
    orgId,
    `SELECT
      pd.id,
      pd.resource_id,
      pd.resource_path,
      pd.grantee_type,
      pd.grantee_id,
      pd.granted_by,
      GROUP_CONCAT(pdt.permission_type) AS permission_types_list,
      pd.begin_date_ms,
      pd.expiry_date_ms,
      pd.inheritable,
      pd.note,
      pd.created_at,
      pd.last_modified_at,
      pd.redeem_code,
      pd.from_placeholder_grantee,
      pd.metadata_type,
      pd.metadata_content
    FROM permissions_directory pd
    JOIN permissions_directory_types pdt ON pd.id = pdt.permission_id
    WHERE pd.resource_type = ? AND pd.resource_id = ?
    GROUP BY pd.id`,
    [dbResourceType, dbResourceId]
  );

  const currentTime = Date.now();

  for (const row of rows) {
    const permission: DirectoryPermission = mapDbRowToDirectoryPermission(row);

    const isActive =
      (permission.begin_date_ms <= 0 ||
        permission.begin_date_ms <= currentTime) &&
      (permission.expiry_date_ms < 0 ||
        permission.expiry_date_ms > currentTime);

    if (!isActive) {
      continue;
    }

    const permissionGrantedTo = permission.granted_to;

    let applies = false;
    if (permissionGrantedTo === PUBLIC_GRANTEE_ID_STRING) {
      applies = true;
    } else if (permissionGrantedTo.startsWith(IDPrefixEnum.User)) {
      applies = permissionGrantedTo === userId;
    } else if (permissionGrantedTo.startsWith(IDPrefixEnum.Group)) {
      applies = await isUserInGroup(
        userId,
        permissionGrantedTo as GroupID,
        orgId
      );
    }

    if (applies) {
      for (const grantType of permission.permission_types) {
        resourcePermissions.push({
          permission_id: permission.id,
          grant_type: grantType,
        });
      }
    }
  }

  return resourcePermissions;
}

export async function deriveBreadcrumbVisibilityPreviews(
  resourceId: DirectoryResourceID,
  orgId: string
): Promise<BreadcrumbVisibilityPreviewEnum[]> {
  let publicCanView = false;
  let publicCanModify = false;
  let privateCanView = false;
  let privateCanModify = false;

  const currentTimeMs = Date.now();

  let dbResourceId: string;
  let dbResourceType: "File" | "Folder";
  if (resourceId.startsWith(IDPrefixEnum.File)) {
    dbResourceId = resourceId;
    dbResourceType = "File";
  } else if (resourceId.startsWith(IDPrefixEnum.Folder)) {
    dbResourceId = resourceId;
    dbResourceType = "Folder";
  } else {
    throw new Error(`Invalid DirectoryResourceID format: ${resourceId}`);
  }

  const rows = await db.queryDrive(
    orgId,
    `SELECT
      pd.id,
      pd.resource_id,
      pd.resource_path,
      pd.grantee_type,
      pd.grantee_id,
      GROUP_CONCAT(pdt.permission_type) AS permission_types_list,
      pd.begin_date_ms,
      pd.expiry_date_ms
    FROM permissions_directory pd
    JOIN permissions_directory_types pdt ON pd.id = pdt.permission_id
    WHERE pd.resource_type = ? AND pd.resource_id = ?
    GROUP BY pd.id`,
    [dbResourceType, dbResourceId]
  );

  for (const row of rows) {
    // Reconstruct minimal permission object for checks
    let permissionGrantedTo: GranteeID;
    const granteeIdString = row.grantee_id; // Assuming full prefixed ID
    switch (row.grantee_type) {
      case "Public":
        permissionGrantedTo = PUBLIC_GRANTEE_ID_STRING;
        break;
      case "User":
        permissionGrantedTo = granteeIdString as UserID;
        break;
      case "Group":
        permissionGrantedTo = granteeIdString as GroupID;
        break;
      case "Placeholder":
        permissionGrantedTo = granteeIdString as GranteeID;
        break;
      default:
        permissionGrantedTo = PUBLIC_GRANTEE_ID_STRING; // Fallback
        break;
    }

    const permissionTypes = (row.permission_types_list || "")
      .split(",")
      .filter(Boolean)
      .map((typeStr: string) => typeStr.trim() as DirectoryPermissionType);

    const isActive =
      (row.begin_date_ms <= 0 || row.begin_date_ms <= currentTimeMs) &&
      (row.expiry_date_ms < 0 || row.expiry_date_ms > currentTimeMs);

    if (!isActive) {
      continue;
    }

    const hasView = permissionTypes.includes(DirectoryPermissionType.VIEW);
    const hasModify =
      permissionTypes.includes(DirectoryPermissionType.UPLOAD) ||
      permissionTypes.includes(DirectoryPermissionType.EDIT) ||
      permissionTypes.includes(DirectoryPermissionType.DELETE) ||
      permissionTypes.includes(DirectoryPermissionType.MANAGE);

    if (permissionGrantedTo === PUBLIC_GRANTEE_ID_STRING) {
      if (hasView) publicCanView = true;
      if (hasModify) publicCanModify = true;
    } else {
      if (hasView) privateCanView = true;
      if (hasModify) privateCanModify = true;
    }
  }

  const results: BreadcrumbVisibilityPreviewEnum[] = [];
  if (publicCanModify) {
    results.push(BreadcrumbVisibilityPreviewEnum.PUBLIC_MODIFY);
  } else if (publicCanView) {
    results.push(BreadcrumbVisibilityPreviewEnum.PUBLIC_VIEW);
  }

  if (privateCanModify) {
    results.push(BreadcrumbVisibilityPreviewEnum.PRIVATE_MODIFY);
  } else if (privateCanView) {
    results.push(BreadcrumbVisibilityPreviewEnum.PRIVATE_VIEW);
  }

  return results;
}

export async function deriveDirectoryBreadcrumbs(
  resourceId: DirectoryResourceID,
  userId: UserID,
  orgId: string
): Promise<FilePathBreadcrumb[]> {
  const breadcrumbs: FilePathBreadcrumb[] = [];
  const isOwner = (await getDriveOwnerId(orgId)) === userId;
  let currentResourceId: DirectoryResourceID | undefined = resourceId;

  const tempBreadcrumbs: FilePathBreadcrumb[] = [];

  while (currentResourceId) {
    let resourceName: string | undefined;
    let parentFolderId: FolderID | undefined;
    let hasSovereignPermissions = false;
    let originalResourceIdString: DirectoryResourceID = currentResourceId; // Use currentResourceId directly

    if (currentResourceId.startsWith(IDPrefixEnum.File)) {
      const fileMetadata = await getFileMetadata(
        orgId,
        currentResourceId as FileID // Use full prefixed ID
      );
      if (!fileMetadata) break;

      resourceName = fileMetadata.name;
      parentFolderId = fileMetadata.parent_folder_uuid;
      hasSovereignPermissions = fileMetadata.has_sovereign_permissions;
    } else if (currentResourceId.startsWith(IDPrefixEnum.Folder)) {
      const folderMetadata = await getFolderMetadata(
        orgId,
        currentResourceId as FolderID // Use full prefixed ID
      );
      if (!folderMetadata) break;

      resourceName = folderMetadata.name;
      parentFolderId = folderMetadata.parent_folder_uuid;
      hasSovereignPermissions = folderMetadata.has_sovereign_permissions;

      if (
        folderMetadata.full_directory_path === `${folderMetadata.disk_id}::/`
      ) {
        const diskRows = await db.queryDrive(
          orgId,
          "SELECT name FROM disks WHERE id = ?",
          [folderMetadata.disk_id] // Use full prefixed ID
        );
        if (diskRows.length > 0) {
          resourceName = diskRows[0].name;
        } else {
          resourceName = `Disk: ${folderMetadata.disk_id}`;
        }
      }
    } else {
      break;
    }

    const permissions = await checkDirectoryPermissions(
      currentResourceId,
      userId,
      orgId
    );
    if (!permissions.includes(DirectoryPermissionType.VIEW) && !isOwner) {
      break;
    }

    tempBreadcrumbs.push({
      resource_id: originalResourceIdString,
      resource_name: resourceName || "Unknown",
      visibility_preview: await deriveBreadcrumbVisibilityPreviews(
        originalResourceIdString,
        orgId
      ),
    });

    if (hasSovereignPermissions) {
      break;
    }

    currentResourceId = parentFolderId
      ? (parentFolderId as DirectoryResourceID) // parent_folder_uuid is already FolderID type
      : undefined;
  }

  return tempBreadcrumbs.reverse();
}

export async function getInheritedResourcesList(
  resourceId: DirectoryResourceID,
  orgId: string
): Promise<DirectoryResourceID[]> {
  const resources: DirectoryResourceID[] = [];
  let currentFolderId: FolderID | undefined;

  const plainResourceId = extractPlainDirectoryResourceId(resourceId);

  if (resourceId.startsWith(IDPrefixEnum.File)) {
    const fileMetadata = await getFileMetadata(
      orgId,
      plainResourceId as FileID
    );
    if (!fileMetadata) return [];

    resources.push(resourceId);

    if (fileMetadata.has_sovereign_permissions) {
      return resources;
    }
    currentFolderId = fileMetadata.parent_folder_uuid;
  } else if (resourceId.startsWith(IDPrefixEnum.Folder)) {
    const folderMetadata = await getFolderMetadata(
      orgId,
      plainResourceId as FolderID
    );
    if (!folderMetadata) return [];

    resources.push(resourceId);

    if (folderMetadata.has_sovereign_permissions) {
      return resources;
    }
    currentFolderId = folderMetadata.parent_folder_uuid;
  } else {
    return [];
  }

  while (currentFolderId) {
    const folderMetadata = await getFolderMetadata(orgId, currentFolderId);
    if (!folderMetadata) break;

    resources.push(`${folderMetadata.id}` as DirectoryResourceID);

    if (folderMetadata.has_sovereign_permissions) {
      break;
    }
    currentFolderId = folderMetadata.parent_folder_uuid;
  }

  return resources.reverse();
}
