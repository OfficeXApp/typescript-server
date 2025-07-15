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
function extractPlainDirectoryResourceId(id: DirectoryResourceID): string {
  if (id.startsWith(IDPrefixEnum.File)) {
    return id.substring(IDPrefixEnum.File.length);
  }
  if (id.startsWith(IDPrefixEnum.Folder)) {
    return id.substring(IDPrefixEnum.Folder.length);
  }
  return id; // Should not happen if types are strictly enforced
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
  let granteeIdPart = row.grantee_id; // This is the ID without prefix from DB

  switch (row.grantee_type) {
    case "Public":
      grantedTo = PUBLIC_GRANTEE_ID_STRING;
      break;
    case "User":
      grantedTo = `${IDPrefixEnum.User}${granteeIdPart}` as UserID;
      break;
    case "Group":
      grantedTo = `${IDPrefixEnum.Group}${granteeIdPart}` as GroupID;
      break;
    case "Placeholder":
      grantedTo =
        `${IDPrefixEnum.PlaceholderPermissionGrantee}${granteeIdPart}` as `PlaceholderPermissionGranteeID_${string}`;
      break;
    default:
      console.warn(
        `Unknown grantee_type: ${row.grantee_type}. Defaulting to Public.`
      );
      grantedTo = PUBLIC_GRANTEE_ID_STRING;
      break;
  }

  // Reconstruct resource_id with its correct prefix from the DB's resource_type and resource_id
  let resourceIdWithPrefix: DirectoryResourceID;
  if (row.resource_type === "File") {
    resourceIdWithPrefix =
      `${IDPrefixEnum.File}${row.resource_id}` as DirectoryResourceID;
  } else if (row.resource_type === "Folder") {
    resourceIdWithPrefix =
      `${IDPrefixEnum.Folder}${row.resource_id}` as DirectoryResourceID;
  } else {
    throw new Error(`Unknown resource_type from DB: ${row.resource_type}`);
  }

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
    granted_by: `${IDPrefixEnum.User}${row.granted_by_user_id}` as UserID,
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
    labels: [], // Labels explicitly skipped as per your request
    external_id: undefined, // External ID explicitly skipped as per your request
    external_payload: undefined, // External Payload explicitly skipped as per your request
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
  const plainResourceId = extractPlainDirectoryResourceId(
    permission.resource_id
  );

  if (permission.resource_id.startsWith(IDPrefixEnum.File)) {
    const fileMetadata = await getFileMetadata(
      orgId,
      plainResourceId as FileID
    );
    resourceName = fileMetadata?.name;
  } else if (permission.resource_id.startsWith(IDPrefixEnum.Folder)) {
    const folderMetadata = await getFolderMetadata(
      orgId,
      plainResourceId as FolderID
    );
    resourceName = folderMetadata?.name;
  }

  // Get grantee_name and grantee_avatar
  let granteeName: string | undefined;
  let granteeAvatar: string | undefined;
  if (permission.granted_to === PUBLIC_GRANTEE_ID_STRING) {
    granteeName = "PUBLIC";
  } else if (permission.granted_to.startsWith(IDPrefixEnum.User)) {
    const plainContactId = permission.granted_to as UserID;
    const contactRows = await db.queryDrive(
      orgId,
      "SELECT name, avatar FROM contacts WHERE id = ?",
      [plainContactId]
    );
    if (contactRows.length > 0) {
      granteeName = contactRows[0].name;
      granteeAvatar = contactRows[0].avatar;
    } else {
      granteeName = `User: ${permission.granted_to}`; // Fallback if contact not found
    }
  } else if (permission.granted_to.startsWith(IDPrefixEnum.Group)) {
    const plainGroupId = permission.granted_to as GroupID;
    const groupRows = await db.queryDrive(
      orgId,
      "SELECT name, avatar FROM groups WHERE id = ?",
      [plainGroupId]
    );
    if (groupRows.length > 0) {
      granteeName = groupRows[0].name;
      granteeAvatar = groupRows[0].avatar;
    } else {
      granteeName = `Group: ${permission.granted_to}`; // Fallback if group not found
    }
  } else if (
    permission.granted_to.startsWith(IDPrefixEnum.PlaceholderPermissionGrantee)
  ) {
    granteeName = "Awaiting Anon";
  }

  // Get granter_name
  const plainGranterId = permission.granted_by;
  const granterRows = await db.queryDrive(
    orgId,
    "SELECT name FROM contacts WHERE id = ?",
    [plainGranterId]
  );
  const granterName =
    granterRows.length > 0
      ? granterRows[0].name
      : `Granter: ${permission.granted_by}`;

  // Get permission previews for the current user on this permission record
  const recordPermissions = await checkSystemPermissions(
    // The Rust SystemRecordIDEnum::Permission(self.id.to_string()) means the SystemPermissionID itself (UUID part).
    // But SystemPermissionID is also prefixed. So this should be the full SystemPermissionID.
    `${IDPrefixEnum.SystemPermission}${permission.id}` as SystemResourceID,
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
    granted_to: permission.granted_to,
    granted_by: permission.granted_by,
    permission_types: permission.permission_types,
    begin_date_ms: permission.begin_date_ms,
    expiry_date_ms: permission.expiry_date_ms,
    inheritable: permission.inheritable,
    note: permission.note,
    created_at: permission.created_at,
    last_modified_at: permission.last_modified_at,
    from_placeholder_grantee: permission.from_placeholder_grantee,
    labels: permission.labels,
    redeem_code: permission.redeem_code,
    metadata: permission.metadata,
    external_id: permission.external_id,
    external_payload: permission.external_payload,
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

  const directoryPermissionLabelsRaw = await db.queryDrive(
    orgId,
    `SELECT T2.value FROM permission_directory_labels AS T1 JOIN labels AS T2 ON T1.label_id = T2.id WHERE T1.permission_id = ?`,
    [permissionFe.id]
  );
  redacted.labels = (
    await Promise.all(
      directoryPermissionLabelsRaw.map((row: any) =>
        redactLabelValue(orgId, row.value, userId)
      )
    )
  ).filter((label: any): label is LabelValue => label !== null);

  redacted.external_id = undefined; // Clear external_id as per skipping request
  redacted.external_payload = undefined; // Clear external_payload as per skipping request

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
  // The Rust logic for `can_user_access_directory_permission` checks granted_by, granted_to, or is_owner.
  // By using `checkDirectoryPermissions`, which aggregates all valid permissions for the user on the resource,
  // we effectively cover all these cases, assuming `checkDirectoryPermissions` is comprehensive.
  // The original Rust `can_user_access_directory_permission` was about *accessing the permission record itself*,
  // which implies "can you see/edit this rule?". If this new function is about "can you access the *resource* based on permissions?",
  // then checking if `permissions.length > 0` or if it contains `VIEW` is appropriate.
  // Given the context of "permission fixes" and "permissions logic", it makes sense to check if *any* permission is granted.
  return permissions.length > 0; // If any permission is found, access is granted.
}

// Function to get the list of inherited resources (parents in the directory hierarchy)
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

    resources.push(
      `${IDPrefixEnum.Folder}${folderMetadata.id}` as DirectoryResourceID
    );

    if (folderMetadata.has_sovereign_permissions) {
      break;
    }
    currentFolderId = folderMetadata.parent_folder_uuid;
  }

  return resources.reverse();
}

// Checks permissions directly applied to a single directory resource for a specific grantee.
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
    dbResourceId = resourceId.substring(IDPrefixEnum.File.length);
    dbResourceType = "File";
  } else if (resourceId.startsWith(IDPrefixEnum.Folder)) {
    dbResourceId = resourceId.substring(IDPrefixEnum.Folder.length);
    dbResourceType = "Folder";
  } else {
    throw new Error(`Invalid DirectoryResourceID format: ${resourceId}`);
  }

  // SQL query updated to exclude labels, external_id, external_payload based on your request
  const rows = await db.queryDrive(
    orgId,
    `SELECT
      pd.id,
      pd.resource_type,
      pd.resource_id,
      pd.resource_path,
      pd.grantee_type,
      pd.grantee_id,
      pd.granted_by_user_id,
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
  const isOwner =
    (await getDriveOwnerId(orgId)) ===
    (granteeId.startsWith(IDPrefixEnum.User)
      ? (granteeId as UserID)
      : undefined);

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
    dbResourceId = resourceId.substring(IDPrefixEnum.File.length);
    dbResourceType = "File";
  } else if (resourceId.startsWith(IDPrefixEnum.Folder)) {
    dbResourceId = resourceId.substring(IDPrefixEnum.Folder.length);
    dbResourceType = "Folder";
  } else {
    throw new Error(`Invalid DirectoryResourceID format: ${resourceId}`);
  }

  // SQL query updated to exclude labels, external_id, external_payload based on your request
  const rows = await db.queryDrive(
    orgId,
    `SELECT
      pd.id,
      pd.resource_id,
      pd.resource_path,
      pd.grantee_type,
      pd.grantee_id,
      pd.granted_by_user_id,
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
    dbResourceId = resourceId.substring(IDPrefixEnum.File.length);
    dbResourceType = "File";
  } else if (resourceId.startsWith(IDPrefixEnum.Folder)) {
    dbResourceId = resourceId.substring(IDPrefixEnum.Folder.length);
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
    // This part should also use IDPrefixEnum for consistency
    let permissionGrantedTo: GranteeID;
    const granteeIdPart = row.grantee_id;
    switch (row.grantee_type) {
      case "Public":
        permissionGrantedTo = PUBLIC_GRANTEE_ID_STRING;
        break;
      case "User":
        permissionGrantedTo = `${IDPrefixEnum.User}${granteeIdPart}` as UserID;
        break;
      case "Group":
        permissionGrantedTo =
          `${IDPrefixEnum.Group}${granteeIdPart}` as GroupID;
        break;
      case "Placeholder":
        permissionGrantedTo =
          `${IDPrefixEnum.PlaceholderPermissionGrantee}${granteeIdPart}` as `PlaceholderPermissionGranteeID_${string}`;
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
    let originalResourceIdString: DirectoryResourceID = currentResourceId;

    const plainResourceId = extractPlainDirectoryResourceId(currentResourceId);

    if (currentResourceId.startsWith(IDPrefixEnum.File)) {
      const fileMetadata = await getFileMetadata(
        orgId,
        plainResourceId as FileID
      );
      if (!fileMetadata) break;

      resourceName = fileMetadata.name;
      parentFolderId = fileMetadata.parent_folder_uuid;
      hasSovereignPermissions = fileMetadata.has_sovereign_permissions;
    } else if (currentResourceId.startsWith(IDPrefixEnum.Folder)) {
      const folderMetadata = await getFolderMetadata(
        orgId,
        plainResourceId as FolderID
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
          [folderMetadata.disk_id]
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
      ? (`${IDPrefixEnum.Folder}${parentFolderId}` as DirectoryResourceID)
      : undefined;
  }

  return tempBreadcrumbs.reverse();
}
