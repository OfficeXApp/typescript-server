// src/services/permissions/directory.ts

import {
  DirectoryPermission,
  DirectoryPermissionFE,
  DirectoryPermissionType,
  IDPrefixEnum,
  DirectoryResourceID,
  UserID,
  GranteeID, // This type needs to be updated to reflect the string format
  FolderID,
  FileID,
  SystemPermissionType, // Used for permission_previews
  SystemResourceID as RustLikeSystemResourceID, // Renamed to avoid conflict with local SystemResourceID in parseSystemResourceId
  PermissionMetadata,
  PermissionMetadataTypeEnum,
  PermissionMetadataContent,
  DriveClippedFilePath,
  LabelValue,
  GroupID,
  FilePathBreadcrumb,
  SystemTableValueEnum,
  BreadcrumbVisibilityPreviewEnum,
} from "@officexapp/types";
import { db } from "../../services/database";
import { isUserInGroup } from "../groups";
import { getFolderMetadata, getFileMetadata } from "../directory";
import { checkSystemPermissions } from "./system";
import { redactLabel } from "../../services/labels";
import { getDriveOwnerId } from "../../routes/v1/types";

// Constants
export const PUBLIC_GRANTEE_ID_STRING = "PUBLIC";
export const USER_ID_PREFIX = "USER_";
export const GROUP_ID_PREFIX = "GROUP_";
export const PLACEHOLDER_DIRECTORY_PERMISSION_GRANTEE_ID_PREFIX =
  "PLACEHOLDER_DIRECTORY_PERMISSION_GRANTEE_";

// Helper to parse DirectoryResourceID from string
function parseDirectoryResourceIDString(
  idStr: string
): DirectoryResourceID | undefined {
  if (idStr.startsWith("FILE_") && idStr.length > "FILE_".length) {
    return idStr.substring("FILE_".length) as FileID; // Assuming FileID is just the UUID part
  } else if (idStr.startsWith("FOLDER_") && idStr.length > "FOLDER_".length) {
    return idStr.substring("FOLDER_".length) as FolderID; // Assuming FolderID is just the UUID part
  }
  return undefined;
}

// Helper to parse PermissionGranteeID from string
export function parsePermissionGranteeIDString(idStr: string): GranteeID {
  // Direct match for PUBLIC
  if (idStr === PUBLIC_GRANTEE_ID_STRING) {
    return PUBLIC_GRANTEE_ID_STRING;
  }
  // Check for prefixes
  if (idStr.startsWith(USER_ID_PREFIX)) {
    return idStr as UserID; // The type is just the string itself now.
  }
  if (idStr.startsWith(GROUP_ID_PREFIX)) {
    return idStr as GroupID; // The type is just the string itself now.
  }
  if (idStr.startsWith(PLACEHOLDER_DIRECTORY_PERMISSION_GRANTEE_ID_PREFIX)) {
    return idStr as `PlaceholderPermissionGranteeID_${string}`; // Cast directly as it's the full string
  }
  // Fallback for invalid formats if necessary, or throw an error
  throw new Error(`Invalid GranteeID format: ${idStr}`);
}

// Utility to convert raw DB row to DirectoryPermission
function mapDbRowToDirectoryPermission(row: any): DirectoryPermission {
  let grantedTo: GranteeID;
  switch (row.grantee_type) {
    case "Public":
      grantedTo = PUBLIC_GRANTEE_ID_STRING;
      break;
    case "User":
      grantedTo = `${USER_ID_PREFIX}${row.grantee_id}`;
      break;
    case "Group":
      grantedTo = `${GROUP_ID_PREFIX}${row.grantee_id}`;
      break;
    case "Placeholder":
      grantedTo = `${PLACEHOLDER_DIRECTORY_PERMISSION_GRANTEE_ID_PREFIX}${row.grantee_id}`;
      break;
    default:
      // Handle unexpected grantee_type, maybe default to public or throw error
      console.warn(
        `Unknown grantee_type: ${row.grantee_type}. Defaulting to Public.`
      );
      grantedTo = PUBLIC_GRANTEE_ID_STRING;
      break;
  }

  // Reconstruct metadata if present
  let metadata: PermissionMetadata | undefined;
  if (row.metadata_type && row.metadata_content) {
    let content: PermissionMetadataContent;
    switch (row.metadata_type) {
      case PermissionMetadataTypeEnum.LABELS:
        content = { Labels: row.metadata_content }; // Labels content is plain string
        break;
      case PermissionMetadataTypeEnum.DIRECTORY_PASSWORD:
        content = { DirectoryPassword: row.metadata_content }; // DirectoryPassword content is plain string
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
    resource_id: row.resource_id, // This should already be in the format 'FILE_...' or 'FOLDER_...' from DB
    resource_path: row.resource_path,
    granted_to: grantedTo,
    granted_by: row.granted_by_user_id,
    permission_types: (row.permission_types_list || "")
      .split(",")
      .filter(Boolean) // Filter out empty strings from split
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
    labels: (row.labels_list || "").split(",").filter(Boolean),
    external_id: row.external_id,
    external_payload: row.external_payload,
  };
}

// Utility to convert DirectoryPermission to DirectoryPermissionFE
async function castToDirectoryPermissionFE(
  permission: DirectoryPermission,
  currentUserId: UserID,
  orgId: string
): Promise<DirectoryPermissionFE> {
  const isOwner = (await getDriveOwnerId(orgId)) === currentUserId;

  // Get resource_name
  let resourceName: string | undefined;
  const rawResourceId =
    permission.resource_id.split("_")[1] || permission.resource_id; // Extract UUID part
  if (permission.resource_id.startsWith("FILE_")) {
    const fileMetadata = await getFileMetadata(orgId, rawResourceId as FileID);
    resourceName = fileMetadata?.name;
  } else if (permission.resource_id.startsWith("FOLDER_")) {
    const folderMetadata = await getFolderMetadata(
      orgId,
      rawResourceId as FolderID
    );
    resourceName = folderMetadata?.name;
  }

  // Get grantee_name and grantee_avatar
  let granteeName: string | undefined;
  let granteeAvatar: string | undefined;
  if (permission.granted_to === PUBLIC_GRANTEE_ID_STRING) {
    granteeName = "PUBLIC";
  } else if (permission.granted_to.startsWith(USER_ID_PREFIX)) {
    const contactId = permission.granted_to as UserID;
    const contactRows = await db.queryDrive(
      orgId,
      "SELECT name, avatar FROM contacts WHERE id = ?",
      [contactId]
    );
    if (contactRows.length > 0) {
      granteeName = contactRows[0].name;
      granteeAvatar = contactRows[0].avatar;
    } else {
      granteeName = `User: ${contactId}`; // Fallback if contact not found
    }
  } else if (permission.granted_to.startsWith(GROUP_ID_PREFIX)) {
    const groupId = permission.granted_to as GroupID;
    const groupRows = await db.queryDrive(
      orgId,
      "SELECT name, avatar FROM groups WHERE id = ?",
      [groupId]
    );
    if (groupRows.length > 0) {
      granteeName = groupRows[0].name;
      granteeAvatar = groupRows[0].avatar;
    } else {
      granteeName = `Group: ${groupId}`; // Fallback if group not found
    }
  } else if (
    permission.granted_to.startsWith(
      PLACEHOLDER_DIRECTORY_PERMISSION_GRANTEE_ID_PREFIX
    )
  ) {
    granteeName = "Awaiting Anon";
  }

  // Get granter_name
  const granterRows = await db.queryDrive(
    orgId,
    "SELECT name FROM contacts WHERE id = ?",
    [permission.granted_by]
  );
  const granterName =
    granterRows.length > 0
      ? granterRows[0].name
      : `Granter: ${permission.granted_by}`;

  // Get permission previews for the current user on this permission record
  const recordPermissions = await checkSystemPermissions(
    // Construct SystemResourceID correctly
    `RECORD_PERMISSION_${permission.id}` as RustLikeSystemResourceID,
    currentUserId,
    orgId
  );
  const tablePermissions = await checkSystemPermissions(
    // Construct SystemResourceID correctly
    `TABLE_${SystemTableValueEnum.PERMISSIONS}` as RustLikeSystemResourceID,
    currentUserId,
    orgId
  );
  const permissionPreviews = Array.from(
    new Set([...recordPermissions, ...tablePermissions])
  );

  // Clip resource_path
  const fullPath = permission.resource_path;
  let clippedPath: DriveClippedFilePath;
  const pathParts = fullPath.split("::/"); // Split disk_id::path/to/file.txt
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
    clippedPath = fullPath as DriveClippedFilePath; // Should not happen if paths are well-formed
  }

  const castedPermission: DirectoryPermissionFE = {
    id: permission.id,
    resource_id: permission.resource_id,
    resource_path: clippedPath,
    granted_to: permission.granted_to, // Keep as the full string representation
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

  return redactDirectoryPermissionFE(castedPermission, currentUserId, isOwner);
}

// Function to redact DirectoryPermissionFE
function redactDirectoryPermissionFE(
  permissionFe: DirectoryPermissionFE,
  userId: UserID,
  isOwner: boolean
): DirectoryPermissionFE {
  const redacted = { ...permissionFe };

  const hasEditPermissions = redacted.permission_previews.includes(
    SystemPermissionType.EDIT
  );

  if (!isOwner) {
    // Redact sensitive fields if not owner
    // Rust only checks for `is_owner`, not `hasEditPermissions` for `resource_path` redaction.
    redacted.resource_path = "" as DriveClippedFilePath;

    // The rust code doesn't explicitly redact `metadata` or `redeem_code` based on permissions,
    // but the `DirectoryPermission::cast_fe` method itself populates them.
    // If you need to redact them further based on `hasEditPermissions`, this is where to do it.
    // For now, mirroring the Rust's `redacted` method which primarily uses `redact_label`.
  }

  // Filter labels based on user permissions
  redacted.labels = redacted.labels
    .map((label) => redactLabel(label, userId)) // Assuming redactLabel returns null for redacted labels
    .filter((label): label is LabelValue => label !== null);

  return redacted;
}

// Function to check if a user can access a directory permission record
async function canUserAccessDirectoryPermission(
  requesterUserId: UserID,
  permission: DirectoryPermission,
  isOwner: boolean,
  orgId: string
): Promise<boolean> {
  // System owner can access all permissions
  if (isOwner) {
    return true;
  }

  // User who granted the permission can access it
  if (permission.granted_by === requesterUserId) {
    return true;
  }

  // Parse the `granted_to` string from the permission object
  const permissionGrantedTo = permission.granted_to;

  // Check if user is the direct grantee
  if (permissionGrantedTo === PUBLIC_GRANTEE_ID_STRING) {
    return true; // Everyone can see public permissions
  } else if (permissionGrantedTo.startsWith(USER_ID_PREFIX)) {
    if (permissionGrantedTo === requesterUserId) {
      return true;
    }
  } else if (permissionGrantedTo.startsWith(GROUP_ID_PREFIX)) {
    if (
      await isUserInGroup(
        requesterUserId,
        permissionGrantedTo as GroupID,
        orgId
      )
    ) {
      return true;
    }
  } else if (
    permissionGrantedTo.startsWith(
      PLACEHOLDER_DIRECTORY_PERMISSION_GRANTEE_ID_PREFIX
    )
  ) {
    // One-time links can only be accessed by the creator
    return permission.granted_by === requesterUserId;
  }

  return false;
}

// Function to get the list of inherited resources (parents in the directory hierarchy)
async function getInheritedResourcesList(
  resourceId: DirectoryResourceID,
  orgId: string
): Promise<DirectoryResourceID[]> {
  const resources: DirectoryResourceID[] = [];
  let currentFolderId: FolderID | undefined;

  // Add the initial resource itself (Rust adds it first, then reverses)
  resources.push(resourceId);

  // Determine the starting point for traversal based on resource type
  // DirectoryResourceID is either FileID*... or FolderID*...
  const actualResourceId = resourceId.split("*")[1] || resourceId; // Extract UUID part
  if (resourceId.startsWith(IDPrefixEnum.File)) {
    const fileMetadata = await getFileMetadata(
      orgId,
      actualResourceId as FileID
    );
    if (!fileMetadata) return []; // File not found

    if (fileMetadata.has_sovereign_permissions) {
      return resources; // If file has sovereign permissions, only itself is relevant
    }
    currentFolderId = fileMetadata.parent_folder_uuid;
  } else if (resourceId.startsWith(IDPrefixEnum.Folder)) {
    const folderMetadata = await getFolderMetadata(
      orgId,
      actualResourceId as FolderID
    );
    if (!folderMetadata) return []; // Folder not found

    if (folderMetadata.has_sovereign_permissions) {
      return resources; // If folder has sovereign permissions, only itself is relevant
    }
    currentFolderId = folderMetadata.parent_folder_uuid;
  } else {
    // Invalid resource ID format, though type guards should prevent this normally
    return [];
  }

  // Traverse up through parent folders
  while (currentFolderId) {
    const folderMetadata = await getFolderMetadata(orgId, currentFolderId);
    if (!folderMetadata) break; // Parent folder not found, stop traversal

    resources.push(
      `${IDPrefixEnum.Folder}${folderMetadata.id}` as DirectoryResourceID
    ); // Add with prefix

    if (folderMetadata.has_sovereign_permissions) {
      break; // Stop if a folder with sovereign permissions is encountered
    }
    currentFolderId = folderMetadata.parent_folder_uuid;
  }

  return resources.reverse(); // Return in order from root to the resource
}

// Checks permissions directly applied to a single directory resource for a specific grantee.
async function checkDirectoryResourcePermissions(
  resourceId: DirectoryResourceID,
  granteeId: GranteeID,
  isParentForInheritance: boolean, // Corresponds to `is_parent_for_inheritance`
  orgId: string
): Promise<DirectoryPermissionType[]> {
  const permissionsSet = new Set<DirectoryPermissionType>();
  const currentTime = Date.now();

  // The Rust code's `check_directory_resource_permissions` directly takes
  // `DirectoryResourceID` and `PermissionGranteeID` as references.
  // It fetches permissions from `DIRECTORY_PERMISSIONS_BY_RESOURCE_HASHTABLE`
  // and `DIRECTORY_PERMISSIONS_BY_ID_HASHTABLE`.

  // The SQL query to fetch permissions for a specific resource ID
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
        pd.metadata_content,
        pd.external_id,
        pd.external_payload
      FROM permissions_directory pd
      JOIN permissions_directory_types pdt ON pd.id = pdt.permission_id
      WHERE pd.resource_id = ?
      GROUP BY pd.id`,
    [resourceId]
  );

  for (const row of rows) {
    // Reconstruct DirectoryPermission object from the row
    const permission: DirectoryPermission = {
      id: row.id,
      resource_id: row.resource_id,
      resource_path: row.resource_path,
      // Reconstruct granted_to string
      granted_to:
        row.grantee_type === "Public"
          ? PUBLIC_GRANTEE_ID_STRING
          : `${row.grantee_type.toUpperCase()}_${row.grantee_id}`,
      granted_by: row.granted_by_user_id,
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
      metadata:
        row.metadata_type && row.metadata_content
          ? {
              metadata_type: row.metadata_type,
              content:
                row.metadata_type === PermissionMetadataTypeEnum.LABELS
                  ? { Labels: row.metadata_content }
                  : { DirectoryPassword: row.metadata_content },
            }
          : undefined,
      labels: [], // Labels are handled via a separate join table in SQL, not directly in this query's row
      external_id: row.external_id,
      external_payload: row.external_payload,
    };

    // Rust's `permission_granted_to` is `PermissionGranteeID` enum, which is then matched.
    // In TS, `permission.granted_to` is already a string like "PUBLIC", "USER_...", "GROUP_...", etc.
    const permissionGrantedTo = permission.granted_to;

    // Check if permission is expired or not yet active
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

    // Skip if permission lacks inheritance and is for a parent resource (Rust's `!permission.inheritable && is_parent_for_inheritance`)
    if (!permission.inheritable && isParentForInheritance) {
      continue;
    }

    let applies = false;

    // Directly compare the string representations
    if (permissionGrantedTo === PUBLIC_GRANTEE_ID_STRING) {
      applies = true;
    } else if (
      granteeId.startsWith(USER_ID_PREFIX) &&
      permissionGrantedTo.startsWith(USER_ID_PREFIX)
    ) {
      applies = granteeId === permissionGrantedTo;
    } else if (
      granteeId.startsWith(GROUP_ID_PREFIX) &&
      permissionGrantedTo.startsWith(GROUP_ID_PREFIX)
    ) {
      applies = granteeId === permissionGrantedTo;
    } else if (
      granteeId.startsWith(USER_ID_PREFIX) &&
      permissionGrantedTo.startsWith(GROUP_ID_PREFIX)
    ) {
      // Check if the user is a member of the group
      applies = await isUserInGroup(
        granteeId as UserID,
        permissionGrantedTo as GroupID,
        orgId
      );
    } else if (
      granteeId.startsWith(
        PLACEHOLDER_DIRECTORY_PERMISSION_GRANTEE_ID_PREFIX
      ) &&
      permissionGrantedTo.startsWith(
        PLACEHOLDER_DIRECTORY_PERMISSION_GRANTEE_ID_PREFIX
      )
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
    (granteeId.startsWith(USER_ID_PREFIX) ? granteeId : "");

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

  // Build the list of resources to check by traversing up the hierarchy
  const resourcesToCheck = await getInheritedResourcesList(resourceId, orgId);

  // Check permissions for each resource and combine them
  const allPermissions = new Set<DirectoryPermissionType>();
  for (const resource of resourcesToCheck) {
    // Rust's `check_directory_resource_permissions` has `resource != resource_id` for `is_parent_for_inheritance`.
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
  // Rust uses DirectoryPermissionType::Invite for has_directory_manage_permission
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
        pd.metadata_content,
        pd.external_id,
        pd.external_payload
      FROM permissions_directory pd
      JOIN permissions_directory_types pdt ON pd.id = pdt.permission_id
      WHERE pd.resource_id = ?
      GROUP BY pd.id`,
    [resourceId]
  );

  const currentTime = Date.now();

  for (const row of rows) {
    const permission: DirectoryPermission = {
      id: row.id,
      resource_id: row.resource_id,
      resource_path: row.resource_path,
      granted_to:
        row.grantee_type === "Public"
          ? PUBLIC_GRANTEE_ID_STRING
          : `${row.grantee_type.toUpperCase()}_${row.grantee_id}`,
      granted_by: row.granted_by_user_id,
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
      metadata:
        row.metadata_type && row.metadata_content
          ? {
              metadata_type: row.metadata_type,
              content:
                row.metadata_type === PermissionMetadataTypeEnum.LABELS
                  ? { Labels: row.metadata_content }
                  : { DirectoryPassword: row.metadata_content },
            }
          : undefined,
      labels: [], // Not fetched by this query
      external_id: row.external_id,
      external_payload: row.external_payload,
    };

    // Check if permission is currently valid (within timeframe)
    const isActive =
      (permission.begin_date_ms <= 0 ||
        permission.begin_date_ms <= currentTime) &&
      (permission.expiry_date_ms < 0 ||
        permission.expiry_date_ms > currentTime);

    if (!isActive) {
      continue; // Skip expired or not-yet-active permissions
    }

    const permissionGrantedTo = permission.granted_to;

    let applies = false;
    if (permissionGrantedTo === PUBLIC_GRANTEE_ID_STRING) {
      applies = true;
    } else if (permissionGrantedTo.startsWith(USER_ID_PREFIX)) {
      applies = permissionGrantedTo === userId;
    } else if (permissionGrantedTo.startsWith(GROUP_ID_PREFIX)) {
      applies = await isUserInGroup(
        userId,
        permissionGrantedTo as GroupID,
        orgId
      );
    }
    // No specific handling for PlaceholderDirectoryPermissionGrantee in preview,
    // as it's typically for one-time redemption.

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
      WHERE pd.resource_id = ?
      GROUP BY pd.id`,
    [resourceId]
  );

  for (const row of rows) {
    // Reconstruct minimal permission object for checks
    const permissionGrantedTo =
      row.grantee_type === "Public"
        ? PUBLIC_GRANTEE_ID_STRING
        : `${row.grantee_type.toUpperCase()}_${row.grantee_id}`;
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
      // Any other grantee type (User, Group, Placeholder) makes it "private"
      if (hasView) privateCanView = true;
      if (hasModify) privateCanModify = true;
    }
  }

  const results: string[] = [];
  // Prioritize modify over view, and public over private
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

  // Use a temporary array to build breadcrumbs in reverse, then unshift them
  const tempBreadcrumbs: FilePathBreadcrumb[] = [];

  while (currentResourceId) {
    let resourceName: string | undefined;
    let parentFolderId: FolderID | undefined;
    let hasSovereignPermissions = false;
    let originalResourceIdString: DirectoryResourceID; // Store the original string to pass to deriveBreadcrumbVisibilityPreviews

    const rawResourceId = currentResourceId.split("_")[1] || currentResourceId;

    if (currentResourceId.startsWith(IDPrefixEnum.File)) {
      const fileMetadata = await getFileMetadata(
        orgId,
        rawResourceId as FileID
      );
      if (!fileMetadata) break;

      resourceName = fileMetadata.name;
      parentFolderId = fileMetadata.parent_folder_uuid;
      hasSovereignPermissions = fileMetadata.has_sovereign_permissions;
      originalResourceIdString = currentResourceId; // Already in correct format
    } else if (currentResourceId.startsWith(IDPrefixEnum.Folder)) {
      const folderMetadata = await getFolderMetadata(
        orgId,
        rawResourceId as FolderID
      );
      if (!folderMetadata) break;

      resourceName = folderMetadata.name;
      parentFolderId = folderMetadata.parent_folder_uuid;
      hasSovereignPermissions = folderMetadata.has_sovereign_permissions;
      originalResourceIdString = currentResourceId; // Already in correct format

      // Special handling for root folder (disk itself)
      // Rust's condition: `folder_metadata.full_directory_path == DriveFullFilePath(format!("{}::/", folder_metadata.disk_id.to_string()))`
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
          resourceName = `Disk: ${folderMetadata.disk_id}`; // Fallback if disk not found
        }
      }
    } else {
      break; // Invalid resource ID
    }

    // Check if user has permission to view this resource
    const permissions = await checkDirectoryPermissions(
      currentResourceId,
      userId,
      orgId
    );
    if (!permissions.includes(DirectoryPermissionType.VIEW) && !isOwner) {
      break; // User doesn't have permission to view this resource or its ancestors
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
      break; // Stop if sovereign permissions are encountered
    }

    // Move up to the parent folder
    currentResourceId = parentFolderId
      ? (`${IDPrefixEnum.Folder}${parentFolderId}` as DirectoryResourceID)
      : undefined;
  }

  // Reverse the temporary array to get the correct order
  return tempBreadcrumbs.reverse();
}
