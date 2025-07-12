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
  SystemTableValueEnum,
  BreadcrumbVisibilityPreviewEnum,
  DriveID,
  SortDirection,
  PermissionMetadata,
  PermissionMetadataTypeEnum,
  PermissionMetadataContent,
  DriveClippedFilePath,
  LabelValue,
  GroupID,
  FilePathBreadcrumb,
  IRequestGetDirectoryPermission,
  IResponseGetDirectoryPermission,
  IRequestListDirectoryPermissions,
  IResponseListDirectoryPermissions,
  IRequestCreateDirectoryPermission,
  IResponseCreateDirectoryPermission,
  IRequestUpdateDirectoryPermission,
  IResponseUpdateDirectoryPermission,
  IRequestDeleteDirectoryPermission,
  IResponseDeleteDirectoryPermission,
  IRequestCheckDirectoryPermissions,
  IResponseCheckDirectoryPermissions,
  IRequestRedeemDirectoryPermission,
  IResponseRedeemDirectoryPermission,
  IRequestGetSystemPermission,
  IResponseGetSystemPermission,
  IRequestListSystemPermissions,
  IResponseListSystemPermissions,
  IRequestCreateSystemPermission,
  IResponseCreateSystemPermission,
  IRequestUpdateSystemPermission,
  IResponseUpdateSystemPermission,
  IRequestDeleteSystemPermission,
  IResponseDeleteSystemPermission,
  IRequestCheckSystemPermissions,
  IResponseCheckSystemPermissions,
  IRequestRedeemSystemPermission,
  IResponseRedeemSystemPermission,
  SystemPermission,
  SystemPermissionID,
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import { v4 as uuidv4 } from "uuid";
import { FastifyRequest, FastifyReply } from "fastify";

// Constants for ID prefixes to match Rust enum variants in string format
export const PUBLIC_GRANTEE_ID_STRING = "PUBLIC";
export const USER_ID_PREFIX = "UserID*";
export const GROUP_ID_PREFIX = "GroupID*";
export const PLACEHOLDER_DIRECTORY_PERMISSION_GRANTEE_ID_PREFIX =
  "PlaceholderDirectoryPermissionGranteeID*";

// --- Helper Functions for Internal Mapping ---

/**
 * Parses a GranteeID string (e.g., "USER_abc", "GROUP_xyz", "PUBLIC")
 * into its type and raw ID part for database storage.
 * @param idStr The GranteeID string.
 * @returns { type: string, id: string | null } - The grantee_type and grantee_id for DB.
 */
function parseGranteeIDForDb(idStr: GranteeID): {
  type: string;
  id: string | null;
} {
  if (idStr === PUBLIC_GRANTEE_ID_STRING) {
    return { type: "Public", id: null };
  }
  if (idStr.startsWith(USER_ID_PREFIX)) {
    return { type: "User", id: idStr.substring(USER_ID_PREFIX.length) };
  }
  if (idStr.startsWith(GROUP_ID_PREFIX)) {
    return { type: "Group", id: idStr.substring(GROUP_ID_PREFIX.length) };
  }
  if (idStr.startsWith(PLACEHOLDER_DIRECTORY_PERMISSION_GRANTEE_ID_PREFIX)) {
    return {
      type: "Placeholder",
      id: idStr.substring(
        PLACEHOLDER_DIRECTORY_PERMISSION_GRANTEE_ID_PREFIX.length
      ),
    };
  }
  throw new Error(`Invalid GranteeID format for DB: ${idStr}`);
}

/**
 * Reconstructs a GranteeID string from DB columns (type and id).
 * @param dbType The grantee_type from DB (e.g., 'User', 'Group', 'Public', 'Placeholder').
 * @param dbId The grantee_id from DB (UUID part or null for Public).
 * @returns The full GranteeID string.
 */
function reconstructGranteeIDFromDb(
  dbType: string,
  dbId: string | null
): GranteeID {
  switch (dbType) {
    case "Public":
      return PUBLIC_GRANTEE_ID_STRING;
    case "User":
      return `${USER_ID_PREFIX}${dbId}` as UserID;
    case "Group":
      return `${GROUP_ID_PREFIX}${dbId}` as GroupID;
    case "Placeholder":
      return `${PLACEHOLDER_DIRECTORY_PERMISSION_GRANTEE_ID_PREFIX}${dbId}` as GranteeID;
    default:
      throw new Error(`Unknown grantee_type from DB: ${dbType}`);
  }
}

/**
 * Parses a DirectoryResourceID string (e.g., "FileID_abc", "FolderID_xyz")
 * into its type and raw ID part for database storage.
 * @param idStr The DirectoryResourceID string.
 * @returns { type: string, id: string } - The resource_type and resource_id for DB.
 */
function parseDirectoryResourceIDForDb(idStr: DirectoryResourceID): {
  type: string;
  id: string;
} {
  if (idStr.startsWith(IDPrefixEnum.File)) {
    return { type: "File", id: idStr.substring(IDPrefixEnum.File.length) };
  }
  if (idStr.startsWith(IDPrefixEnum.Folder)) {
    return { type: "Folder", id: idStr.substring(IDPrefixEnum.Folder.length) };
  }
  throw new Error(`Invalid DirectoryResourceID format for DB: ${idStr}`);
}

/**
 * Reconstructs a DirectoryResourceID string from DB columns (type and id).
 * @param dbType The resource_type from DB (e.g., 'File', 'Folder').
 * @param dbId The resource_id from DB (UUID part).
 * @returns The full DirectoryResourceID string.
 */
function reconstructDirectoryResourceIDFromDb(
  dbType: string,
  dbId: string
): DirectoryResourceID {
  switch (dbType) {
    case "File":
      return `${IDPrefixEnum.File}${dbId}` as DirectoryResourceID;
    case "Folder":
      return `${IDPrefixEnum.Folder}${dbId}` as DirectoryResourceID;
    default:
      throw new Error(`Unknown resource_type from DB: ${dbType}`);
  }
}

/**
 * Utility to convert raw DB row to DirectoryPermission, including labels and permission types.
 * This is crucial for reconstructing the full object from flattened SQL data.
 * @param row A database row containing permission data.
 * @returns A fully constructed DirectoryPermission object.
 */
export function mapDbRowToDirectoryPermission(row: any): DirectoryPermission {
  const grantedTo = reconstructGranteeIDFromDb(
    row.grantee_type,
    row.grantee_id
  );
  const resourceId = reconstructDirectoryResourceIDFromDb(
    row.resource_type,
    row.resource_id
  );

  let metadata: PermissionMetadata | undefined;
  if (row.metadata_type && row.metadata_content) {
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
          `Unknown metadata_type from DB: ${row.metadata_type}. Skipping metadata.`
        );
        content = { Labels: "" }; // Fallback
        break;
    }
    metadata = {
      metadata_type: row.metadata_type as PermissionMetadataTypeEnum,
      content: content,
    };
  }

  const permissionTypes = (row.permission_types_list || "")
    .split(",")
    .filter(Boolean)
    .map((typeStr: string) => typeStr.trim() as DirectoryPermissionType);

  const labels = (row.labels_list || "")
    .split(",")
    .filter(Boolean)
    .map((labelId: string) => labelId.trim() as LabelValue);

  return {
    id: row.id,
    resource_id: resourceId,
    resource_path: row.resource_path,
    granted_to: grantedTo,
    granted_by: row.granted_by_user_id,
    permission_types: permissionTypes,
    begin_date_ms: row.begin_date_ms,
    expiry_date_ms: row.expiry_date_ms,
    inheritable: row.inheritable === 1,
    note: row.note,
    created_at: row.created_at,
    last_modified_at: row.last_modified_at,
    redeem_code: row.redeem_code,
    from_placeholder_grantee: row.from_placeholder_grantee,
    metadata: metadata,
    labels: labels,
    external_id: row.external_id,
    external_payload: row.external_payload,
  };
}

/**
 * Utility to convert raw DB row to SystemPermission, including labels and permission types.
 * @param row A database row containing system permission data.
 * @returns A fully constructed SystemPermission object.
 */
export function mapDbRowToSystemPermission(row: any): SystemPermission {
  const grantedTo = reconstructGranteeIDFromDb(
    row.grantee_type,
    row.grantee_id
  );

  let metadata: PermissionMetadata | undefined;
  if (row.metadata_type && row.metadata_content) {
    let content: PermissionMetadataContent;
    switch (row.metadata_type) {
      case PermissionMetadataTypeEnum.LABELS:
        content = { Labels: row.metadata_content };
        break;
      case PermissionMetadataTypeEnum.DIRECTORY_PASSWORD: // System permissions might not have this, but for type consistency
        content = { DirectoryPassword: row.metadata_content };
        break;
      default:
        console.warn(
          `Unknown metadata_type from DB: ${row.metadata_type}. Skipping metadata.`
        );
        content = { Labels: "" }; // Fallback
        break;
    }
    metadata = {
      metadata_type: row.metadata_type as PermissionMetadataTypeEnum,
      content: content,
    };
  }

  const permissionTypes = (row.permission_types_list || "")
    .split(",")
    .filter(Boolean)
    .map((typeStr: string) => typeStr.trim() as SystemPermissionType);

  const labels = (row.labels_list || "")
    .split(",")
    .filter(Boolean)
    .map((labelId: string) => labelId.trim() as LabelValue);

  return {
    id: row.id,
    resource_id: row.resource_identifier, // For system permissions, resource_id is resource_identifier
    granted_to: grantedTo,
    granted_by: row.granted_by_user_id,
    permission_types: permissionTypes,
    begin_date_ms: row.begin_date_ms,
    expiry_date_ms: row.expiry_date_ms,
    note: row.note,
    created_at: row.created_at,
    last_modified_at: row.last_modified_at,
    redeem_code: row.redeem_code,
    from_placeholder_grantee: row.from_placeholder_grantee,
    metadata: metadata,
    labels: labels,
    external_id: row.external_id,
    external_payload: row.external_payload,
  };
}

// --- Mocked/Simplified External Dependencies (as per request to bake into handler) ---

async function getDriveOwnerId(orgId: string): Promise<UserID> {
  const rows = await db.queryDrive(
    orgId,
    "SELECT owner_id FROM about_drive LIMIT 1"
  );
  return rows.length > 0 ? rows[0].owner_id : "owner_id_placeholder"; // Placeholder for owner ID
}

async function getFileMetadata(
  orgId: string,
  fileId: FileID
): Promise<any | null> {
  const rows = await db.queryDrive(
    orgId,
    "SELECT name, parent_folder_id, full_directory_path, has_sovereign_permissions FROM files WHERE id = ?",
    [fileId]
  );
  if (rows.length > 0) {
    return {
      name: rows[0].name,
      parent_folder_uuid: rows[0].parent_folder_id,
      full_directory_path: rows[0].full_directory_path,
      has_sovereign_permissions: rows[0].has_sovereign_permissions === 1,
    };
  }
  return null;
}

async function getFolderMetadata(
  orgId: string,
  folderId: FolderID
): Promise<any | null> {
  const rows = await db.queryDrive(
    orgId,
    "SELECT name, parent_folder_id, full_directory_path, has_sovereign_permissions, disk_id FROM folders WHERE id = ?",
    [folderId]
  );
  if (rows.length > 0) {
    return {
      id: folderId, // Include ID for reconstruction in getInheritedResourcesList
      name: rows[0].name,
      parent_folder_uuid: rows[0].parent_folder_id,
      full_directory_path: rows[0].full_directory_path,
      has_sovereign_permissions: rows[0].has_sovereign_permissions === 1,
      disk_id: rows[0].disk_id,
    };
  }
  return null;
}

async function isUserInGroup(
  userId: UserID,
  groupId: GroupID,
  orgId: string
): Promise<boolean> {
  const rows = await db.queryDrive(
    orgId,
    "SELECT 1 FROM contact_groups WHERE user_id = ? AND group_id = ?",
    [userId, groupId]
  );
  return rows.length > 0;
}

async function redactLabelValue(
  orgId: DriveID,
  labelValue: LabelValue,
  userId: UserID
): Promise<LabelValue | null> {
  // In a real system, this would check user permissions for the label itself.
  // For now, simply return the label value.
  return labelValue;
}

// Helper to get contact name and avatar
async function getContactInfo(
  orgId: string,
  contactId: UserID
): Promise<{ name?: string; avatar?: string } | null> {
  const rows = await db.queryDrive(
    orgId,
    "SELECT name, avatar FROM contacts WHERE id = ?",
    [contactId]
  );
  return rows.length > 0 ? rows[0] : null;
}

// Helper to get group name and avatar
async function getGroupInfo(
  orgId: string,
  groupId: GroupID
): Promise<{ name?: string; avatar?: string } | null> {
  const rows = await db.queryDrive(
    orgId,
    "SELECT name, avatar FROM groups WHERE id = ?",
    [groupId]
  );
  return rows.length > 0 ? rows[0] : null;
}

// --- Directory Permission Core Logic ---

/**
 * Checks if a user can access a directory permission record (view/manage its details).
 * This logic mirrors Rust's `can_user_access_directory_permission`.
 * @param requesterUserId The ID of the user trying to access the record.
 * @param permission The DirectoryPermission record itself.
 * @param isOwner Whether the requester is the drive owner.
 * @param orgId The drive ID.
 * @returns True if the user can access the record, false otherwise.
 */
export async function canUserAccessDirectoryPermission(
  requesterUserId: UserID,
  permission: DirectoryPermission,
  isOwner: boolean,
  orgId: string
): Promise<boolean> {
  if (isOwner) {
    return true;
  }

  if (permission.granted_by === requesterUserId) {
    return true;
  }

  const permissionGrantedTo = permission.granted_to;

  if (permissionGrantedTo === PUBLIC_GRANTEE_ID_STRING) {
    return true;
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
    return permission.granted_by === requesterUserId;
  }

  return false;
}

/**
 * Checks what kind of permission a specific user has on a specific directory resource.
 * This function effectively combines and evaluates all relevant directory permissions
 * (direct, inherited, group-based, public). It mirrors Rust's `check_directory_permissions`.
 * @param resourceId The ID of the directory resource.
 * @param granteeId The ID of the user or group (as a string GranteeID) to check permissions for.
 * @param orgId The drive ID.
 * @returns A list of unique DirectoryPermissionType enums that the grantee has.
 */
export async function checkDirectoryPermissions(
  resourceId: DirectoryResourceID,
  granteeId: GranteeID,
  orgId: string
): Promise<DirectoryPermissionType[]> {
  const allPermissions = new Set<DirectoryPermissionType>();

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

  const resourcesToCheck = await getInheritedResourcesList(resourceId, orgId);

  for (const resource of resourcesToCheck) {
    const isParentForInheritance = resource !== resourceId;
    const resourcePermissions = await checkDirectoryResourcePermissions(
      resource,
      granteeId,
      isParentForInheritance,
      orgId
    );
    resourcePermissions.forEach((p) => allPermissions.add(p));
  }

  return Array.from(allPermissions);
}

/**
 * Checks permissions directly applied to a single directory resource for a specific grantee.
 * This is an internal helper for `checkDirectoryPermissions` and `deriveBreadcrumbVisibilityPreviews`.
 * It mirrors Rust's `check_directory_resource_permissions`.
 * @param resourceId The ID of the directory resource (e.g., "FileID*...", "FolderID*...").
 * @param granteeId The ID of the user or group to check permissions for (e.g., "UserID*...", "GroupID*...", "PUBLIC").
 * @param isParentForInheritance Boolean indicating if this resource is a parent in an inheritance chain.
 * @param orgId The drive ID.
 * @returns A set of DirectoryPermissionType enums applicable to this resource and grantee.
 */
export async function checkDirectoryResourcePermissions(
  resourceId: DirectoryResourceID,
  granteeId: GranteeID,
  isParentForInheritance: boolean,
  orgId: string
): Promise<DirectoryPermissionType[]> {
  const permissionsSet = new Set<DirectoryPermissionType>();
  const currentTime = Date.now();

  const { type: resourceType, id: resourceUUID } =
    parseDirectoryResourceIDForDb(resourceId);

  const rows = await db.queryDrive(
    orgId,
    `SELECT
        pd.id, pd.resource_type, pd.resource_id, pd.resource_path,
        pd.grantee_type, pd.grantee_id, pd.granted_by_user_id,
        GROUP_CONCAT(pdt.permission_type) AS permission_types_list,
        pd.begin_date_ms, pd.expiry_date_ms, pd.inheritable, pd.note,
        pd.created_at, pd.last_modified_at, pd.redeem_code, pd.from_placeholder_grantee,
        pd.metadata_type, pd.metadata_content, pd.external_id, pd.external_payload,
        GROUP_CONCAT(pdl.label_id) AS labels_list
    FROM permissions_directory pd
    LEFT JOIN permissions_directory_types pdt ON pd.id = pdt.permission_id
    LEFT JOIN permission_directory_labels pdl ON pd.id = pdl.permission_id
    WHERE pd.resource_id = ? AND pd.resource_type = ?
    GROUP BY pd.id;`,
    [resourceUUID, resourceType]
  );

  for (const row of rows) {
    const permission = mapDbRowToDirectoryPermission(row);

    if (
      (permission.expiry_date_ms > 0 &&
        permission.expiry_date_ms <= currentTime) ||
      (permission.begin_date_ms > 0 && permission.begin_date_ms > currentTime)
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

/**
 * Gets the list of resources to check for inherited permissions by traversing up the directory hierarchy.
 * This mirrors Rust's `get_inherited_resources_list`.
 * @param resourceId The starting directory resource ID.
 * @param orgId The drive ID.
 * @returns A list of DirectoryResourceIDs, starting from the current resource and going up to the root,
 * stopping at a sovereign permission boundary if encountered.
 */
export async function getInheritedResourcesList(
  resourceId: DirectoryResourceID,
  orgId: string
): Promise<DirectoryResourceID[]> {
  const resources: DirectoryResourceID[] = [];
  let currentFolderId: FolderID | undefined;

  resources.push(resourceId);

  const resourcePrefix = resourceId.split("*")[0] + "*";
  const resourceActualId = resourceId.substring(resourcePrefix.length);

  if (resourcePrefix === IDPrefixEnum.File) {
    const fileMetadata = await getFileMetadata(orgId, resourceActualId);
    if (!fileMetadata) return [];

    if (fileMetadata.has_sovereign_permissions) {
      return resources;
    }
    currentFolderId = fileMetadata.parent_folder_uuid;
  } else if (resourcePrefix === IDPrefixEnum.Folder) {
    const folderMetadata = await getFolderMetadata(orgId, resourceActualId);
    if (!folderMetadata) return [];

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

/**
 * Derives visibility previews for breadcrumbs based on directory permissions.
 * This mirrors Rust's `derive_breadcrumb_visibility_previews`.
 * @param resourceId The ID of the directory resource.
 * @param orgId The drive ID.
 * @returns A list of BreadcrumbVisibilityPreviewEnum indicating public/private view/modify status.
 */
export async function deriveBreadcrumbVisibilityPreviews(
  resourceId: DirectoryResourceID,
  orgId: string
): Promise<BreadcrumbVisibilityPreviewEnum[]> {
  let publicCanView = false;
  let publicCanModify = false;
  let privateCanView = false;
  let privateCanModify = false;

  const currentTimeMs = Date.now();

  const { type: resourceType, id: resourceUUID } =
    parseDirectoryResourceIDForDb(resourceId);

  const rows = await db.queryDrive(
    orgId,
    `SELECT
        pd.id, pd.grantee_type, pd.grantee_id,
        GROUP_CONCAT(pdt.permission_type) AS permission_types_list,
        pd.begin_date_ms, pd.expiry_date_ms
    FROM permissions_directory pd
    JOIN permissions_directory_types pdt ON pd.id = pdt.permission_id
    WHERE pd.resource_id = ? AND pd.resource_type = ?
    GROUP BY pd.id;`,
    [resourceUUID, resourceType]
  );

  for (const row of rows) {
    const permissionGrantedTo = reconstructGranteeIDFromDb(
      row.grantee_type,
      row.grantee_id
    );
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

/**
 * Derives breadcrumbs for a directory path, respecting user permissions.
 * This mirrors Rust's `derive_directory_breadcrumbs`.
 * @param resourceId The starting directory resource ID.
 * @param userId The ID of the user.
 * @param orgId The drive ID.
 * @returns A list of FilePathBreadcrumb objects.
 */
export async function deriveDirectoryBreadcrumbs(
  resourceId: DirectoryResourceID,
  userId: UserID,
  orgId: string
): Promise<FilePathBreadcrumb[]> {
  const tempBreadcrumbs: FilePathBreadcrumb[] = [];
  const isOwner = (await getDriveOwnerId(orgId)) === userId;
  let currentResourceId: DirectoryResourceID | undefined = resourceId;

  while (currentResourceId) {
    let resourceName: string | undefined;
    let parentFolderId: FolderID | undefined;
    let hasSovereignPermissions = false;
    let resourceIsDiskRoot = false;

    const resourcePrefix = currentResourceId.split("*")[0] + "*";
    const resourceActualId = currentResourceId.substring(resourcePrefix.length);

    if (resourcePrefix === IDPrefixEnum.File) {
      const fileMetadata = await getFileMetadata(orgId, resourceActualId);
      if (!fileMetadata) break;

      resourceName = fileMetadata.name;
      parentFolderId = fileMetadata.parent_folder_uuid;
      hasSovereignPermissions = fileMetadata.has_sovereign_permissions;
    } else if (resourcePrefix === IDPrefixEnum.Folder) {
      const folderMetadata = await getFolderMetadata(orgId, resourceActualId);
      if (!folderMetadata) break;

      resourceName = folderMetadata.name;
      parentFolderId = folderMetadata.parent_folder_uuid;
      hasSovereignPermissions = folderMetadata.has_sovereign_permissions;

      if (
        folderMetadata.full_directory_path === `${folderMetadata.disk_id}::/`
      ) {
        resourceIsDiskRoot = true;
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
      resource_id: currentResourceId,
      resource_name: resourceName || "Unknown",
      visibility_preview: await deriveBreadcrumbVisibilityPreviews(
        currentResourceId,
        orgId
      ),
    });

    if (hasSovereignPermissions) {
      break;
    }

    currentResourceId = parentFolderId
      ? (`${IDPrefixEnum.Folder}${parentFolderId}` as DirectoryResourceID)
      : undefined;

    if (resourceIsDiskRoot) {
      break;
    }
  }

  return tempBreadcrumbs.reverse();
}

/**
 * Utility to convert DirectoryPermission to DirectoryPermissionFE.
 * This includes fetching related data like resource name, grantee/granter names,
 * and calculating `permission_previews` and `clipped_directory_path`.
 * @param permission A raw DirectoryPermission object.
 * @param currentUserId The ID of the user for whom to cast (for previews/redaction).
 * @param orgId The drive ID.
 * @returns A fully populated DirectoryPermissionFE object.
 */
export async function castToDirectoryPermissionFE(
  permission: DirectoryPermission,
  currentUserId: UserID,
  orgId: string
): Promise<DirectoryPermissionFE> {
  const isOwner = (await getDriveOwnerId(orgId)) === currentUserId;

  let resourceName: string | undefined;
  const resourcePrefix = permission.resource_id.split("*")[0] + "*";
  const resourceActualId = permission.resource_id.substring(
    resourcePrefix.length
  );

  if (resourcePrefix === IDPrefixEnum.File) {
    const fileMetadata = await getFileMetadata(orgId, resourceActualId);
    resourceName = fileMetadata?.name;
  } else if (resourcePrefix === IDPrefixEnum.Folder) {
    const folderMetadata = await getFolderMetadata(orgId, resourceActualId);
    resourceName = folderMetadata?.name;
  }

  let granteeName: string | undefined;
  let granteeAvatar: string | undefined;
  if (permission.granted_to === PUBLIC_GRANTEE_ID_STRING) {
    granteeName = "PUBLIC";
  } else if (permission.granted_to.startsWith(USER_ID_PREFIX)) {
    const contactInfo = await getContactInfo(
      orgId,
      permission.granted_to as UserID
    );
    granteeName = contactInfo?.name;
    granteeAvatar = contactInfo?.avatar;
    if (!granteeName) granteeName = `User: ${permission.granted_to}`;
  } else if (permission.granted_to.startsWith(GROUP_ID_PREFIX)) {
    const groupInfo = await getGroupInfo(
      orgId,
      permission.granted_to as GroupID
    );
    granteeName = groupInfo?.name;
    granteeAvatar = groupInfo?.avatar;
    if (!granteeName) granteeName = `Group: ${permission.granted_to}`;
  } else if (
    permission.granted_to.startsWith(
      PLACEHOLDER_DIRECTORY_PERMISSION_GRANTEE_ID_PREFIX
    )
  ) {
    granteeName = "Awaiting Anon";
  }

  const granterInfo = await getContactInfo(orgId, permission.granted_by);
  const granterName = granterInfo?.name || `Granter: ${permission.granted_by}`;

  // This checkSystemPermissions function is for general system permissions, not directory-specific ones.
  // The types for `permission_previews` on DirectoryPermissionFE are `SystemPermissionType[]`.
  const permissionPreviews = await getSystemPermissionsForRecord(
    `RECORD_Permission_${permission.id}`, // Example SystemResourceID for a permission record
    currentUserId,
    orgId
  );

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

/**
 * Function to redact DirectoryPermissionFE, mirroring Rust's `redacted` method.
 * @param permissionFe The frontend permission object to redact.
 * @param userId The ID of the user for whom to redact.
 * @param isOwner Whether the user is the drive owner.
 * @param orgId The drive ID.
 * @returns The redacted DirectoryPermissionFE.
 */
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

  redacted.labels = (
    await Promise.all(
      redacted.labels.map(
        async (labelValue) => await redactLabelValue(orgId, labelValue, userId)
      )
    )
  ).filter((label): label is LabelValue => label !== null);

  return redacted;
}

// Helper to parse DirectoryResourceID string (used by handlers for validation/casting)
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

// Helper to parse PermissionGranteeID from string (used by handlers for validation/casting)
export function parsePermissionGranteeIDString(idStr: string): GranteeID {
  if (idStr === PUBLIC_GRANTEE_ID_STRING) {
    return PUBLIC_GRANTEE_ID_STRING;
  }
  if (
    idStr.startsWith(USER_ID_PREFIX) ||
    idStr.startsWith(GROUP_ID_PREFIX) ||
    idStr.startsWith(PLACEHOLDER_DIRECTORY_PERMISSION_GRANTEE_ID_PREFIX)
  ) {
    return idStr as GranteeID;
  }
  throw new Error(`Invalid GranteeID format: ${idStr}`);
}

/**
 * Retrieves a directory permission by its ID.
 * @param orgId The drive ID.
 * @param permissionId The ID of the permission to retrieve.
 * @param requesterId The ID of the user requesting the permission (for internal auth/previews).
 * @param skipAuthorizationCheck If true, returns the raw permission without checking if `requesterId` can access _this record_.
 * @returns The raw DirectoryPermission object, or null if not found.
 */
export async function getDirectoryPermissionById(
  orgId: string,
  permissionId: string,
  requesterId: UserID,
  skipAuthorizationCheck: boolean = false
): Promise<DirectoryPermission | null> {
  const query = `
    SELECT
        pd.id, pd.resource_type, pd.resource_id, pd.resource_path,
        pd.grantee_type, pd.grantee_id, pd.granted_by_user_id,
        GROUP_CONCAT(pdt.permission_type) AS permission_types_list,
        pd.begin_date_ms, pd.expiry_date_ms, pd.inheritable, pd.note,
        pd.created_at, pd.last_modified_at, pd.redeem_code, pd.from_placeholder_grantee,
        pd.metadata_type, pd.metadata_content, pd.external_id, pd.external_payload,
        GROUP_CONCAT(pdl.label_id) AS labels_list
    FROM permissions_directory pd
    LEFT JOIN permissions_directory_types pdt ON pd.id = pdt.permission_id
    LEFT JOIN permission_directory_labels pdl ON pd.id = pdl.permission_id
    WHERE pd.id = ?
    GROUP BY pd.id;`;

  const rows = await db.queryDrive(orgId, query, [permissionId]);

  if (rows.length === 0) {
    return null;
  }

  const rawPermission = mapDbRowToDirectoryPermission(rows[0]);

  if (!skipAuthorizationCheck) {
    const isOwner = (await getDriveOwnerId(orgId)) === requesterId;
    const canAccess = await canUserAccessDirectoryPermission(
      requesterId,
      rawPermission,
      isOwner,
      orgId
    );
    if (!canAccess) {
      return null;
    }
  }

  return rawPermission;
}

/**
 * Lists directory permissions for a specific resource, with pagination and authorization.
 * @param options Query options including resourceId, requesterId, pageSize, direction, and cursor.
 * @returns An object containing items, total count, new cursor, and authorization status.
 */
export async function listDirectoryPermissionsForResource(options: {
  orgId: string;
  resourceId: DirectoryResourceID;
  requesterId: UserID;
  pageSize: number;
  direction: SortDirection;
  cursor?: string | null;
}): Promise<{
  items: DirectoryPermissionFE[];
  total: number;
  newCursor?: string | null;
  authorized: boolean;
}> {
  const { orgId, resourceId, requesterId, pageSize, direction, cursor } =
    options;
  const items: DirectoryPermissionFE[] = [];
  let total = 0;
  let newCursor: string | null = null;

  const isOwner = (await getDriveOwnerId(orgId)) === requesterId;
  const resourcePermissions = await checkDirectoryPermissions(
    resourceId,
    requesterId,
    orgId
  );
  const hasViewPermissionOnResource = resourcePermissions.includes(
    DirectoryPermissionType.VIEW
  );

  if (!isOwner && !hasViewPermissionOnResource) {
    return { items: [], total: 0, authorized: false };
  }

  let query = `
    SELECT
        pd.id, pd.resource_type, pd.resource_id, pd.resource_path,
        pd.grantee_type, pd.grantee_id, pd.granted_by_user_id,
        GROUP_CONCAT(DISTINCT pdt.permission_type) AS permission_types_list,
        pd.begin_date_ms, pd.expiry_date_ms, pd.inheritable, pd.note,
        pd.created_at, pd.last_modified_at, pd.redeem_code, pd.from_placeholder_grantee,
        pd.metadata_type, pd.metadata_content, pd.external_id, pd.external_payload,
        GROUP_CONCAT(DISTINCT pdl.label_id) AS labels_list
    FROM permissions_directory pd
    LEFT JOIN permissions_directory_types pdt ON pd.id = pdt.permission_id
    LEFT JOIN permission_directory_labels pdl ON pd.id = pdl.permission_id
    WHERE pd.resource_id = ?
    GROUP BY pd.id
  `;

  const countQuery = `SELECT COUNT(DISTINCT id) AS count FROM permissions_directory WHERE resource_id = ?`;
  const countRows = await db.queryDrive(orgId, countQuery, [resourceId]);
  total = countRows[0]?.count || 0;

  let params: any[] = [resourceId];
  let orderByClause = `ORDER BY pd.created_at ${direction}`;
  let limitClause = `LIMIT ?`;

  if (cursor) {
    const cursorTime = parseInt(cursor, 10);
    if (direction === SortDirection.ASC) {
      orderByClause = `ORDER BY pd.created_at ASC`;
      query += ` AND pd.created_at > ?`;
      params.push(cursorTime);
    } else {
      orderByClause = `ORDER BY pd.created_at DESC`;
      query += ` AND pd.created_at < ?`;
      params.push(cursorTime);
    }
  }

  query += ` ${orderByClause} ${limitClause}`;
  params.push(pageSize + 1);

  const rows = await db.queryDrive(orgId, query, params);

  for (let i = 0; i < rows.length && i < pageSize; i++) {
    const rawPerm = mapDbRowToDirectoryPermission(rows[i]);
    const canAccessRecord = await canUserAccessDirectoryPermission(
      requesterId,
      rawPerm,
      isOwner,
      orgId
    );
    if (canAccessRecord) {
      items.push(
        await castToDirectoryPermissionFE(rawPerm, requesterId, orgId)
      );
    }
  }

  if (rows.length > pageSize) {
    newCursor = rows[pageSize - 1].created_at.toString();
  }

  return { items, total, newCursor, authorized: true };
}

/**
 * Creates a new directory permission.
 * @param orgId The drive ID.
 * @param data The request body containing permission details.
 * @param requesterId The user creating the permission.
 * @returns The created DirectoryPermissionFE.
 */
export async function createDirectoryPermission(
  orgId: string,
  data: {
    id?: DirectoryPermission["id"];
    resource_id: DirectoryResourceID;
    granted_to?: GranteeID;
    permission_types: DirectoryPermissionType[];
    begin_date_ms?: number;
    expiry_date_ms?: number;
    inheritable: boolean;
    note?: string;
    metadata?: PermissionMetadata;
    external_id?: string;
    external_payload?: string;
    redeem_code?: string;
  },
  requesterId: UserID
): Promise<DirectoryPermissionFE> {
  const newPermissionId =
    data.id || IDPrefixEnum.DirectoryPermission + uuidv4();
  const currentTime = Date.now();

  const { type: resourceType, id: resourceUUID } =
    parseDirectoryResourceIDForDb(data.resource_id);
  const { type: granteeType, id: granteeUUID } = parseGranteeIDForDb(
    data.granted_to || PUBLIC_GRANTEE_ID_STRING
  );

  let resourcePath = "";
  if (resourceType === "File") {
    const file = await getFileMetadata(orgId, resourceUUID as FileID);
    if (file) resourcePath = file.full_directory_path;
  } else if (resourceType === "Folder") {
    const folder = await getFolderMetadata(orgId, resourceUUID as FolderID);
    if (folder) resourcePath = folder.full_directory_path;
  }
  if (!resourcePath) {
    throw new Error(
      `Resource with ID ${data.resource_id} not found to determine its path.`
    );
  }

  const metadataType = data.metadata?.metadata_type || null;
  let metadataContent: string | null = null;
  if (data.metadata) {
    if ("Labels" in data.metadata.content) {
      metadataContent = data.metadata.content.Labels;
    } else if ("DirectoryPassword" in data.metadata.content) {
      metadataContent = data.metadata.content.DirectoryPassword;
    }
  }

  return dbHelpers.transaction("drive", orgId, async (tx) => {
    tx.prepare(
      `
        INSERT INTO permissions_directory (
          id, resource_type, resource_id, resource_path, grantee_type, grantee_id,
          granted_by_user_id, begin_date_ms, expiry_date_ms, inheritable, note,
          created_at, last_modified_at, redeem_code, from_placeholder_grantee,
          metadata_type, metadata_content, external_id, external_payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `
    ).run(
      newPermissionId,
      resourceType,
      resourceUUID,
      resourcePath,
      granteeType,
      granteeUUID,
      requesterId,
      data.begin_date_ms || 0,
      data.expiry_date_ms || -1,
      data.inheritable ? 1 : 0,
      data.note || "",
      currentTime,
      currentTime,
      data.redeem_code || null,
      null, // `from_placeholder_grantee` is null on creation
      metadataType,
      metadataContent,
      data.external_id || null,
      data.external_payload || null
    );

    const insertPermTypeStmt = tx.prepare(`
        INSERT INTO permissions_directory_types (permission_id, permission_type) VALUES (?, ?)
      `);
    for (const type of data.permission_types) {
      insertPermTypeStmt.run(newPermissionId, type);
    }

    const createdPermission = await getDirectoryPermissionById(
      orgId,
      newPermissionId,
      requesterId,
      true
    );
    if (!createdPermission) {
      throw new Error("Failed to retrieve newly created permission.");
    }
    return castToDirectoryPermissionFE(createdPermission, requesterId, orgId);
  });
}

/**
 * Updates an existing directory permission.
 * @param orgId The drive ID.
 * @param data The request body containing updates.
 * @param requesterId The user performing the update.
 * @returns The updated DirectoryPermissionFE, or null if not found.
 */
export async function updateDirectoryPermission(
  orgId: string,
  data: {
    id: DirectoryPermission["id"];
    permission_types?: DirectoryPermissionType[];
    begin_date_ms?: number;
    expiry_date_ms?: number;
    inheritable?: boolean;
    note?: string;
    metadata?: PermissionMetadata;
    external_id?: string;
    external_payload?: string;
    redeem_code?: string;
  },
  requesterId: UserID
): Promise<DirectoryPermissionFE | null> {
  const currentTime = Date.now();

  const metadataType = data.metadata?.metadata_type || null;
  let metadataContent: string | null = null;
  if (data.metadata) {
    if ("Labels" in data.metadata.content) {
      metadataContent = data.metadata.content.Labels;
    } else if ("DirectoryPassword" in data.metadata.content) {
      metadataContent = data.metadata.content.DirectoryPassword;
    }
  }

  const result = await dbHelpers.transaction("drive", orgId, async (tx) => {
    let updateFields: string[] = [];
    let updateParams: any[] = [];

    if (data.begin_date_ms !== undefined) {
      updateFields.push("begin_date_ms = ?");
      updateParams.push(data.begin_date_ms);
    }
    if (data.expiry_date_ms !== undefined) {
      updateFields.push("expiry_date_ms = ?");
      updateParams.push(data.expiry_date_ms);
    }
    if (data.inheritable !== undefined) {
      updateFields.push("inheritable = ?");
      updateParams.push(data.inheritable ? 1 : 0);
    }
    if (data.note !== undefined) {
      updateFields.push("note = ?");
      updateParams.push(data.note);
    }
    if (data.metadata !== undefined) {
      updateFields.push("metadata_type = ?", "metadata_content = ?");
      updateParams.push(metadataType, metadataContent);
    }
    if (data.external_id !== undefined) {
      updateFields.push("external_id = ?");
      updateParams.push(data.external_id);
    }
    if (data.external_payload !== undefined) {
      updateFields.push("external_payload = ?");
      updateParams.push(data.external_payload);
    }
    if (data.redeem_code !== undefined) {
      updateFields.push("redeem_code = ?");
      updateParams.push(data.redeem_code);
    }

    updateFields.push("last_modified_at = ?");
    updateParams.push(currentTime);

    if (updateFields.length > 0) {
      const updateQuery = `
        UPDATE permissions_directory
        SET ${updateFields.join(", ")}
        WHERE id = ?
      `;
      tx.prepare(updateQuery).run(...updateParams, data.id);
    }

    if (data.permission_types !== undefined) {
      tx.prepare(
        `DELETE FROM permissions_directory_types WHERE permission_id = ?`
      ).run(data.id);
      const insertPermTypeStmt = tx.prepare(`
        INSERT INTO permissions_directory_types (permission_id, permission_type) VALUES (?, ?)
      `);
      for (const type of data.permission_types) {
        insertPermTypeStmt.run(data.id, type);
      }
    }

    const updatedPermission = await getDirectoryPermissionById(
      orgId,
      data.id,
      requesterId,
      true
    );
    return updatedPermission;
  });

  if (!result) {
    return null;
  }
  return castToDirectoryPermissionFE(result, requesterId, orgId);
}

/**
 * Deletes a directory permission.
 * @param orgId The drive ID.
 * @param permissionId The ID of the permission to delete.
 * @param requesterId The user performing the deletion.
 * @returns The ID of the deleted permission, or null if not found/deleted.
 */
export async function deleteDirectoryPermission(
  orgId: string,
  permissionId: string,
  requesterId: UserID
): Promise<string | null> {
  return dbHelpers.transaction("drive", orgId, (tx) => {
    tx.prepare(
      `DELETE FROM permissions_directory_types WHERE permission_id = ?`
    ).run(permissionId);
    tx.prepare(
      `DELETE FROM permission_directory_labels WHERE permission_id = ?`
    ).run(permissionId);

    const deleteResult = tx
      .prepare(`DELETE FROM permissions_directory WHERE id = ?`)
      .run(permissionId);

    if (deleteResult.changes && deleteResult.changes > 0) {
      return permissionId;
    }
    return null;
  });
}

/**
 * Redeems a placeholder directory permission, converting it to a user-specific permission.
 * @param orgId The drive ID.
 * @param data Redemption details.
 * @returns The redeemed DirectoryPermissionFE or an error.
 */
export async function redeemDirectoryPermission(
  orgId: string,
  data: {
    permission_id: DirectoryPermission["id"];
    user_id: UserID;
    redeem_code: string;
    note?: string;
    requesterId: UserID;
  }
): Promise<{ permission?: DirectoryPermissionFE; error?: string }> {
  const currentTime = Date.now();

  const existingPermission = await getDirectoryPermissionById(
    orgId,
    data.permission_id,
    data.requesterId,
    true
  );

  if (!existingPermission) {
    return { error: "Permission not found" };
  }

  if (
    !existingPermission.granted_to.startsWith(
      PLACEHOLDER_DIRECTORY_PERMISSION_GRANTEE_ID_PREFIX
    )
  ) {
    return { error: "Permission is not a redeemable placeholder" };
  }
  if (
    !existingPermission.redeem_code ||
    existingPermission.redeem_code !== data.redeem_code
  ) {
    return { error: "Invalid redeem code" };
  }

  if (
    existingPermission.expiry_date_ms > 0 &&
    existingPermission.expiry_date_ms <= currentTime
  ) {
    return { error: "Permission has expired" };
  }
  if (
    existingPermission.begin_date_ms > 0 &&
    existingPermission.begin_date_ms > currentTime
  ) {
    return { error: "Permission is not yet active" };
  }

  return dbHelpers.transaction("drive", orgId, async (tx) => {
    const { type: newGranteeType, id: newGranteeId } = parseGranteeIDForDb(
      data.user_id
    );

    tx.prepare(
      `
      UPDATE permissions_directory
      SET
        grantee_type = ?,
        grantee_id = ?,
        redeem_code = NULL,
        from_placeholder_grantee = ?,
        note = ?,
        last_modified_at = ?
      WHERE id = ?
    `
    ).run(
      newGranteeType,
      newGranteeId,
      existingPermission.granted_to, // Store original placeholder as from_placeholder_grantee
      data.note || existingPermission.note,
      currentTime,
      data.permission_id
    );

    const updatedPermission = await getDirectoryPermissionById(
      orgId,
      data.permission_id,
      data.requesterId,
      true
    );

    if (!updatedPermission) {
      return { error: "Failed to redeem permission: internal error." };
    }

    return {
      permission: await castToDirectoryPermissionFE(
        updatedPermission,
        data.requesterId,
        orgId
      ),
    };
  });
}

// --- System Permission Core Logic ---

/**
 * Checks what kind of permission a specific user has on a specific system resource.
 * This function effectively combines and evaluates all relevant system permissions
 * (direct, group-based, public).
 * @param resourceId The ID of the system resource (e.g., "TABLE_DRIVES", "RECORD_DriveID_xyz").
 * @param granteeId The ID of the user or group to check permissions for (e.g., "UserID*...", "GroupID*...", "PUBLIC").
 * @param orgId The drive ID.
 * @returns A list of unique SystemPermissionType enums that the grantee has.
 */
export async function getSystemPermissionsForRecord(
  resourceId: string, // This is SystemResourceID in Rust, but mapped to string here.
  granteeId: GranteeID,
  orgId: string
): Promise<SystemPermissionType[]> {
  const allPermissions = new Set<SystemPermissionType>();
  const currentTime = Date.now();

  const isOwner =
    (await getDriveOwnerId(orgId)) ===
    (granteeId.startsWith(USER_ID_PREFIX) ? granteeId : "");
  if (isOwner) {
    // Owner has all permissions on all system resources
    return [
      SystemPermissionType.CREATE,
      SystemPermissionType.EDIT,
      SystemPermissionType.DELETE,
      SystemPermissionType.VIEW,
      SystemPermissionType.INVITE,
    ];
  }

  // Determine resource_type ('Table' or 'Record') and resource_identifier
  let resourceType: string;
  let resourceIdentifier: string;

  if (resourceId.startsWith("TABLE_")) {
    resourceType = "Table";
    resourceIdentifier = resourceId.substring("TABLE_".length);
  } else if (resourceId.startsWith("RECORD_")) {
    resourceType = "Record";
    resourceIdentifier = resourceId.substring("RECORD_".length);
  } else {
    // Fallback or throw for unknown system resource IDs
    console.warn(
      `Unknown SystemResourceID format: ${resourceId}. Treating as unknown.`
    );
    resourceType = "Unknown";
    resourceIdentifier = resourceId;
  }

  const rows = await db.queryDrive(
    orgId,
    `SELECT
        ps.id, ps.resource_type, ps.resource_identifier, ps.grantee_type, ps.grantee_id,
        ps.granted_by_user_id, GROUP_CONCAT(pst.permission_type) AS permission_types_list,
        ps.begin_date_ms, ps.expiry_date_ms, ps.note, ps.created_at, ps.last_modified_at,
        ps.redeem_code, ps.from_placeholder_grantee, ps.metadata_type, ps.metadata_content,
        ps.external_id, ps.external_payload,
        GROUP_CONCAT(psl.label_id) AS labels_list
    FROM permissions_system ps
    LEFT JOIN permissions_system_types pst ON ps.id = pst.permission_id
    LEFT JOIN permission_system_labels psl ON ps.id = psl.permission_id
    WHERE ps.resource_type = ? AND ps.resource_identifier = ?
    GROUP BY ps.id;`,
    [resourceType, resourceIdentifier]
  );

  for (const row of rows) {
    const permission = mapDbRowToSystemPermission(row);

    if (
      (permission.expiry_date_ms > 0 &&
        permission.expiry_date_ms <= currentTime) ||
      (permission.begin_date_ms > 0 && permission.begin_date_ms > currentTime)
    ) {
      continue;
    }

    let applies = false;
    const permissionGrantedTo = permission.granted_to;

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
      applies = await isUserInGroup(
        granteeId as UserID,
        permissionGrantedTo as GroupID,
        orgId
      );
    } else if (
      granteeId.startsWith(
        PLACEHOLDER_DIRECTORY_PERMISSION_GRANTEE_ID_PREFIX
      ) && // Placeholder for system permissions also
      permissionGrantedTo.startsWith(
        PLACEHOLDER_DIRECTORY_PERMISSION_GRANTEE_ID_PREFIX
      )
    ) {
      applies = granteeId === permissionGrantedTo;
    }

    if (applies) {
      permission.permission_types.forEach((type) => allPermissions.add(type));
    }
  }
  return Array.from(allPermissions);
}

/**
 * Utility to convert SystemPermission to SystemPermissionFE.
 * @param permission A raw SystemPermission object.
 * @param currentUserId The ID of the user for whom to cast (for previews/redaction).
 * @param orgId The drive ID.
 * @returns A fully populated SystemPermissionFE object.
 */
export async function castToSystemPermissionFE(
  permission: SystemPermission,
  currentUserId: UserID,
  orgId: string
): Promise<any> {
  // SystemPermissionFE
  const isOwner = (await getDriveOwnerId(orgId)) === currentUserId;

  let granteeName: string | undefined;
  let granteeAvatar: string | undefined;
  if (permission.granted_to === PUBLIC_GRANTEE_ID_STRING) {
    granteeName = "PUBLIC";
  } else if (permission.granted_to.startsWith(USER_ID_PREFIX)) {
    const contactInfo = await getContactInfo(
      orgId,
      permission.granted_to as UserID
    );
    granteeName = contactInfo?.name;
    granteeAvatar = contactInfo?.avatar;
    if (!granteeName) granteeName = `User: ${permission.granted_to}`;
  } else if (permission.granted_to.startsWith(GROUP_ID_PREFIX)) {
    const groupInfo = await getGroupInfo(
      orgId,
      permission.granted_to as GroupID
    );
    granteeName = groupInfo?.name;
    granteeAvatar = groupInfo?.avatar;
    if (!granteeName) granteeName = `Group: ${permission.granted_to}`;
  } else if (
    permission.granted_to.startsWith(
      PLACEHOLDER_DIRECTORY_PERMISSION_GRANTEE_ID_PREFIX
    )
  ) {
    granteeName = "Awaiting Anon";
  }

  const granterInfo = await getContactInfo(orgId, permission.granted_by);
  const granterName = granterInfo?.name || `Granter: ${permission.granted_by}`;

  const permissionPreviews = await getSystemPermissionsForRecord(
    `RECORD_Permission_${permission.id}`,
    currentUserId,
    orgId
  );

  const castedPermission: any = {
    // Use 'any' or define SystemPermissionFE more strictly
    id: permission.id,
    resource_id: permission.resource_id,
    granted_to: permission.granted_to,
    granted_by: permission.granted_by,
    permission_types: permission.permission_types,
    begin_date_ms: permission.begin_date_ms,
    expiry_date_ms: permission.expiry_date_ms,
    note: permission.note,
    created_at: permission.created_at,
    last_modified_at: permission.last_modified_at,
    from_placeholder_grantee: permission.from_placeholder_grantee,
    labels: permission.labels,
    redeem_code: permission.redeem_code,
    metadata: permission.metadata,
    external_id: permission.external_id,
    external_payload: permission.external_payload,
    resource_name: permission.resource_id, // For system, resource_id is often the "name"
    grantee_name: granteeName,
    grantee_avatar: granteeAvatar,
    granter_name: granterName,
    permission_previews: permissionPreviews,
  };

  return redactSystemPermissionFE(
    castedPermission,
    currentUserId,
    isOwner,
    orgId
  );
}

/**
 * Function to redact SystemPermissionFE.
 * @param permissionFe The frontend permission object to redact.
 * @param userId The ID of the user for whom to redact.
 * @param isOwner Whether the user is the drive owner.
 * @param orgId The drive ID.
 * @returns The redacted SystemPermissionFE.
 */
export async function redactSystemPermissionFE(
  permissionFe: any, // SystemPermissionFE
  userId: UserID,
  isOwner: boolean,
  orgId: DriveID
): Promise<any> {
  // SystemPermissionFE
  const redacted = { ...permissionFe };

  // Redaction logic for system permissions if any specific fields need it
  // Similar to directory permissions, labels can be redacted
  redacted.labels = (
    await Promise.all(
      redacted.labels.map(
        async (labelValue: LabelValue) =>
          await redactLabelValue(orgId, labelValue, userId)
      )
    )
  ).filter((label): label is LabelValue => label !== null);

  return redacted;
}

/**
 * Retrieves a system permission by its ID.
 * @param orgId The drive ID.
 * @param permissionId The ID of the permission to retrieve.
 * @param requesterId The ID of the user requesting the permission.
 * @param skipAuthorizationCheck If true, returns the raw permission without checking if `requesterId` can access _this record_.
 * @returns The raw SystemPermission object, or null if not found.
 */
export async function getSystemPermissionById(
  orgId: string,
  permissionId: string,
  requesterId: UserID,
  skipAuthorizationCheck: boolean = false
): Promise<SystemPermission | null> {
  const query = `
    SELECT
        ps.id, ps.resource_type, ps.resource_identifier, ps.grantee_type, ps.grantee_id,
        ps.granted_by_user_id, GROUP_CONCAT(pst.permission_type) AS permission_types_list,
        ps.begin_date_ms, ps.expiry_date_ms, ps.note, ps.created_at, ps.last_modified_at,
        ps.redeem_code, ps.from_placeholder_grantee, ps.metadata_type, ps.metadata_content,
        ps.external_id, ps.external_payload,
        GROUP_CONCAT(psl.label_id) AS labels_list
    FROM permissions_system ps
    LEFT JOIN permissions_system_types pst ON ps.id = pst.permission_id
    LEFT JOIN permission_system_labels psl ON ps.id = psl.permission_id
    WHERE ps.id = ?
    GROUP BY ps.id;`;

  const rows = await db.queryDrive(orgId, query, [permissionId]);

  if (rows.length === 0) {
    return null;
  }

  const rawPermission = mapDbRowToSystemPermission(rows[0]);

  if (!skipAuthorizationCheck) {
    const isOwner = (await getDriveOwnerId(orgId)) === requesterId;
    const canAccess =
      (
        await getSystemPermissionsForRecord(
          `RECORD_Permission_${permissionId}`,
          requesterId,
          orgId
        )
      ).includes(SystemPermissionType.VIEW) || isOwner;
    if (!canAccess) {
      return null;
    }
  }

  return rawPermission;
}

/**
 * Lists system permissions, with pagination and authorization.
 * @param options Query options including filters, requesterId, pageSize, direction, and cursor.
 * @returns An object containing items, total count, new cursor, and authorization status.
 */
export async function listSystemPermissions(options: {
  orgId: string;
  filters?: {
    resource_ids?: string[];
    grantee_ids?: GranteeID[];
    labels?: string[]; // Assuming label values are provided for filtering
  };
  requesterId: UserID;
  pageSize: number;
  direction: SortDirection;
  cursor?: string | null;
}): Promise<{
  items: any[]; // SystemPermissionFE[]
  total: number;
  newCursor?: string | null;
  authorized: boolean;
}> {
  const { orgId, filters, requesterId, pageSize, direction, cursor } = options;
  const items: any[] = []; // SystemPermissionFE[]
  let total = 0;
  let newCursor: string | null = null;

  const isOwner = (await getDriveOwnerId(orgId)) === requesterId;
  const canListPermissions =
    (
      await getSystemPermissionsForRecord(
        `TABLE_${SystemTableValueEnum.PERMISSIONS}`,
        requesterId,
        orgId
      )
    ).includes(SystemPermissionType.VIEW) || isOwner;

  if (!canListPermissions) {
    return { items: [], total: 0, authorized: false };
  }

  let query = `
    SELECT
        ps.id, ps.resource_type, ps.resource_identifier, ps.grantee_type, ps.grantee_id,
        ps.granted_by_user_id, GROUP_CONCAT(DISTINCT pst.permission_type) AS permission_types_list,
        ps.begin_date_ms, ps.expiry_date_ms, ps.note, ps.created_at, ps.last_modified_at,
        ps.redeem_code, ps.from_placeholder_grantee, ps.metadata_type, ps.metadata_content,
        ps.external_id, ps.external_payload,
        GROUP_CONCAT(DISTINCT psl.label_id) AS labels_list
    FROM permissions_system ps
    LEFT JOIN permissions_system_types pst ON ps.id = pst.permission_id
    LEFT JOIN permission_system_labels psl ON ps.id = psl.permission_id
    WHERE 1=1
  `;

  let countQuery = `SELECT COUNT(DISTINCT id) AS count FROM permissions_system WHERE 1=1`;
  const queryParams: any[] = [];
  const countParams: any[] = [];

  if (filters?.resource_ids && filters.resource_ids.length > 0) {
    const placeholders = filters.resource_ids.map(() => "?").join(",");
    query += ` AND ps.resource_identifier IN (${placeholders})`;
    countQuery += ` AND resource_identifier IN (${placeholders})`;
    queryParams.push(...filters.resource_ids);
    countParams.push(...filters.resource_ids);
  }
  if (filters?.grantee_ids && filters.grantee_ids.length > 0) {
    const placeholders = filters.grantee_ids.map(() => "?").join(",");
    query += ` AND ps.grantee_id IN (${placeholders})`; // Note: This might need more complex parsing if grantee_id is composite
    countQuery += ` AND grantee_id IN (${placeholders})`;
    queryParams.push(...filters.grantee_ids);
    countParams.push(...filters.grantee_ids);
  }
  // Labels filtering would require joining with permission_system_labels and filtering by label_id
  // This is a simplified implementation. A robust filter for labels would be more complex.
  if (filters?.labels && filters.labels.length > 0) {
    const labelPlaceholders = filters.labels.map(() => "?").join(",");
    query += ` AND ps.id IN (SELECT permission_id FROM permission_system_labels WHERE label_id IN (${labelPlaceholders}))`;
    countQuery += ` AND id IN (SELECT permission_id FROM permission_system_labels WHERE label_id IN (${labelPlaceholders}))`;
    queryParams.push(...filters.labels);
    countParams.push(...filters.labels);
  }

  query += ` GROUP BY ps.id`; // Group by for GROUP_CONCAT

  const countRows = await db.queryDrive(orgId, countQuery, countParams);
  total = countRows[0]?.count || 0;

  let orderByClause = `ORDER BY ps.created_at ${direction}`;
  let limitClause = `LIMIT ?`;

  if (cursor) {
    const cursorTime = parseInt(cursor, 10);
    if (direction === SortDirection.ASC) {
      orderByClause = `ORDER BY ps.created_at ASC`;
      query += ` AND ps.created_at > ?`;
      queryParams.push(cursorTime);
    } else {
      orderByClause = `ORDER BY ps.created_at DESC`;
      query += ` AND ps.created_at < ?`;
      queryParams.push(cursorTime);
    }
  }

  query += ` ${orderByClause} ${limitClause}`;
  queryParams.push(pageSize + 1);

  const rows = await db.queryDrive(orgId, query, queryParams);

  for (let i = 0; i < rows.length && i < pageSize; i++) {
    const rawPerm = mapDbRowToSystemPermission(rows[i]);
    const canAccessRecord =
      (
        await getSystemPermissionsForRecord(
          `RECORD_Permission_${rawPerm.id}`,
          requesterId,
          orgId
        )
      ).includes(SystemPermissionType.VIEW) || isOwner;
    if (canAccessRecord) {
      items.push(await castToSystemPermissionFE(rawPerm, requesterId, orgId));
    }
  }

  if (rows.length > pageSize) {
    newCursor = rows[pageSize - 1].created_at.toString();
  }

  return { items, total, newCursor, authorized: true };
}

/**
 * Creates a new system permission.
 * @param orgId The drive ID.
 * @param data The request body containing permission details.
 * @param requesterId The user creating the permission.
 * @returns The created SystemPermissionFE.
 */
export async function createSystemPermission(
  orgId: string,
  data: {
    id?: SystemPermission["id"];
    resource_id: string; // This is SystemResourceID in Rust, but string here.
    granted_to?: GranteeID;
    permission_types: SystemPermissionType[];
    begin_date_ms?: number;
    expiry_date_ms?: number;
    note?: string;
    metadata?: PermissionMetadata;
    external_id?: string;
    external_payload?: string;
    redeem_code?: string;
  },
  requesterId: UserID
): Promise<any> {
  // SystemPermissionFE
  const newPermissionId = data.id || IDPrefixEnum.SystemPermission + uuidv4();
  const currentTime = Date.now();

  const { type: granteeType, id: granteeUUID } = parseGranteeIDForDb(
    data.granted_to || PUBLIC_GRANTEE_ID_STRING
  );

  // Determine resource_type ('Table' or 'Record') and resource_identifier
  let resourceType: string;
  let resourceIdentifier: string;

  if (data.resource_id.startsWith("TABLE_")) {
    resourceType = "Table";
    resourceIdentifier = data.resource_id.substring("TABLE_".length);
  } else if (data.resource_id.startsWith("RECORD_")) {
    resourceType = "Record";
    resourceIdentifier = data.resource_id.substring("RECORD_".length);
  } else {
    // Default or throw error for invalid resource_id format
    resourceType = "Unknown"; // Or throw new Error
    resourceIdentifier = data.resource_id;
  }

  const metadataType = data.metadata?.metadata_type || null;
  let metadataContent: string | null = null;
  if (data.metadata) {
    if ("Labels" in data.metadata.content) {
      metadataContent = data.metadata.content.Labels;
    } else if ("DirectoryPassword" in data.metadata.content) {
      metadataContent = data.metadata.content.DirectoryPassword;
    }
  }

  return dbHelpers.transaction("drive", orgId, async (tx) => {
    tx.prepare(
      `
        INSERT INTO permissions_system (
          id, resource_type, resource_identifier, grantee_type, grantee_id,
          granted_by_user_id, begin_date_ms, expiry_date_ms, note,
          created_at, last_modified_at, redeem_code, from_placeholder_grantee,
          metadata_type, metadata_content, external_id, external_payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `
    ).run(
      newPermissionId,
      resourceType,
      resourceIdentifier,
      granteeType,
      granteeUUID,
      requesterId,
      data.begin_date_ms || 0,
      data.expiry_date_ms || -1,
      data.note || "",
      currentTime,
      currentTime,
      data.redeem_code || null,
      null, // from_placeholder_grantee is null on creation
      metadataType,
      metadataContent,
      data.external_id || null,
      data.external_payload || null
    );

    const insertPermTypeStmt = tx.prepare(`
        INSERT INTO permissions_system_types (permission_id, permission_type) VALUES (?, ?)
      `);
    for (const type of data.permission_types) {
      insertPermTypeStmt.run(newPermissionId, type);
    }

    const createdPermission = await getSystemPermissionById(
      orgId,
      newPermissionId,
      requesterId,
      true
    );
    if (!createdPermission) {
      throw new Error("Failed to retrieve newly created system permission.");
    }
    return castToSystemPermissionFE(createdPermission, requesterId, orgId);
  });
}

/**
 * Updates an existing system permission.
 * @param orgId The drive ID.
 * @param data The request body containing updates.
 * @param requesterId The user performing the update.
 * @returns The updated SystemPermissionFE, or null if not found.
 */
export async function updateSystemPermission(
  orgId: string,
  data: {
    id: SystemPermissionID;
    resource_id?: string;
    granted_to?: GranteeID;
    permission_types?: SystemPermissionType[];
    begin_date_ms?: number;
    expiry_date_ms?: number;
    note?: string;
    metadata?: PermissionMetadata;
    external_id?: string;
    external_payload?: string;
    redeem_code?: string;
  },
  requesterId: UserID
): Promise<any | null> {
  // SystemPermissionFE
  const currentTime = Date.now();

  const metadataType = data.metadata?.metadata_type || null;
  let metadataContent: string | null = null;
  if (data.metadata) {
    if ("Labels" in data.metadata.content) {
      metadataContent = data.metadata.content.Labels;
    } else if ("DirectoryPassword" in data.metadata.content) {
      metadataContent = data.metadata.content.DirectoryPassword;
    }
  }

  const result = await dbHelpers.transaction("drive", orgId, async (tx) => {
    let updateFields: string[] = [];
    let updateParams: any[] = [];

    // Only update resource_type and resource_identifier if resource_id is provided
    if (data.resource_id !== undefined) {
      let resourceType: string;
      let resourceIdentifier: string;
      if (data.resource_id.startsWith("TABLE_")) {
        resourceType = "Table";
        resourceIdentifier = data.resource_id.substring("TABLE_".length);
      } else if (data.resource_id.startsWith("RECORD_")) {
        resourceType = "Record";
        resourceIdentifier = data.resource_id.substring("RECORD_".length);
      } else {
        resourceType = "Unknown";
        resourceIdentifier = data.resource_id;
      }
      updateFields.push("resource_type = ?", "resource_identifier = ?");
      updateParams.push(resourceType, resourceIdentifier);
    }

    if (data.granted_to !== undefined) {
      const { type: granteeType, id: granteeUUID } = parseGranteeIDForDb(
        data.granted_to
      );
      updateFields.push("grantee_type = ?", "grantee_id = ?");
      updateParams.push(granteeType, granteeUUID);
    }
    if (data.begin_date_ms !== undefined) {
      updateFields.push("begin_date_ms = ?");
      updateParams.push(data.begin_date_ms);
    }
    if (data.expiry_date_ms !== undefined) {
      updateFields.push("expiry_date_ms = ?");
      updateParams.push(data.expiry_date_ms);
    }
    if (data.note !== undefined) {
      updateFields.push("note = ?");
      updateParams.push(data.note);
    }
    if (data.metadata !== undefined) {
      updateFields.push("metadata_type = ?", "metadata_content = ?");
      updateParams.push(metadataType, metadataContent);
    }
    if (data.external_id !== undefined) {
      updateFields.push("external_id = ?");
      updateParams.push(data.external_id);
    }
    if (data.external_payload !== undefined) {
      updateFields.push("external_payload = ?");
      updateParams.push(data.external_payload);
    }
    if (data.redeem_code !== undefined) {
      updateFields.push("redeem_code = ?");
      updateParams.push(data.redeem_code);
    }

    updateFields.push("last_modified_at = ?");
    updateParams.push(currentTime);

    if (updateFields.length > 0) {
      const updateQuery = `
        UPDATE permissions_system
        SET ${updateFields.join(", ")}
        WHERE id = ?
      `;
      tx.prepare(updateQuery).run(...updateParams, data.id);
    }

    if (data.permission_types !== undefined) {
      tx.prepare(
        `DELETE FROM permissions_system_types WHERE permission_id = ?`
      ).run(data.id);
      const insertPermTypeStmt = tx.prepare(`
        INSERT INTO permissions_system_types (permission_id, permission_type) VALUES (?, ?)
      `);
      for (const type of data.permission_types) {
        insertPermTypeStmt.run(data.id, type);
      }
    }

    const updatedPermission = await getSystemPermissionById(
      orgId,
      data.id,
      requesterId,
      true
    );
    return updatedPermission;
  });

  if (!result) {
    return null;
  }
  return castToSystemPermissionFE(result, requesterId, orgId);
}

/**
 * Deletes a system permission.
 * @param orgId The drive ID.
 * @param permissionId The ID of the permission to delete.
 * @param requesterId The user performing the deletion.
 * @returns The ID of the deleted permission, or null if not found/deleted.
 */
export async function deleteSystemPermission(
  orgId: string,
  permissionId: string,
  requesterId: UserID
): Promise<string | null> {
  return dbHelpers.transaction("drive", orgId, (tx) => {
    tx.prepare(
      `DELETE FROM permissions_system_types WHERE permission_id = ?`
    ).run(permissionId);
    tx.prepare(
      `DELETE FROM permission_system_labels WHERE permission_id = ?`
    ).run(permissionId);

    const deleteResult = tx
      .prepare(`DELETE FROM permissions_system WHERE id = ?`)
      .run(permissionId);

    if (deleteResult.changes && deleteResult.changes > 0) {
      return permissionId;
    }
    return null;
  });
}

/**
 * Redeems a placeholder system permission, converting it to a user-specific permission.
 * @param orgId The drive ID.
 * @param data Redemption details.
 * @returns The redeemed SystemPermissionFE or an error.
 */
export async function redeemSystemPermission(
  orgId: string,
  data: {
    permission_id: SystemPermission["id"];
    user_id: UserID;
    redeem_code: string;
    note?: string;
    requesterId: UserID;
  }
): Promise<{ permission?: any; error?: string }> {
  // SystemPermissionFE
  const currentTime = Date.now();

  const existingPermission = await getSystemPermissionById(
    orgId,
    data.permission_id,
    data.requesterId,
    true
  );

  if (!existingPermission) {
    return { error: "Permission not found" };
  }

  if (
    !existingPermission.granted_to.startsWith(
      PLACEHOLDER_DIRECTORY_PERMISSION_GRANTEE_ID_PREFIX
    )
  ) {
    // Using same placeholder prefix
    return { error: "Permission is not a redeemable placeholder" };
  }
  if (
    !existingPermission.redeem_code ||
    existingPermission.redeem_code !== data.redeem_code
  ) {
    return { error: "Invalid redeem code" };
  }

  if (
    existingPermission.expiry_date_ms > 0 &&
    existingPermission.expiry_date_ms <= currentTime
  ) {
    return { error: "Permission has expired" };
  }
  if (
    existingPermission.begin_date_ms > 0 &&
    existingPermission.begin_date_ms > currentTime
  ) {
    return { error: "Permission is not yet active" };
  }

  return dbHelpers.transaction("drive", orgId, async (tx) => {
    const { type: newGranteeType, id: newGranteeId } = parseGranteeIDForDb(
      data.user_id
    );

    tx.prepare(
      `
      UPDATE permissions_system
      SET
        grantee_type = ?,
        grantee_id = ?,
        redeem_code = NULL,
        from_placeholder_grantee = ?,
        note = ?,
        last_modified_at = ?
      WHERE id = ?
    `
    ).run(
      newGranteeType,
      newGranteeId,
      existingPermission.granted_to, // Store original placeholder as from_placeholder_grantee
      data.note || existingPermission.note,
      currentTime,
      data.permission_id
    );

    const updatedPermission = await getSystemPermissionById(
      orgId,
      data.permission_id,
      data.requesterId,
      true
    );

    if (!updatedPermission) {
      return { error: "Failed to redeem permission: internal error." };
    }

    return {
      permission: await castToSystemPermissionFE(
        updatedPermission,
        data.requesterId,
        orgId
      ),
    };
  });
}

// --- Fastify Route Handlers ---

// Utility to get requester ID (replace with actual auth logic)
function getRequesterId(request: FastifyRequest): UserID {
  // IMPORTANT: Replace this with actual user ID extraction from your authentication system.
  // For example: `(request as any).user.id` if using a Fastify auth plugin.
  return "mock_requester_id";
}

// Directory Permissions Handlers

export async function getDirectoryPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string; directory_permission_id: string };
  }>,
  reply: FastifyReply
) {
  const { org_id, directory_permission_id } = request.params;
  const requesterId: UserID = getRequesterId(request);

  try {
    const permission = await getDirectoryPermissionById(
      org_id,
      directory_permission_id,
      requesterId
    );
    if (permission) {
      const permissionFE = await castToDirectoryPermissionFE(
        permission,
        requesterId,
        org_id
      );
      reply.send({
        ok: { data: permissionFE } as IResponseGetDirectoryPermission["ok"],
      });
    } else {
      reply.status(404).send({
        err: { code: 404, message: "Directory permission not found." },
      });
    }
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function listDirectoryPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestListDirectoryPermissions;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const { filters, page_size, direction, cursor } = request.body;
  const requesterId: UserID = getRequesterId(request);

  try {
    if (!filters?.resource_id) {
      reply.status(400).send({
        err: {
          code: 400,
          message:
            "resource_id is required in filters for listing directory permissions.",
        },
      });
      return;
    }

    const result = await listDirectoryPermissionsForResource({
      orgId: org_id,
      resourceId: filters.resource_id,
      requesterId,
      pageSize: page_size || 10,
      direction: direction || SortDirection.ASC,
      cursor,
    });

    if (!result.authorized) {
      reply.status(403).send({
        err: {
          code: 403,
          message: "Unauthorized to list permissions for this resource.",
        },
      });
      return;
    }

    reply.send({
      ok: {
        data: {
          items: result.items,
          page_size: page_size || 10,
          total: result.total,
          cursor: result.newCursor,
        },
      } as IResponseListDirectoryPermissions["ok"],
    });
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function createDirectoryPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestCreateDirectoryPermission;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const data = request.body;
  const requesterId: UserID = getRequesterId(request);

  try {
    const createdPermissionFE = await createDirectoryPermission(
      org_id,
      data,
      requesterId
    );
    reply.status(201).send({
      ok: {
        data: createdPermissionFE,
      } as IResponseCreateDirectoryPermission["ok"],
    });
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function updateDirectoryPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestUpdateDirectoryPermission;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const data = request.body;
  const requesterId: UserID = getRequesterId(request);

  try {
    const updatedPermissionFE = await updateDirectoryPermission(
      org_id,
      data,
      requesterId
    );
    if (updatedPermissionFE) {
      reply.send({
        ok: {
          data: updatedPermissionFE,
        } as IResponseUpdateDirectoryPermission["ok"],
      });
    } else {
      reply.status(404).send({
        err: {
          code: 404,
          message: "Directory permission not found or failed to update.",
        },
      });
    }
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function deleteDirectoryPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestDeleteDirectoryPermission;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const { permission_id } = request.body;
  const requesterId: UserID = getRequesterId(request);

  try {
    const deletedId = await deleteDirectoryPermission(
      org_id,
      permission_id,
      requesterId
    );
    if (deletedId) {
      reply.send({
        ok: {
          data: { deleted_id: deletedId },
        } as IResponseDeleteDirectoryPermission["ok"],
      });
    } else {
      reply.status(404).send({
        err: {
          code: 404,
          message: "Directory permission not found or failed to delete.",
        },
      });
    }
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function checkDirectoryPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestCheckDirectoryPermissions;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const { resource_id, grantee_id } = request.body;

  try {
    if (!resource_id || !grantee_id) {
      reply.status(400).send({
        err: {
          code: 400,
          message: "resource_id and grantee_id are required.",
        },
      });
      return;
    }

    const permissions = await checkDirectoryPermissions(
      resource_id,
      grantee_id,
      org_id
    );
    reply.send({
      ok: {
        data: { resource_id, grantee_id, permissions },
      } as IResponseCheckDirectoryPermissions["ok"],
    });
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function redeemDirectoryPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestRedeemDirectoryPermission;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const data = request.body;
  const requesterId: UserID = getRequesterId(request);

  try {
    const result = await redeemDirectoryPermission(org_id, {
      ...data,
      requesterId,
    });
    if (result.permission) {
      reply.send({
        ok: {
          data: { permission: result.permission },
        } as IResponseRedeemDirectoryPermission["ok"],
      });
    } else {
      reply.status(400).send({
        err: {
          code: 400,
          message: result.error || "Failed to redeem directory permission.",
        },
      });
    }
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

// System Permissions Handlers

export async function getSystemPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string; system_permission_id: string };
  }>,
  reply: FastifyReply
) {
  const { org_id, system_permission_id } = request.params;
  const requesterId: UserID = getRequesterId(request);

  try {
    const permission = await getSystemPermissionById(
      org_id,
      system_permission_id,
      requesterId
    );
    if (permission) {
      const permissionFE = await castToSystemPermissionFE(
        permission,
        requesterId,
        org_id
      );
      reply.send({
        ok: { data: permissionFE } as IResponseGetSystemPermission["ok"],
      });
    } else {
      reply
        .status(404)
        .send({ err: { code: 404, message: "System permission not found." } });
    }
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function listSystemPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestListSystemPermissions;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const { filters, page_size, direction, cursor } = request.body;
  const requesterId: UserID = getRequesterId(request);

  try {
    const result = await listSystemPermissions({
      orgId: org_id,
      filters,
      requesterId,
      pageSize: page_size || 10,
      direction: direction || SortDirection.ASC,
      cursor,
    });

    if (!result.authorized) {
      reply.status(403).send({
        err: {
          code: 403,
          message: "Unauthorized to list system permissions.",
        },
      });
      return;
    }

    reply.send({
      ok: {
        data: {
          items: result.items,
          page_size: page_size || 10,
          total: result.total,
          cursor: result.newCursor,
        },
      } as IResponseListSystemPermissions["ok"],
    });
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function createSystemPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestCreateSystemPermission;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const data = request.body;
  const requesterId: UserID = getRequesterId(request);

  try {
    const createdPermissionFE = await createSystemPermission(
      org_id,
      data,
      requesterId
    );
    reply.status(201).send({
      ok: {
        data: createdPermissionFE,
      } as IResponseCreateSystemPermission["ok"],
    });
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function updateSystemPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestUpdateSystemPermission;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const data = request.body;
  const requesterId: UserID = getRequesterId(request);

  try {
    const updatedPermissionFE = await updateSystemPermission(
      org_id,
      data,
      requesterId
    );
    if (updatedPermissionFE) {
      reply.send({
        ok: {
          data: updatedPermissionFE,
        } as IResponseUpdateSystemPermission["ok"],
      });
    } else {
      reply.status(404).send({
        err: {
          code: 404,
          message: "System permission not found or failed to update.",
        },
      });
    }
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function deleteSystemPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestDeleteSystemPermission;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const { permission_id } = request.body;
  const requesterId: UserID = getRequesterId(request);

  try {
    const deletedId = await deleteSystemPermission(
      org_id,
      permission_id,
      requesterId
    );
    if (deletedId) {
      reply.send({
        ok: {
          data: { deleted_id: deletedId },
        } as IResponseDeleteSystemPermission["ok"],
      });
    } else {
      reply.status(404).send({
        err: {
          code: 404,
          message: "System permission not found or failed to delete.",
        },
      });
    }
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function checkSystemPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestCheckSystemPermissions;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const { resource_id, grantee_id } = request.body;
  const requesterId: UserID = getRequesterId(request); // Not strictly needed for check, but good practice

  try {
    if (!resource_id || !grantee_id) {
      reply.status(400).send({
        err: {
          code: 400,
          message: "resource_id and grantee_id are required.",
        },
      });
      return;
    }

    const permissions = await getSystemPermissionsForRecord(
      resource_id,
      grantee_id,
      org_id
    );
    reply.send({
      ok: {
        data: { resource_id, grantee_id, permissions },
      } as IResponseCheckSystemPermissions["ok"],
    });
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function redeemSystemPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestRedeemSystemPermission;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const data = request.body;
  const requesterId: UserID = getRequesterId(request);

  try {
    const result = await redeemSystemPermission(org_id, {
      ...data,
      requesterId,
    });
    if (result.permission) {
      reply.send({
        ok: {
          data: result.permission,
        } as IResponseRedeemSystemPermission["ok"],
      });
    } else {
      reply.status(400).send({
        err: {
          code: 400,
          message: result.error || "Failed to redeem system permission.",
        },
      });
    }
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}
