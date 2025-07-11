// src/services/permissions/directory.ts

import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import {
  DirectoryPermission,
  DirectoryPermissionFE,
  DirectoryPermissionType,
  IDPrefixEnum,
  DirectoryResourceID,
  IRequestCheckDirectoryPermissions,
  IRequestCreateDirectoryPermission,
  IRequestDeleteDirectoryPermission,
  IRequestListDirectoryPermissions,
  IRequestRedeemDirectoryPermission,
  IRequestUpdateDirectoryPermission,
  IPaginatedResponse,
  IResponseCheckDirectoryPermissions,
  IResponseCreateDirectoryPermission,
  IResponseDeleteDirectoryPermission,
  IResponseListDirectoryPermissions,
  IResponseRedeemDirectoryPermission,
  IResponseUpdateDirectoryPermission,
  UserID,
  GranteeID,
  FolderID,
  FileID,
  DirectoryPermissionID,
  SystemPermissionType,
  SystemResourceID,
  PermissionMetadata,
  PermissionMetadataTypeEnum,
  PermissionMetadataContent,
  DriveFullFilePath,
  DriveClippedFilePath,
  LabelValue,
  GroupID,
  FilePathBreadcrumb,
} from "@officexapp/types";
import { db, dbHelpers } from "../../services/database"; // Assuming this path
import { authenticateRequest } from "../../services/auth"; // Assuming this path
import { isUserInGroup, isGroupAdmin } from "../../services/groups"; // Assuming a new groups service
import { getFolderMetadata, getFileMetadata } from "../../services/directory"; // Assuming a new directory service
import {
  createApiResponse,
  ErrorResponse,
  OrgIdParams,
  validateIdString,
  validateDescription,
  validateExternalId,
  validateExternalPayload,
} from "../utils"; // Assuming this utility file exists
import { checkSystemPermissions } from "./system"; // Import checkSystemPermissions from system service
import { redactLabel } from "../../services/labels"; // Assuming a labels service for redaction

// Constants
const PUBLIC_GRANTEE_ID = "PUBLIC";

// Helper function to get current time in milliseconds
function getCurrentTimeMs(): number {
  return Date.now();
}

// TODO: Replace with actual owner ID retrieval from drive context or config
async function getOwnerId(orgId: string): Promise<UserID> {
  // This is a placeholder. In a real scenario, the drive owner ID should be
  // fetched from the `about_drive` table for the given `orgId`.
  // For now, returning a mock owner.
  try {
    const result = await db.queryDrive(
      orgId,
      `SELECT owner_id FROM about_drive LIMIT 1`
    );
    if (result.length > 0) {
      return result[0].owner_id as UserID;
    }
  } catch (error) {
    console.error(`Error fetching owner_id for drive ${orgId}:`, error);
  }
  return "UserID_mock_owner"; // sensible placeholder
}

// Helper to parse DirectoryResourceID
function parseDirectoryResourceID(
  idStr: string
): DirectoryResourceID | undefined {
  if (idStr.startsWith(IDPrefixEnum.File)) {
    return idStr as DirectoryResourceID;
  } else if (idStr.startsWith(IDPrefixEnum.Folder)) {
    return idStr as DirectoryResourceID;
  }
  return undefined;
}

// Helper to parse PermissionGranteeID
export function parsePermissionGranteeID(idStr: string): GranteeID | undefined {
  if (idStr === PUBLIC_GRANTEE_ID) {
    return "Public";
  } else if (idStr.startsWith(IDPrefixEnum.User)) {
    return idStr as UserID;
  } else if (idStr.startsWith(IDPrefixEnum.Group)) {
    return idStr as GroupID;
  } else if (idStr.startsWith(IDPrefixEnum.PlaceholderPermissionGrantee)) {
    return `PlaceholderDirectoryPermissionGrantee_${idStr.substring(IDPrefixEnum.PlaceholderPermissionGrantee.length)}`;
  }
  return undefined;
}

// Utility to convert raw DB row to DirectoryPermission
function mapDbRowToDirectoryPermission(row: any): DirectoryPermission {
  return {
    id: row.id,
    resource_id: row.resource_id,
    resource_path: row.resource_path,
    granted_to:
      parsePermissionGranteeID(
        row.grantee_type === "Public" ? PUBLIC_GRANTEE_ID : row.grantee_id
      ) || "Public", // TODO: Handle actual grantee_id for User/Group/Placeholder
    granted_by: row.granted_by_user_id,
    permission_types: JSON.parse(row.permission_types_json), // Assuming permission_types are stored as JSON string
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
            metadata_type: row.metadata_type as PermissionMetadataTypeEnum,
            content: JSON.parse(row.metadata_content), // Assuming JSON string
          }
        : undefined,
    labels: JSON.parse(row.labels_json || "[]"), // Assuming labels are stored as JSON string
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
  // TODO: Implement actual logic for resource_name, grantee_name, grantee_avatar, granter_name
  // These would typically involve lookups in other tables (e.g., files, folders, contacts, groups)
  // For now, providing placeholders or basic derivations.

  const isOwner = (await getOwnerId(orgId)) === currentUserId;

  // Get resource_name
  let resourceName: string | undefined;
  if (permission.resource_id.startsWith(IDPrefixEnum.File)) {
    const fileMetadata = await getFileMetadata(
      orgId,
      permission.resource_id as FileID
    );
    resourceName = fileMetadata?.name;
  } else if (permission.resource_id.startsWith(IDPrefixEnum.Folder)) {
    const folderMetadata = await getFolderMetadata(
      orgId,
      permission.resource_id as FolderID
    );
    resourceName = folderMetadata?.name;
  }

  // Get grantee_name and grantee_avatar
  let granteeName: string | undefined;
  let granteeAvatar: string | undefined;
  if (typeof permission.granted_to === "string") {
    // Public case or unparsed placeholder
    if (permission.granted_to === PUBLIC_GRANTEE_ID) {
      granteeName = "PUBLIC";
    } else if (
      permission.granted_to.startsWith("PlaceholderDirectoryPermissionGrantee_")
    ) {
      granteeName = "Awaiting Anon";
    } else {
      // Attempt to fetch from Contacts or Groups if it's a UserID or GroupID string
      // This is a simplified check, full implementation would query your contacts/groups service
      if (permission.granted_to.startsWith(IDPrefixEnum.User)) {
        // TODO: Fetch user name and avatar from contacts table
        granteeName = "User: " + permission.granted_to;
        granteeAvatar = undefined;
      } else if (permission.granted_to.startsWith(IDPrefixEnum.Group)) {
        // TODO: Fetch group name and avatar from groups table
        granteeName = "Group: " + permission.granted_to;
        granteeAvatar = undefined;
      }
    }
  } else if (typeof permission.granted_to === "object") {
    // Handle discriminated union if it were properly typed
    if ("User" in permission.granted_to) {
      // TODO: Fetch user name and avatar from contacts table
      granteeName = "User: " + permission.granted_to;
      granteeAvatar = undefined;
    } else if ("Group" in permission.granted_to) {
      // TODO: Fetch group name and avatar from groups table
      granteeName = "Group: " + permission.granted_to;
      granteeAvatar = undefined;
    }
  }

  // Get granter_name
  // TODO: Fetch granter name from contacts table
  const granterName = "Granter: " + permission.granted_by;

  // Get permission previews for the current user on this permission record
  const recordPermissions = await checkSystemPermissions(
    {
      type: "Record",
      value: { type: "Permission", value: permission.id },
    } as SystemResourceID, // TODO: Correctly type SystemResourceID
    currentUserId, // Assuming checkSystemPermissions takes UserID as GranteeID for simplicity
    orgId
  );
  const tablePermissions = await checkSystemPermissions(
    { type: "Table", value: "PERMISSIONS" } as SystemResourceID, // TODO: Correctly type SystemResourceID
    currentUserId,
    orgId
  );
  const permissionPreviews = Array.from(
    new Set([...recordPermissions, ...tablePermissions])
  );

  const castedPermission: DirectoryPermissionFE = {
    id: permission.id,
    resource_id: permission.resource_id,
    resource_path: permission.resource_path, // TODO: Implement clipping logic
    granted_to:
      typeof permission.granted_to === "string"
        ? permission.granted_to
        : (permission.granted_to as any).User ||
          (permission.granted_to as any).Group ||
          (permission.granted_to as any).PlaceholderDirectoryPermissionGrantee, // Simplify for FE representation
    granted_by: permission.granted_by,
    permission_types: permission.permission_types,
    begin_date_ms: permission.begin_date_ms,
    expiry_date_ms: permission.expiry_date_ms,
    inheritable: permission.inheritable,
    note: permission.note,
    created_at: permission.created_at,
    last_modified_at: permission.last_modified_at,
    from_placeholder_grantee: permission.from_placeholder_grantee,
    labels: permission.labels, // TODO: Redact labels based on user permissions
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
    redacted.resource_path = "" as DriveClippedFilePath; // Empty string for clipped path if not owner

    if (!hasEditPermissions) {
      // Further redaction if no edit permissions (e.g., private notes if they existed here)
      // This part depends on what "private_note" would apply to a permission itself
    }
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

  const permissionGrantedTo = parsePermissionGranteeID(
    (permission.granted_to as any).Public ||
      (permission.granted_to as any).User ||
      (permission.granted_to as any).Group ||
      (permission.granted_to as any).PlaceholderDirectoryPermissionGrantee ||
      ""
  ); // Handle direct string 'Public' or other forms

  if (!permissionGrantedTo) {
    return false; // Invalid grantee ID
  }

  // Check if user is the direct grantee
  if (typeof permissionGrantedTo === "string") {
    if (permissionGrantedTo === "Public") {
      return true; // Everyone can see public permissions
    } else if (permissionGrantedTo.startsWith(IDPrefixEnum.User)) {
      if (permissionGrantedTo === requesterUserId) {
        return true;
      }
    } else if (permissionGrantedTo.startsWith(IDPrefixEnum.Group)) {
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
      permissionGrantedTo.startsWith(IDPrefixEnum.PlaceholderPermissionGrantee)
    ) {
      // One-time links can only be accessed by the creator
      return permission.granted_by === requesterUserId;
    }
  }
  // TODO: Handle structured PermissionGranteeID types if they are objects

  return false;
}

// Function to get the list of inherited resources (parents in the directory hierarchy)
async function getInheritedResourcesList(
  resourceId: DirectoryResourceID,
  orgId: string
): Promise<DirectoryResourceID[]> {
  const resources: DirectoryResourceID[] = [];
  let currentFolderId: FolderID | undefined;

  // Add the initial resource itself
  resources.push(resourceId);

  // Determine the starting point for traversal based on resource type
  if (resourceId.startsWith(IDPrefixEnum.File)) {
    const fileId = resourceId as FileID;
    const fileMetadata = await getFileMetadata(orgId, fileId);
    if (!fileMetadata) return []; // File not found

    if (fileMetadata.has_sovereign_permissions) {
      // If file has sovereign permissions, only itself is relevant for inheritance
      return resources;
    }
    currentFolderId = fileMetadata.parent_folder_uuid;
  } else if (resourceId.startsWith(IDPrefixEnum.Folder)) {
    const folderId = resourceId as FolderID;
    const folderMetadata = await getFolderMetadata(orgId, folderId);
    if (!folderMetadata) return []; // Folder not found

    if (folderMetadata.has_sovereign_permissions) {
      // If folder has sovereign permissions, only itself is relevant for inheritance
      return resources;
    }
    currentFolderId = folderMetadata.parent_folder_uuid;
  } else {
    // Invalid resource ID format
    return [];
  }

  // Traverse up through parent folders
  while (currentFolderId) {
    const folderMetadata = await getFolderMetadata(orgId, currentFolderId);
    if (!folderMetadata) break; // Parent folder not found, stop traversal

    resources.push(folderMetadata.id as DirectoryResourceID);

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
  isParentForInheritance: boolean,
  orgId: string
): Promise<DirectoryPermissionType[]> {
  const permissionsSet = new Set<DirectoryPermissionType>();
  const currentTime = getCurrentTimeMs();

  // Fetch permissions for the resource from the database
  // The SQL query joins permissions_directory with permissions_directory_types
  // to get all permission types for each permission record.
  const rows = await db.queryDrive(
    orgId,
    `
      SELECT
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
          pd.external_payload,
          (SELECT GROUP_CONCAT(label_id) FROM permission_directory_labels WHERE permission_id = pd.id) AS labels_list
      FROM permissions_directory pd
      JOIN permissions_directory_types pdt ON pd.id = pdt.permission_id
      WHERE pd.resource_id = ?
      GROUP BY pd.id
    `,
    [resourceId]
  );

  for (const row of rows) {
    const permission: DirectoryPermission = {
      id: row.id,
      resource_id: row.resource_id,
      resource_path: row.resource_path,
      // Reconstruct grantee_id and permission_types from DB data
      granted_to:
        row.grantee_type === "Public" ? "Public" : row.grantee_id || "", // Simplified
      granted_by: row.granted_by_user_id,
      permission_types: row.permission_types_list
        .split(",")
        .map((typeStr: string) => typeStr.trim() as DirectoryPermissionType), // Convert string list back to enum array
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
              content: JSON.parse(row.metadata_content), // Assuming content is JSON string
            }
          : undefined,
      labels: row.labels_list ? row.labels_list.split(",") : [],
      external_id: row.external_id,
      external_payload: row.external_payload,
    };

    // Skip if permission is expired or not yet active
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

    // Skip if permission lacks inheritance and is a parent being checked for inheritance
    if (!permission.inheritable && isParentForInheritance) {
      continue;
    }

    const permissionGrantedTo = parsePermissionGranteeID(
      (permission.granted_to as any).Public ||
        (permission.granted_to as any).User ||
        (permission.granted_to as any).Group ||
        (permission.granted_to as any).PlaceholderDirectoryPermissionGrantee ||
        ""
    );

    let applies = false;
    if (permissionGrantedTo === "Public") {
      applies = true;
    } else if (typeof permissionGrantedTo === "string") {
      if (granteeId === "Public" && permissionGrantedTo === PUBLIC_GRANTEE_ID) {
        applies = true;
      } else if (
        granteeId.startsWith(IDPrefixEnum.User) &&
        permissionGrantedTo.startsWith(IDPrefixEnum.User) &&
        granteeId === permissionGrantedTo
      ) {
        applies = true;
      } else if (
        granteeId.startsWith(IDPrefixEnum.Group) &&
        permissionGrantedTo.startsWith(IDPrefixEnum.Group) &&
        granteeId === permissionGrantedTo
      ) {
        applies = true;
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
        permissionGrantedTo.startsWith(
          IDPrefixEnum.PlaceholderPermissionGrantee
        ) &&
        granteeId === permissionGrantedTo
      ) {
        applies = true;
      }
    }
    // TODO: Handle structured PermissionGranteeID if necessary

    if (applies) {
      permission.permission_types.forEach((type) => permissionsSet.add(type));
    }
  }

  return Array.from(permissionsSet);
}

// check what kind of permission a specific user has on a specific resource
export async function checkDirectoryPermissions(
  resourceId: DirectoryResourceID,
  granteeId: GranteeID,
  orgId: string
): Promise<DirectoryPermissionType[]> {
  const isOwner =
    (await getOwnerId(orgId)) ===
    (typeof granteeId === "string" && granteeId.startsWith(IDPrefixEnum.User)
      ? granteeId
      : "");

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
    const resourcePermissions = await checkDirectoryResourcePermissions(
      resource,
      granteeId,
      resource !== resourceId, // isParentForInheritance is true if it's a parent
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
  return permissions.includes(DirectoryPermissionType.INVITE); // Rust uses Invite for Manage equivalent check
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
    `
    SELECT
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
        pd.external_payload,
        (SELECT GROUP_CONCAT(label_id) FROM permission_directory_labels WHERE permission_id = pd.id) AS labels_list
    FROM permissions_directory pd
    JOIN permissions_directory_types pdt ON pd.id = pdt.permission_id
    WHERE pd.resource_id = ?
    GROUP BY pd.id
    `,
    [resourceId]
  );

  const currentTime = getCurrentTimeMs();

  for (const row of rows) {
    const permission: DirectoryPermission = {
      id: row.id,
      resource_id: row.resource_id,
      resource_path: row.resource_path,
      granted_to:
        row.grantee_type === "Public" ? "Public" : row.grantee_id || "",
      granted_by: row.granted_by_user_id,
      permission_types: row.permission_types_list
        .split(",")
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
              content: JSON.parse(row.metadata_content),
            }
          : undefined,
      labels: row.labels_list ? row.labels_list.split(",") : [],
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

    const permissionGrantedTo = parsePermissionGranteeID(
      (permission.granted_to as any).Public ||
        (permission.granted_to as any).User ||
        (permission.granted_to as any).Group ||
        (permission.granted_to as any).PlaceholderDirectoryPermissionGrantee ||
        ""
    );

    let applies = false;
    if (permissionGrantedTo === "Public") {
      applies = true;
    } else if (typeof permissionGrantedTo === "string") {
      if (
        permissionGrantedTo.startsWith(IDPrefixEnum.User) &&
        permissionGrantedTo === userId
      ) {
        applies = true;
      } else if (permissionGrantedTo.startsWith(IDPrefixEnum.Group)) {
        applies = await isUserInGroup(
          userId,
          permissionGrantedTo as GroupID,
          orgId
        );
      }
    }
    // TODO: Handle structured PermissionGranteeID if necessary

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
): Promise<string[]> {
  let publicCanView = false;
  let publicCanModify = false;
  let privateCanView = false;
  let privateCanModify = false;

  const currentTimeMs = getCurrentTimeMs();

  const rows = await db.queryDrive(
    orgId,
    `
    SELECT
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
        pd.external_payload,
        (SELECT GROUP_CONCAT(label_id) FROM permission_directory_labels WHERE permission_id = pd.id) AS labels_list
    FROM permissions_directory pd
    JOIN permissions_directory_types pdt ON pd.id = pdt.permission_id
    WHERE pd.resource_id = ?
    GROUP BY pd.id
    `,
    [resourceId]
  );

  for (const row of rows) {
    const permission: DirectoryPermission = {
      id: row.id,
      resource_id: row.resource_id,
      resource_path: row.resource_path,
      granted_to:
        row.grantee_type === "Public" ? "Public" : row.grantee_id || "",
      granted_by: row.granted_by_user_id,
      permission_types: row.permission_types_list
        .split(",")
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
              content: JSON.parse(row.metadata_content),
            }
          : undefined,
      labels: row.labels_list ? row.labels_list.split(",") : [],
      external_id: row.external_id,
      external_payload: row.external_payload,
    };

    const isActive =
      (permission.begin_date_ms <= 0 ||
        permission.begin_date_ms <= currentTimeMs) &&
      (permission.expiry_date_ms < 0 ||
        permission.expiry_date_ms > currentTimeMs);

    if (!isActive) {
      continue;
    }

    const hasView = permission.permission_types.includes(
      DirectoryPermissionType.VIEW
    );
    const hasModify =
      permission.permission_types.includes(DirectoryPermissionType.UPLOAD) ||
      permission.permission_types.includes(DirectoryPermissionType.EDIT) ||
      permission.permission_types.includes(DirectoryPermissionType.DELETE) ||
      permission.permission_types.includes(DirectoryPermissionType.MANAGE);

    const permissionGrantedTo = parsePermissionGranteeID(
      (permission.granted_to as any).Public ||
        (permission.granted_to as any).User ||
        (permission.granted_to as any).Group ||
        (permission.granted_to as any).PlaceholderDirectoryPermissionGrantee ||
        ""
    );

    if (permissionGrantedTo === "Public") {
      if (hasView) publicCanView = true;
      if (hasModify) publicCanModify = true;
    } else {
      if (hasView) privateCanView = true;
      if (hasModify) privateCanModify = true;
    }
  }

  const results: string[] = [];
  if (publicCanModify) {
    results.push("PUBLIC_MODIFY");
  } else if (publicCanView) {
    results.push("PUBLIC_VIEW");
  }

  if (privateCanModify) {
    results.push("PRIVATE_MODIFY");
  } else if (privateCanView) {
    results.push("PRIVATE_VIEW");
  }

  return results;
}

export async function deriveDirectoryBreadcrumbs(
  resourceId: DirectoryResourceID,
  userId: UserID,
  orgId: string
): Promise<FilePathBreadcrumb[]> {
  const breadcrumbs: FilePathBreadcrumb[] = [];
  const isOwner = (await getOwnerId(orgId)) === userId;
  let currentResourceId: DirectoryResourceID | undefined = resourceId;

  while (currentResourceId) {
    let resourceName: string | undefined;
    let parentFolderId: FolderID | undefined;
    let hasSovereignPermissions = false;

    if (currentResourceId.startsWith(IDPrefixEnum.File)) {
      const fileMetadata = await getFileMetadata(
        orgId,
        currentResourceId as FileID
      );
      if (!fileMetadata) break;

      resourceName = fileMetadata.name;
      parentFolderId = fileMetadata.parent_folder_uuid;
      hasSovereignPermissions = fileMetadata.has_sovereign_permissions;
    } else if (currentResourceId.startsWith(IDPrefixEnum.Folder)) {
      const folderMetadata = await getFolderMetadata(
        orgId,
        currentResourceId as FolderID
      );
      if (!folderMetadata) break;

      resourceName = folderMetadata.name;
      parentFolderId = folderMetadata.parent_folder_uuid;
      hasSovereignPermissions = folderMetadata.has_sovereign_permissions;

      // Special handling for root folder (disk itself)
      if (
        folderMetadata.full_directory_path === `${folderMetadata.disk_id}::/`
      ) {
        // TODO: Fetch disk name from disks table
        const disk = (
          await db.queryDrive(orgId, "SELECT name FROM disks WHERE id = ?", [
            folderMetadata.disk_id,
          ])
        )[0];
        if (disk) {
          resourceName = disk.name;
        } else {
          resourceName = `Disk: ${folderMetadata.disk_id}`;
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

    breadcrumbs.unshift({
      resource_id: currentResourceId,
      resource_name: resourceName || "Unknown",
      visibility_preview: await deriveBreadcrumbVisibilityPreviews(
        currentResourceId,
        orgId
      ),
    });

    if (hasSovereignPermissions) {
      break; // Stop if sovereign permissions are encountered
    }

    currentResourceId = parentFolderId;
  }

  return breadcrumbs;
}
