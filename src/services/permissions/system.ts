// src/services/permissions/system.ts

import {
  UserID,
  GranteeID,
  SystemPermission,
  SystemPermissionType,
  SystemResourceID as RustLikeSystemResourceID, // Renamed to avoid conflict with local SystemResourceID
  SystemTableValueEnum, // Assuming this maps directly to SystemTableEnum
  PermissionMetadata,
  PermissionMetadataTypeEnum,
  PermissionMetadataContent,
  GroupID, // For group checks
} from "@officexapp/types";
import { db } from "../../services/database";
import { getDriveOwnerId } from "../../routes/v1/types";
import { isUserInGroup } from "../groups";
import {
  parsePermissionGranteeIDString, // Use the shared parser from directory.ts
  PUBLIC_GRANTEE_ID_STRING,
  USER_ID_PREFIX,
  GROUP_ID_PREFIX,
  PLACEHOLDER_DIRECTORY_PERMISSION_GRANTEE_ID_PREFIX,
} from "./directory"; // Import constants and parser from directory service

// Helper to parse SystemResourceID from string.
// This matches the Rust SystemResourceID enum's Display implementation:
// SystemResourceID::Table -> "TABLE_TABLE_NAME"
// SystemResourceID::Record -> "RECORD_RECORD_TYPE_ID" (e.g., "RECORD_DriveID_xyz", "RECORD_UserID_abc")
function parseSystemResourceIDString(idStr: string): {
  type: "Table" | "Record";
  value: string;
} {
  if (idStr.startsWith("TABLE_")) {
    return { type: "Table", value: idStr.substring("TABLE_".length) };
  } else if (idStr.startsWith("RECORD_")) {
    return { type: "Record", value: idStr.substring("RECORD_".length) };
  }
  // Fallback for unexpected formats, adjust as needed for error handling
  throw new Error(`Invalid SystemResourceID format: ${idStr}`);
}

// Check if a user can CRUD the permission record
export async function canUserAccessSystemPermission(
  requesterUserId: UserID,
  permission: SystemPermission,
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

  // Rust's `parse_permission_grantee_id` handles the string parsing.
  // In TS, `permission.granted_to` is already the string representation.
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

export async function hasSystemManagePermission(
  userId: UserID,
  resourceId: RustLikeSystemResourceID,
  orgId: string
): Promise<boolean> {
  // Use our existing checkSystemPermissions which already handles group membership etc.
  const permissions = await checkSystemPermissions(resourceId, userId, orgId);
  // Rust uses SystemPermissionType::Invite for has_system_manage_permission
  return permissions.includes(SystemPermissionType.INVITE);
}

// check what kind of permission a specific user has on a specific resource
export async function checkSystemPermissions(
  resourceId: RustLikeSystemResourceID,
  granteeId: GranteeID,
  orgId: string
): Promise<SystemPermissionType[]> {
  const allPermissions = new Set<SystemPermissionType>();

  // Check if the grantee is the owner of the drive
  const isOwner =
    (await getDriveOwnerId(orgId)) ===
    (granteeId.startsWith(USER_ID_PREFIX) ? granteeId : "");
  if (isOwner) {
    // Owner gets all system permissions
    return [
      SystemPermissionType.CREATE,
      SystemPermissionType.VIEW,
      SystemPermissionType.EDIT,
      SystemPermissionType.DELETE,
      SystemPermissionType.INVITE,
    ];
  }

  // First, check direct permissions for the grantee
  // Rust calls `check_system_resource_permissions` with `resource_id` and `grantee_id`.
  const directPermissions = await checkSystemResourcePermissions(
    resourceId,
    granteeId,
    orgId
  );
  directPermissions.forEach((p) => allPermissions.add(p));

  // Always check public permissions (for any grantee type)
  // Rust explicitly checks `PermissionGranteeID::Public`
  const publicPermissions = await checkSystemResourcePermissions(
    resourceId,
    PUBLIC_GRANTEE_ID_STRING,
    orgId
  );
  publicPermissions.forEach((p) => allPermissions.add(p));

  // If the grantee is a user, also check group permissions
  // Rust checks `PermissionGranteeID::User` and then iterates through `GROUPS_BY_TIME_LIST`
  // and `INVITES_BY_ID_HASHTABLE` to determine group membership.
  if (granteeId.startsWith(USER_ID_PREFIX)) {
    const userId = granteeId as UserID;
    // Fetch all groups from the DB (Rust iterates GROUPS_BY_TIME_LIST)
    const allGroups = await db.queryDrive(
      orgId,
      `SELECT id, name, owner_user_id, avatar, private_note, public_note, created_at, last_modified_at, drive_id, endpoint_url, external_id, external_payload FROM groups`
    );

    for (const groupRow of allGroups) {
      // Create a simplified Group object for `isUserOnLocalGroup`
      const group = {
        id: groupRow.id,
        name: groupRow.name,
        owner: groupRow.owner_user_id,
        avatar: groupRow.avatar,
        private_note: groupRow.private_note,
        public_note: groupRow.public_note,
        admin_invites: [], // Not needed for isUserOnLocalGroup check in Rust
        member_invites: [], // Not needed for isUserOnLocalGroup check in Rust
        created_at: groupRow.created_at,
        last_modified_at: groupRow.last_modified_at,
        drive_id: groupRow.drive_id,
        endpoint_url: groupRow.endpoint_url,
        labels: [], // Not needed for isUserOnLocalGroup check in Rust
        external_id: groupRow.external_id,
        external_payload: groupRow.external_payload,
      };

      // `isUserOnLocalGroup` in Rust directly accesses `INVITES_BY_ID_HASHTABLE` and `USERS_INVITES_LIST_HASHTABLE`.
      // We need to implement similar logic in TS using SQL queries.
      // The `isUserInGroup` in `services/groups.ts` already handles this correctly by querying DB.
      if (await isUserInGroup(userId, group.id as GroupID, orgId)) {
        // Add this group's permissions
        const groupPermissions = await checkSystemResourcePermissions(
          resourceId,
          group.id as GroupID, // Pass GroupID string as grantee
          orgId
        );
        groupPermissions.forEach((p) => allPermissions.add(p));
      }
    }
  }

  return Array.from(allPermissions);
}

async function checkSystemResourcePermissions(
  resourceId: RustLikeSystemResourceID, // This is the string representation like "TABLE_DRIVES" or "RECORD_UserID_abc"
  granteeId: GranteeID, // This is the string representation like "PUBLIC", "USER_...", "GROUP_..."
  orgId: string
): Promise<SystemPermissionType[]> {
  const permissionsSet = new Set<SystemPermissionType>();
  const currentTime = Date.now();

  // Parse the resourceId string back into its type and identifier for the SQL query
  const parsedResourceId = parseSystemResourceIDString(resourceId);

  // The SQL query needs to match the resource_type and resource_identifier
  const rows = await db.queryDrive(
    orgId,
    `SELECT
        ps.id,
        ps.resource_type,
        ps.resource_identifier,
        ps.grantee_type,
        ps.grantee_id,
        ps.granted_by_user_id,
        GROUP_CONCAT(pst.permission_type) AS permission_types_list,
        ps.begin_date_ms,
        ps.expiry_date_ms,
        ps.note,
        ps.created_at,
        ps.last_modified_at,
        ps.redeem_code,
        ps.from_placeholder_grantee,
        ps.metadata_type,
        ps.metadata_content,
        ps.external_id,
        ps.external_payload
      FROM permissions_system ps
      JOIN permissions_system_types pst ON ps.id = pst.permission_id
      WHERE ps.resource_type = ? AND ps.resource_identifier = ?
      GROUP BY ps.id`,
    [parsedResourceId.type, parsedResourceId.value]
  );

  for (const row of rows) {
    // Reconstruct SystemPermission object from the row
    const permission: SystemPermission = {
      id: row.id,
      resource_id: resourceId, // Keep the original string as it's the `SystemResourceID` type
      granted_to:
        row.grantee_type === "Public"
          ? PUBLIC_GRANTEE_ID_STRING
          : `${row.grantee_type.toUpperCase()}_${row.grantee_id}`,
      granted_by: row.granted_by_user_id,
      permission_types: (row.permission_types_list || "")
        .split(",")
        .filter(Boolean)
        .map((typeStr: string) => typeStr.trim() as SystemPermissionType),
      begin_date_ms: row.begin_date_ms,
      expiry_date_ms: row.expiry_date_ms,
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
      labels: [], // Not fetched by this query directly
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

    // Check if permission applies to this grantee
    let applies = false;
    const permissionGrantedTo = permission.granted_to; // Already string format

    // Direct comparison of string representations matching Rust's logic
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

// This is a helper function specifically for checking permissions table access
export async function checkPermissionsTableAccess(
  userId: UserID,
  requiredPermission: SystemPermissionType,
  isOwner: boolean,
  orgId: string
): Promise<boolean> {
  if (isOwner) {
    return true;
  }

  const permissions = await checkSystemPermissions(
    `TABLE_${SystemTableValueEnum.PERMISSIONS}` as RustLikeSystemResourceID,
    userId,
    orgId
  );
  return permissions.includes(requiredPermission);
}

export async function checkSystemResourcePermissionsLabels(
  resourceId: RustLikeSystemResourceID,
  granteeId: GranteeID,
  labelStringValue: string,
  orgId: string
): Promise<SystemPermissionType[]> {
  const permissionsSet = new Set<SystemPermissionType>();

  // Get direct permissions
  const directPermissions = await checkSystemResourcePermissionsLabelsInternal(
    resourceId,
    granteeId,
    labelStringValue,
    orgId
  );
  directPermissions.forEach((p) => permissionsSet.add(p));

  // Always check public permissions
  const publicPermissions = await checkSystemResourcePermissionsLabelsInternal(
    resourceId,
    PUBLIC_GRANTEE_ID_STRING,
    labelStringValue,
    orgId
  );
  publicPermissions.forEach((p) => permissionsSet.add(p));

  // If the grantee is a user, also check group permissions
  if (granteeId.startsWith(USER_ID_PREFIX)) {
    const userId = granteeId as UserID;
    const allGroups = await db.queryDrive(
      orgId,
      `SELECT id, name, owner_user_id, avatar, private_note, public_note, created_at, last_modified_at, drive_id, endpoint_url, external_id, external_payload FROM groups`
    );

    for (const groupRow of allGroups) {
      const group = {
        id: groupRow.id,
        name: groupRow.name,
        owner: groupRow.owner_user_id,
        avatar: groupRow.avatar,
        private_note: groupRow.private_note,
        public_note: groupRow.public_note,
        admin_invites: [],
        member_invites: [],
        created_at: groupRow.created_at,
        last_modified_at: groupRow.last_modified_at,
        drive_id: groupRow.drive_id,
        endpoint_url: groupRow.endpoint_url,
        labels: [],
        external_id: groupRow.external_id,
        external_payload: groupRow.external_payload,
      };

      if (await isUserInGroup(userId, group.id as GroupID, orgId)) {
        // Add this group's permissions
        const groupPermissions =
          await checkSystemResourcePermissionsLabelsInternal(
            resourceId,
            group.id as GroupID,
            labelStringValue,
            orgId
          );
        groupPermissions.forEach((p) => permissionsSet.add(p));
      }
    }
  }

  return Array.from(permissionsSet);
}

async function checkSystemResourcePermissionsLabelsInternal(
  resourceId: RustLikeSystemResourceID,
  granteeId: GranteeID,
  labelStringValue: string,
  orgId: string
): Promise<SystemPermissionType[]> {
  const permissionsSet = new Set<SystemPermissionType>();
  const currentTime = Date.now();

  const parsedResourceId = parseSystemResourceIDString(resourceId);

  const rows = await db.queryDrive(
    orgId,
    `SELECT
        ps.id,
        ps.resource_type,
        ps.resource_identifier,
        ps.grantee_type,
        ps.grantee_id,
        ps.granted_by_user_id,
        GROUP_CONCAT(pst.permission_type) AS permission_types_list,
        ps.begin_date_ms,
        ps.expiry_date_ms,
        ps.note,
        ps.created_at,
        ps.last_modified_at,
        ps.redeem_code,
        ps.from_placeholder_grantee,
        ps.metadata_type,
        ps.metadata_content,
        ps.external_id,
        ps.external_payload
      FROM permissions_system ps
      JOIN permissions_system_types pst ON ps.id = pst.permission_id
      WHERE ps.resource_type = ? AND ps.resource_identifier = ?
      GROUP BY ps.id`,
    [parsedResourceId.type, parsedResourceId.value]
  );

  for (const row of rows) {
    const permission: SystemPermission = {
      id: row.id,
      resource_id: resourceId,
      granted_to:
        row.grantee_type === "Public"
          ? PUBLIC_GRANTEE_ID_STRING
          : `${row.grantee_type.toUpperCase()}_${row.grantee_id}`,
      granted_by: row.granted_by_user_id,
      permission_types: (row.permission_types_list || "")
        .split(",")
        .filter(Boolean)
        .map((typeStr: string) => typeStr.trim() as SystemPermissionType),
      begin_date_ms: row.begin_date_ms,
      expiry_date_ms: row.expiry_date_ms,
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
      labels: [],
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
      // Check for label metadata match
      let labelMatch = true; // Assume true if no label metadata exists
      if (
        permission.metadata &&
        permission.metadata.metadata_type === PermissionMetadataTypeEnum.LABELS
      ) {
        if ("Labels" in permission.metadata.content) {
          const prefix = (permission.metadata.content as { Labels: string })
            .Labels;
          labelMatch = labelStringValue
            .toLowerCase()
            .startsWith(prefix.toLowerCase());
        }
      }

      if (labelMatch) {
        permission.permission_types.forEach((type) => permissionsSet.add(type));
      }
    }
  }

  return Array.from(permissionsSet);
}
