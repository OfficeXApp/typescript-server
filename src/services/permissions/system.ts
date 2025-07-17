import {
  UserID,
  GranteeID,
  SystemPermission,
  SystemPermissionType,
  SystemResourceID, // Corrected import
  SystemTableValueEnum,
  PermissionMetadata,
  PermissionMetadataTypeEnum,
  PermissionMetadataContent,
  GroupID,
  IDPrefixEnum,
  DriveID,
  LabelValue, // Import IDPrefixEnum
} from "@officexapp/types";
import { db } from "../../services/database";
import { getDriveOwnerId } from "../../routes/v1/types";
import { isUserInGroup } from "../groups";
import { PUBLIC_GRANTEE_ID_STRING } from "./directory"; // Still using this common constant
import {
  getSystemPermissionsForRecord,
  GROUP_ID_PREFIX,
  PLACEHOLDER_GRANTEE_ID_PREFIX,
  USER_ID_PREFIX,
} from "../../routes/v1/drive/permissions/handlers";

// Helper to extract the plain ID part from a SystemRecordIDEnum string
// This function is generally not needed for DB queries as resource_identifier stores the full prefixed ID for records.
// It might be useful if you need the UUID part for other logic.
function extractPlainSystemRecordId(id: string): string {
  if (id.startsWith(IDPrefixEnum.Drive)) return id;
  if (id.startsWith(IDPrefixEnum.Disk)) return id;
  if (id.startsWith(IDPrefixEnum.User)) return id;
  if (id.startsWith(IDPrefixEnum.Group)) return id;
  if (id.startsWith(IDPrefixEnum.ApiKey)) return id;
  if (id.startsWith(IDPrefixEnum.SystemPermission)) return id;
  if (id.startsWith(IDPrefixEnum.DirectoryPermission)) return id;
  if (id.startsWith(IDPrefixEnum.Webhook)) return id;
  if (id.startsWith(IDPrefixEnum.LabelID)) return id;
  // Assuming "Unknown_" prefix for SystemRecordIDEnum.Unknown in Rust Display
  if (id.startsWith("Unknown_")) return id;
  return id; // Fallback for cases like directly passed UUIDs or if it's already plain
}

// Helper to parse SystemResourceID from string.
// Rust's Display implementation:
// SystemResourceID::Table(SystemTableEnum) => "TABLE_{}"
// SystemResourceID::Record(SystemRecordIDEnum) => "{}" (which is something like DriveID_xyz or UserID_abc)
// This function needs to extract the actual value for SQL queries.
function parseSystemResourceIDString(idStr: string): {
  type: "Table" | "Record";
  value: string; // The actual ID or table enum value without its prefix for DB lookup
} {
  if (idStr.startsWith("TABLE_")) {
    return { type: "Table", value: idStr };
  } else {
    // If it's a Record, the `resource_identifier` in DB is the full prefixed ID.
    // So, we just use the `idStr` directly as the value for the query.
    return { type: "Record", value: idStr };
  }
}

// Utility to convert raw DB row to SystemPermission
export function mapDbRowToSystemPermission(row: any): SystemPermission {
  let grantedTo: GranteeID;
  let granteeIdPart = row.grantee_id; // This is the ID without prefix from DB

  switch (row.grantee_type) {
    case "Public":
      grantedTo = PUBLIC_GRANTEE_ID_STRING;
      break;
    case "User":
      grantedTo = `${granteeIdPart}` as UserID;
      break;
    case "Group":
      grantedTo = `${granteeIdPart}` as GroupID;
      break;
    case "Placeholder":
      grantedTo = `${granteeIdPart}` as GranteeID;
      break;
    default:
      console.warn(
        `Unknown grantee_type: ${row.grantee_type}. Defaulting to Public.`
      );
      grantedTo = PUBLIC_GRANTEE_ID_STRING;
      break;
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
        content = { Labels: "" }; // Fallback
        break;
    }
    metadata = {
      metadata_type: row.metadata_type as PermissionMetadataTypeEnum,
      content: content,
    };
  }

  // Reconstruct resource_id based on resource_type and resource_identifier from DB
  let resourceIdWithPrefix: SystemResourceID;
  if (row.resource_type === "Table") {
    resourceIdWithPrefix =
      `TABLE_${row.resource_identifier}` as SystemResourceID;
  } else if (row.resource_type === "Record") {
    resourceIdWithPrefix = row.resource_identifier as SystemResourceID; // resource_identifier is already the full prefixed ID from DB
  } else {
    throw new Error(`Unknown resource_type from DB: ${row.resource_type}`);
  }

  return {
    id: row.id,
    resource_id: resourceIdWithPrefix,
    granted_to: grantedTo,
    granted_by: `${row.granted_by}` as UserID,
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
    metadata: metadata,
    labels: [], // Labels explicitly skipped as per your request
    external_id: undefined, // External ID explicitly skipped as per your request
    external_payload: undefined, // External Payload explicitly skipped as per your request
  };
}

/**
 * Checks if a user has any permission on a specific system resource.
 * @param resourceId The ID of the SystemResource (Table or Record) to check.
 * @param requesterUserId The ID of the user attempting to access the resource.
 * @param orgId The ID of the organization/drive.
 * @returns A Promise that resolves to true if the user has any permission, false otherwise.
 */
export async function canUserAccessSystemPermission(
  resourceId: SystemResourceID, // Corrected to take SystemResourceID
  requesterUserId: UserID,
  orgId: DriveID
): Promise<boolean> {
  const isOwner = (await getDriveOwnerId(orgId)) === requesterUserId;

  if (isOwner) {
    return true;
  }

  // Leverage the existing checkSystemPermissions function to get all applicable permissions
  const permissions = await checkSystemPermissions(
    resourceId,
    requesterUserId, // Pass the user as the grantee
    orgId
  );

  // If the user has any permission type (i.e., the array is not empty), return true.
  return permissions.length > 0;
}

export async function hasSystemManagePermission(
  userId: UserID,
  resourceId: SystemResourceID,
  orgId: DriveID // Ensure orgId is DriveID
): Promise<boolean> {
  const permissions = await checkSystemPermissions(resourceId, userId, orgId);
  return permissions.includes(SystemPermissionType.INVITE);
}

// check what kind of permission a specific user has on a specific resource
export async function checkSystemPermissions(
  resourceId: SystemResourceID,
  granteeId: GranteeID,
  orgId: DriveID
): Promise<SystemPermissionType[]> {
  const allPermissions = new Set<SystemPermissionType>();
  const isOwner = (await getDriveOwnerId(orgId)) === granteeId;

  if (isOwner) {
    return [
      SystemPermissionType.CREATE,
      SystemPermissionType.VIEW,
      SystemPermissionType.EDIT,
      SystemPermissionType.DELETE,
      SystemPermissionType.INVITE,
    ];
  }

  // Check direct permissions for the grantee
  const directPermissions = await checkSystemResourcePermissions(
    resourceId,
    granteeId,
    orgId
  );
  directPermissions.forEach((p) => allPermissions.add(p));

  // Check public permissions
  const publicPermissions = await checkSystemResourcePermissions(
    resourceId,
    "PUBLIC",
    orgId
  );
  publicPermissions.forEach((p) => allPermissions.add(p));

  // If the grantee is a user, check their group permissions
  if (granteeId.startsWith(IDPrefixEnum.User)) {
    const userId = granteeId as UserID;

    // *** CORRECTED LOGIC ***
    // Get all groups the user is a member of via group_invites
    const currentTime = Date.now();
    const userGroupsQuery = `
        SELECT DISTINCT group_id FROM group_invites
        WHERE invitee_id = ?
        AND invitee_type = 'USER'
        AND active_from <= ?
        AND (expires_at <= 0 OR expires_at > ?)
    `;
    const userGroupsRows = await db.queryDrive(orgId, userGroupsQuery, [
      userId,
      currentTime,
      currentTime,
    ]);

    for (const row of userGroupsRows) {
      const groupId = row.group_id as GroupID;
      const groupPermissions = await checkSystemResourcePermissions(
        resourceId,
        groupId, // Check permissions for the group
        orgId
      );
      groupPermissions.forEach((p) => allPermissions.add(p));
    }
  }

  return Array.from(allPermissions);
}

async function checkSystemResourcePermissions(
  resourceId: SystemResourceID,
  granteeId: GranteeID,
  orgId: DriveID // Ensure orgId is DriveID
): Promise<SystemPermissionType[]> {
  const permissionsSet = new Set<SystemPermissionType>();
  const currentTime = Date.now();

  const parsedResourceId = parseSystemResourceIDString(resourceId);

  // SQL query updated to exclude labels, external_id, external_payload
  const rows = await db.queryDrive(
    orgId,
    `SELECT
      ps.id,
      ps.resource_type,
      ps.resource_identifier,
      ps.grantee_type,
      ps.grantee_id,
      ps.granted_by,
      GROUP_CONCAT(pst.permission_type) AS permission_types_list,
      ps.begin_date_ms,
      ps.expiry_date_ms,
      ps.note,
      ps.created_at,
      ps.last_modified_at,
      ps.redeem_code,
      ps.from_placeholder_grantee,
      ps.metadata_type,
      ps.metadata_content
    FROM permissions_system ps
    JOIN permissions_system_types pst ON ps.id = pst.permission_id
    WHERE ps.resource_type = ? AND ps.resource_identifier = ?
    GROUP BY ps.id`,
    [parsedResourceId.type, parsedResourceId.value]
  );

  for (const row of rows) {
    const permission: SystemPermission = mapDbRowToSystemPermission(row);

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

/**
 * Checks if a user has a specific required permission for the 'Permissions' system table.
 * This function performs its own owner check.
 * @param userId The ID of the user to check.
 * @param requiredPermission The specific SystemPermissionType required (e.g., VIEW, CREATE).
 * @param orgId The ID of the organization/drive.
 * @returns A Promise that resolves to true if the user has the required permission or is the owner, false otherwise.
 */
export async function checkPermissionsTableAccess(
  userId: UserID,
  requiredPermission: SystemPermissionType,
  orgId: DriveID // Ensure orgId is DriveID, removed isOwner
): Promise<boolean> {
  const isOwner = (await getDriveOwnerId(orgId)) === userId;

  if (isOwner) {
    return true;
  }

  const permissions = await checkSystemPermissions(
    `TABLE_${SystemTableValueEnum.PERMISSIONS}` as SystemResourceID,
    userId,
    orgId
  );
  return permissions.includes(requiredPermission);
}

export async function checkSystemResourcePermissionsLabels(
  resourceId: SystemResourceID,
  granteeId: GranteeID,
  labelStringValue: string,
  orgId: DriveID // Ensure orgId is DriveID
): Promise<SystemPermissionType[]> {
  const permissionsSet = new Set<SystemPermissionType>();

  const directPermissions = await checkSystemResourcePermissionsLabelsInternal(
    resourceId,
    granteeId,
    labelStringValue,
    orgId
  );
  directPermissions.forEach((p) => permissionsSet.add(p));

  const publicPermissions = await checkSystemResourcePermissionsLabelsInternal(
    resourceId,
    PUBLIC_GRANTEE_ID_STRING,
    labelStringValue,
    orgId
  );
  publicPermissions.forEach((p) => permissionsSet.add(p));

  if (granteeId.startsWith(IDPrefixEnum.User)) {
    const userId = granteeId as UserID;

    const currentTime = Date.now();
    const userGroupsQuery = `
    SELECT DISTINCT group_id FROM group_invites
    WHERE invitee_id = ?
    AND invitee_type = 'USER'
    AND active_from <= ?
    AND (expires_at <= 0 OR expires_at > ?)
`;
    const userGroupsRows = await db.queryDrive(orgId, userGroupsQuery, [
      userId,
      currentTime,
      currentTime,
    ]);

    for (const row of userGroupsRows) {
      const groupId = `${row.group_id}` as GroupID;
      const groupPermissions =
        await checkSystemResourcePermissionsLabelsInternal(
          resourceId,
          groupId,
          labelStringValue,
          orgId
        );
      groupPermissions.forEach((p) => permissionsSet.add(p));
    }
  }

  return Array.from(permissionsSet);
}

async function checkSystemResourcePermissionsLabelsInternal(
  resourceId: SystemResourceID,
  granteeId: GranteeID,
  labelStringValue: string,
  orgId: DriveID // Ensure orgId is DriveID
): Promise<SystemPermissionType[]> {
  const permissionsSet = new Set<SystemPermissionType>();
  const currentTime = Date.now();

  const parsedResourceId = parseSystemResourceIDString(resourceId);

  // SQL query updated to exclude labels, external_id, external_payload
  const rows = await db.queryDrive(
    orgId,
    `SELECT
      ps.id,
      ps.resource_type,
      ps.resource_identifier,
      ps.grantee_type,
      ps.grantee_id,
      ps.granted_by,
      GROUP_CONCAT(pst.permission_type) AS permission_types_list,
      ps.begin_date_ms,
      ps.expiry_date_ms,
      ps.note,
      ps.created_at,
      ps.last_modified_at,
      ps.redeem_code,
      ps.from_placeholder_grantee,
      ps.metadata_type,
      ps.metadata_content
    FROM permissions_system ps
    JOIN permissions_system_types pst ON ps.id = pst.permission_id
    WHERE ps.resource_type = ? AND ps.resource_identifier = ?
    GROUP BY ps.id`,
    [parsedResourceId.type, parsedResourceId.value]
  );

  for (const row of rows) {
    const permission: SystemPermission = mapDbRowToSystemPermission(row);

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
      granteeId.startsWith(IDPrefixEnum.PlaceholderPermissionGrantee) &&
      permissionGrantedTo.startsWith(IDPrefixEnum.PlaceholderPermissionGrantee)
    ) {
      applies = granteeId === permissionGrantedTo;
    }

    if (applies) {
      let labelMatch = true;
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
  } else if (permission.granted_to.startsWith(PLACEHOLDER_GRANTEE_ID_PREFIX)) {
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

export async function redactLabelValue(
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
