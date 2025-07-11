// src/services/permissions/system.ts

import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import {
  SystemPermission,
  SystemPermissionFE,
  SystemPermissionType,
  IDPrefixEnum,
  SystemResourceID,
  SystemTableValueEnum,
  SystemRecordIDEnum,
  PermissionMetadata,
  PermissionMetadataTypeEnum,
  PermissionMetadataContent,
  UserID,
  GranteeID,
  SystemPermissionID,
  IRequestCheckSystemPermissions,
  IRequestCreateSystemPermission,
  IRequestDeleteSystemPermission,
  IRequestListSystemPermissions,
  IRequestRedeemSystemPermission,
  IRequestUpdateSystemPermission,
  GroupID,
  DriveID,
  DiskID,
  ApiKeyID,
  WebhookID,
  LabelID,
} from "@officexapp/types";
import { db, dbHelpers } from "../../services/database";
import { authenticateRequest } from "../../services/auth";
import { isUserInGroup, isGroupAdmin } from "../../services/groups"; // Assuming this exists for checking group membership
import { getContactById } from "../../services/contacts"; // Assuming a contacts service
import { getGroupById } from "../../services/groups"; // Assuming a groups service
import { getDriveById } from "../../services/drives"; // Assuming a drives service
import { getDiskById } from "../../services/disks"; // Assuming a disks service
import { getApiKeyById } from "../../services/api-keys"; // Assuming an api-keys service
import { getWebhookById } from "../../services/webhooks"; // Assuming a webhooks service
import { getLabelById, redactLabel } from "../../services/labels"; // Assuming a labels service
import {
  createApiResponse,
  ErrorResponse,
  OrgIdParams,
  validateIdString,
  validateDescription,
  validateExternalId,
  validateExternalPayload,
  parsePermissionGranteeID, // Re-export from directory permissions
} from "../utils";
import { SortDirection } from "@officexapp/types";

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

// Utility to convert raw DB row to SystemPermission
function mapDbRowToSystemPermission(row: any): SystemPermission {
  let metadata: PermissionMetadata | undefined;
  if (row.metadata_type && row.metadata_content) {
    let content: PermissionMetadataContent;
    try {
      // Assuming metadata_content is a JSON string
      const parsedContent = JSON.parse(row.metadata_content);

      switch (row.metadata_type) {
        case PermissionMetadataTypeEnum.LABELS:
          content = { Labels: parsedContent }; // Assuming Labels content is a string value directly
          break;
        case PermissionMetadataTypeEnum.DIRECTORY_PASSWORD:
          content = { DirectoryPassword: parsedContent }; // Assuming DirectoryPassword content is a string value directly
          break;
        default:
          content = parsedContent; // Fallback for unknown or complex types
          break;
      }

      metadata = {
        metadata_type: row.metadata_type as PermissionMetadataTypeEnum,
        content: content,
      };
    } catch (e) {
      console.warn(
        "Failed to parse metadata_content:",
        row.metadata_content,
        e
      );
      // Optionally set metadata to undefined or a default error state
    }
  }

  // Reconstruct SystemResourceID
  let resourceId: SystemResourceID;
  const [prefix, idPart] = row.resource_identifier.split("_", 2);
  if (prefix === "TABLE") {
    switch (idPart) {
      case "DRIVES":
        resourceId = `Table_${SystemTableValueEnum.DRIVES}`;
        break;
      case "DISKS":
        resourceId = `Table_${SystemTableValueEnum.DISKS}`;
        break;
      case "CONTACTS":
        resourceId = `Table_${SystemTableValueEnum.CONTACTS}`;
        break;
      case "GROUPS":
        resourceId = `Table_${SystemTableValueEnum.GROUPS}`;
        break;
      case "API_KEYS":
        resourceId = `Table_${SystemTableValueEnum.API_KEYS}`;
        break;
      case "PERMISSIONS":
        resourceId = `Table_${SystemTableValueEnum.PERMISSIONS}`;
        break;
      case "WEBHOOKS":
        resourceId = `Table_${SystemTableValueEnum.WEBHOOKS}`;
        break;
      case "LABELS":
        resourceId = `Table_${SystemTableValueEnum.LABELS}`;
        break;
      case "INBOX":
        resourceId = `Table_${SystemTableValueEnum.INBOX}`;
        break;
      default:
        resourceId = `Record_${row.resource_identifier}`;
        break;
    }
  } else {
    let recordType: SystemRecordIDEnum["type"] = "Unknown";
    if (row.resource_identifier.startsWith(IDPrefixEnum.Drive))
      recordType = "Drive";
    else if (row.resource_identifier.startsWith(IDPrefixEnum.Disk))
      recordType = "Disk";
    else if (row.resource_identifier.startsWith(IDPrefixEnum.User))
      recordType = "User";
    else if (row.resource_identifier.startsWith(IDPrefixEnum.Group))
      recordType = "Group";
    else if (row.resource_identifier.startsWith(IDPrefixEnum.ApiKey))
      recordType = "ApiKey";
    else if (
      row.resource_identifier.startsWith(IDPrefixEnum.SystemPermission) ||
      row.resource_identifier.startsWith(IDPrefixEnum.DirectoryPermission)
    )
      recordType = "Permission";
    else if (row.resource_identifier.startsWith(IDPrefixEnum.Webhook))
      recordType = "Webhook";
    else if (row.resource_identifier.startsWith(IDPrefixEnum.LabelID))
      recordType = "Label";

    resourceId = {
      type: "Record",
      value: { type: recordType, value: row.resource_identifier },
    };
  }

  // Reconstruct GranteeID
  let grantedTo: GranteeID;
  if (row.grantee_type === "Public") {
    grantedTo = "Public";
  } else if (row.grantee_type === "User") {
    grantedTo = row.grantee_id as UserID;
  } else if (row.grantee_type === "Group") {
    grantedTo = row.grantee_id as UserID; // Stored as UserID in DB for simplicity but is GroupID
  } else if (row.grantee_type === "Placeholder") {
    grantedTo = `PlaceholderDirectoryPermissionGrantee_${row.grantee_id}`;
  } else {
    grantedTo = "Public"; // Fallback
  }

  return {
    id: row.id,
    resource_id: resourceId,
    granted_to: grantedTo,
    granted_by: row.granted_by_user_id,
    permission_types: (row.permission_types_list as string)
      .split(",")
      .map((typeStr) => typeStr.trim() as SystemPermissionType),
    begin_date_ms: row.begin_date_ms,
    expiry_date_ms: row.expiry_date_ms,
    note: row.note,
    created_at: row.created_at,
    last_modified_at: row.last_modified_at,
    redeem_code: row.redeem_code,
    from_placeholder_grantee: row.from_placeholder_grantee,
    labels: row.labels_list ? (row.labels_list as string).split(",") : [],
    metadata: metadata,
    external_id: row.external_id,
    external_payload: row.external_payload,
  };
}

// Utility to convert SystemPermission to SystemPermissionFE
async function castToSystemPermissionFE(
  permission: SystemPermission,
  currentUserId: UserID,
  orgId: string
): Promise<SystemPermissionFE> {
  const isOwner = (await getOwnerId(orgId)) === currentUserId;

  let resourceName: string | undefined;
  if (permission.resource_id.startsWith("TABLE_")) {
    resourceName = `${permission.resource_id} Table`;
  } else if (permission.resource_id.startsWith("RECORD_")) {
    switch (permission.resource_id.substring("RECORD_".length)) {
      case "User":
        const contact = await getContactById(
          orgId,
          permission.resource_id.substring("RECORD_".length) as UserID
        );
        resourceName = contact?.name;
        break;
      case "Group":
        const group = await getGroupById(
          orgId,
          permission.resource_id.substring("RECORD_".length) as GroupID
        );
        resourceName = group?.name;
        break;
      case "Drive":
        const drive = await getDriveById(
          orgId,
          permission.resource_id.substring("RECORD_".length) as DriveID
        );
        resourceName = drive?.name;
        break;
      case "Disk":
        const disk = await getDiskById(
          orgId,
          permission.resource_id.substring("RECORD_".length) as DiskID
        );
        resourceName = disk?.name;
        break;
      case "ApiKey":
        const apiKey = await getApiKeyById(
          orgId,
          permission.resource_id.substring("RECORD_".length) as ApiKeyID
        );
        resourceName = apiKey?.name;
        break;
      case "Webhook":
        const webhook = await getWebhookById(
          orgId,
          permission.resource_id.substring("RECORD_".length) as WebhookID
        );
        resourceName = webhook?.name;
        break;
      case "Label":
        const label = await getLabelById(
          orgId,
          permission.resource_id.substring("RECORD_".length) as LabelID
        );
        resourceName = label?.value;
        break;
      case "Permission":
        resourceName = `Permission ${permission.resource_id.substring("RECORD_".length)}`;
        break;
      default:
        resourceName = permission.resource_id.substring("RECORD_".length);
    }
  }

  // Get grantee_name and grantee_avatar
  let granteeName: string | undefined;
  let granteeAvatar: string | undefined;
  if (typeof permission.granted_to === "string") {
    if (permission.granted_to === "Public") {
      granteeName = "PUBLIC";
    } else if (permission.granted_to.startsWith(IDPrefixEnum.User)) {
      const contact = await getContactById(
        orgId,
        permission.granted_to as UserID
      );
      granteeName = contact?.name;
      granteeAvatar = contact?.avatar;
    } else if (permission.granted_to.startsWith(IDPrefixEnum.Group)) {
      const group = await getGroupById(orgId, permission.granted_to);
      granteeName = group?.name;
      granteeAvatar = group?.avatar;
    } else if (
      permission.granted_to.startsWith(
        IDPrefixEnum.PlaceholderPermissionGrantee
      )
    ) {
      granteeName = "Awaiting Anon";
    }
  }

  // Get granter_name
  const granter = await getContactById(orgId, permission.granted_by);
  const granterName = granter?.name;

  // Get permission previews for the current user on this permission record
  const recordPermissions = await checkSystemPermissions(
    {
      type: "Record",
      value: { type: "Permission", value: permission.id },
    } as SystemResourceID,
    currentUserId,
    orgId
  );
  const tablePermissions = await checkSystemPermissions(
    { type: "Table", value: SystemTableEnum.PERMISSIONS },
    currentUserId,
    orgId
  );
  const permissionPreviews = Array.from(
    new Set([...recordPermissions, ...tablePermissions])
  );

  const castedPermission: SystemPermissionFE = {
    id: permission.id,
    resource_id: permission.resource_id,
    granted_to:
      typeof permission.granted_to === "string"
        ? permission.granted_to
        : (permission.granted_to as any).User ||
          (permission.granted_to as any).Group ||
          (permission.granted_to as any).PlaceholderDirectoryPermissionGrantee,
    granted_by: permission.granted_by,
    permission_types: permission.permission_types,
    begin_date_ms: permission.begin_date_ms,
    expiry_date_ms: permission.expiry_date_ms,
    note: permission.note,
    created_at: permission.created_at,
    last_modified_at: permission.last_modified_at,
    from_placeholder_grantee: permission.from_placeholder_grantee,
    labels: permission.labels, // Will be redacted by `redactSystemPermissionFE`
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

  return redactSystemPermissionFE(castedPermission, currentUserId, isOwner);
}

// Function to redact SystemPermissionFE
function redactSystemPermissionFE(
  permissionFe: SystemPermissionFE,
  userId: UserID,
  isOwner: boolean
): SystemPermissionFE {
  const redacted = { ...permissionFe };

  const hasEditPermissions = redacted.permission_previews.includes(
    SystemPermissionType.EDIT
  );

  if (!isOwner) {
    // Redact sensitive fields if not owner
    // Example: if there were 'private_note' on SystemPermission, it would be set to undefined here
    if (!hasEditPermissions) {
      // Further redaction if no edit permissions
    }
  }

  // Filter labels
  redacted.labels = redacted.labels
    .map((label) => redactLabel(label, userId)) // Assuming redactLabel returns null for redacted labels
    .filter((label): label is string => label !== null); // Filter out nulls and assert type

  return redacted;
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

  const permissionGrantedTo = parsePermissionGranteeID(
    typeof permission.granted_to === "string"
      ? permission.granted_to // Directly handle "Public" or simple string IDs
      : (permission.granted_to as any).User ||
          (permission.granted_to as any).Group ||
          (permission.granted_to as any).PlaceholderDirectoryPermissionGrantee
  );

  if (!permissionGrantedTo) {
    return false; // Invalid grantee ID
  }

  // Check if user is the direct grantee
  if (permissionGrantedTo === "Public") {
    return true; // Everyone can see public permissions
  } else if (permissionGrantedTo.startsWith(IDPrefixEnum.User)) {
    if (permissionGrantedTo === requesterUserId) {
      return true;
    }
  } else if (permissionGrantedTo.startsWith(IDPrefixEnum.Group)) {
    if (await isUserInGroup(requesterUserId, permissionGrantedTo, orgId)) {
      return true;
    }
  } else if (
    permissionGrantedTo.startsWith(IDPrefixEnum.PlaceholderPermissionGrantee)
  ) {
    // One-time links can only be accessed by the creator
    return permission.granted_by === requesterUserId;
  }

  return false;
}

export async function hasSystemManagePermission(
  userId: UserID,
  resourceId: SystemResourceID,
  orgId: string
): Promise<boolean> {
  const permissions = await checkSystemPermissions(resourceId, userId, orgId);
  return permissions.includes(SystemPermissionType.INVITE);
}

// Check what kind of permission a specific user has on a specific resource
export async function checkSystemPermissions(
  resourceId: SystemResourceID,
  granteeId: GranteeID,
  orgId: string
): Promise<SystemPermissionType[]> {
  const allPermissions = new Set<SystemPermissionType>();
  const isOwner = (await getOwnerId(orgId)) === granteeId; // Simplified check for owner being the grantee

  if (isOwner) {
    return [
      SystemPermissionType.CREATE,
      SystemPermissionType.VIEW,
      SystemPermissionType.EDIT,
      SystemPermissionType.DELETE,
      SystemPermissionType.INVITE,
    ];
  }

  // First, check direct permissions for the grantee
  const resourcePermissions = await checkSystemResourcePermissions(
    resourceId,
    granteeId,
    orgId
  );
  resourcePermissions.forEach((p) => allPermissions.add(p));

  // Always check public permissions (for any grantee type)
  const publicPermissions = await checkSystemResourcePermissions(
    resourceId,
    "Public",
    orgId
  );
  publicPermissions.forEach((p) => allPermissions.add(p));

  // If the grantee is a user, also check group permissions
  if (
    typeof granteeId === "string" &&
    granteeId.startsWith(IDPrefixEnum.User)
  ) {
    const userGroups = await db.queryDrive(
      orgId,
      `SELECT group_id FROM contact_groups WHERE user_id = ?`,
      [granteeId]
    );

    for (const row of userGroups) {
      const groupPermissions = await checkSystemResourcePermissions(
        resourceId,
        row.group_id as GranteeID, // Cast to GranteeID
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
  orgId: string
): Promise<SystemPermissionType[]> {
  const permissionsSet = new Set<SystemPermissionType>();
  const currentTime = getCurrentTimeMs();

  let resourceIdentifier: string;
  let granteeType: string;
  let granteeIdValue: string | null = null;

  if (resourceId.startsWith("TABLE_")) {
    resourceIdentifier = resourceId;
  } else {
    resourceIdentifier = resourceId;
  }

  if (granteeId === "Public") {
    granteeType = "Public";
  } else if (granteeId.startsWith(IDPrefixEnum.User)) {
    granteeType = "User";
    granteeIdValue = granteeId;
  } else if (granteeId.startsWith(IDPrefixEnum.Group)) {
    granteeType = "Group";
    granteeIdValue = granteeId;
  } else if (granteeId.startsWith(IDPrefixEnum.PlaceholderPermissionGrantee)) {
    granteeType = "Placeholder";
    granteeIdValue = granteeId.substring(
      IDPrefixEnum.PlaceholderPermissionGrantee.length + 1
    ); // Extract actual ID
  } else {
    // Should not happen with proper type handling, but for safety:
    return [];
  }

  const queryParams = [resourceIdentifier, granteeType];
  if (granteeIdValue) {
    queryParams.push(granteeIdValue);
  }

  const rows = await db.queryDrive(
    orgId,
    `
      SELECT
        ps.id,
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
        ps.external_payload,
        (SELECT GROUP_CONCAT(label_id) FROM permission_system_labels WHERE permission_id = ps.id) AS labels_list
      FROM permissions_system ps
      JOIN permissions_system_types pst ON ps.id = pst.permission_id
      WHERE ps.resource_identifier = ? AND ps.grantee_type = ? ${granteeIdValue ? "AND ps.grantee_id = ?" : ""}
      GROUP BY ps.id
    `,
    queryParams
  );

  for (const row of rows) {
    const permission: SystemPermission = mapDbRowToSystemPermission(row);

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

    permission.permission_types.forEach((type) => permissionsSet.add(type));
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
    { type: "Table", value: SystemTableValueEnum.PERMISSIONS },
    userId,
    orgId
  );
  return permissions.includes(requiredPermission);
}

export async function checkSystemResourcePermissionsByLabels(
  resourceId: SystemResourceID,
  granteeId: GranteeID,
  labelStringValue: string,
  orgId: string
): Promise<SystemPermissionType[]> {
  const permissionsSet = new Set<SystemPermissionType>();
  const currentTime = getCurrentTimeMs();

  let resourceIdentifier: string;
  if (resourceId.startsWith("TABLE_")) {
    resourceIdentifier = resourceId;
  } else {
    resourceIdentifier = resourceId;
  }

  // This query is more complex as it needs to filter by metadata content (label prefix)
  // and join with permission_system_types and potentially filter by grantee.
  // SQLite doesn't have a direct way to query JSON content easily for the 'LIKE' operator.
  // We will need to retrieve all permissions for the resource and filter in application logic.
  const rows = await db.queryDrive(
    orgId,
    `
      SELECT
        ps.id,
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
        ps.external_payload,
        (SELECT GROUP_CONCAT(label_id) FROM permission_system_labels WHERE permission_id = ps.id) AS labels_list
      FROM permissions_system ps
      JOIN permissions_system_types pst ON ps.id = pst.permission_id
      WHERE ps.resource_identifier = ?
      GROUP BY ps.id
    `,
    [resourceIdentifier]
  );

  for (const row of rows) {
    const permission: SystemPermission = mapDbRowToSystemPermission(row);

    // Filter by time
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
    if (permission.granted_to === "Public") {
      applies = true;
    } else if (typeof permission.granted_to === "string") {
      if (granteeId === "Public" && permission.granted_to === "Public") {
        applies = true;
      } else if (
        granteeId.startsWith(IDPrefixEnum.User) &&
        permission.granted_to.startsWith(IDPrefixEnum.User) &&
        granteeId === permission.granted_to
      ) {
        applies = true;
      } else if (
        granteeId.startsWith(IDPrefixEnum.Group) &&
        permission.granted_to.startsWith(IDPrefixEnum.Group) &&
        granteeId === permission.granted_to
      ) {
        applies = true;
      } else if (
        granteeId.startsWith(IDPrefixEnum.User) &&
        permission.granted_to.startsWith(IDPrefixEnum.Group)
      ) {
        applies = await isUserInGroup(
          granteeId as UserID,
          permission.granted_to as string,
          orgId
        );
      } else if (
        granteeId.startsWith(IDPrefixEnum.PlaceholderPermissionGrantee) &&
        permission.granted_to.startsWith(
          IDPrefixEnum.PlaceholderPermissionGrantee
        ) &&
        granteeId === permission.granted_to
      ) {
        applies = true;
      }
    }
    // TODO: Handle structured GranteeID if necessary

    if (!applies) {
      continue;
    }

    // Check metadata for label prefix match
    let labelMatch = true;
    if (permission.metadata) {
      if (
        permission.metadata.metadata_type === PermissionMetadataTypeEnum.LABELS
      ) {
        if (
          typeof permission.metadata.content === "object" &&
          "Labels" in permission.metadata.content &&
          typeof permission.metadata.content.Labels === "string"
        ) {
          const prefix = permission.metadata.content.Labels;
          if (
            !labelStringValue.toLowerCase().startsWith(prefix.toLowerCase())
          ) {
            labelMatch = false;
          }
        } else {
          // Metadata content for labels is not a string, or not in expected format
          labelMatch = false;
        }
      }
    }

    if (labelMatch) {
      permission.permission_types.forEach((type) => permissionsSet.add(type));
    }
  }

  return Array.from(permissionsSet);
}
