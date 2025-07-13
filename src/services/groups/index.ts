// src/services/groups.ts

import {
  UserID,
  GroupID,
  Group, // Assuming this is your TypeScript interface for Group
  GroupInvite, // Assuming this is your TypeScript interface for GroupInvite
  GroupInviteeID, // Assuming this is your TypeScript type for GroupInviteeID
  IDPrefixEnum,
  URLEndpoint,
  IResponseValidateGroupMember,
  GroupInviteID,
  GroupRole, // Assuming this type exists for external group validation response
} from "@officexapp/types"; // Adjust this path to your actual types
import { db } from "../../services/database"; // Adjust this path to your database service
import { getDriveOwnerId } from "../../routes/v1/types";

/**
 * Represents a Group record as stored in the SQLite database.
 * This should match the `groups` table schema.
 */
interface GroupDbRow {
  id: string; // GroupID without prefix (just UUID part)
  name: string;
  owner_user_id: string; // UserID without prefix (just UUID part)
  avatar?: string;
  private_note?: string;
  public_note?: string;
  created_at: number;
  last_modified_at: number;
  drive_id: string; // DriveID
  endpoint_url: string; // URLEndpoint
  external_id?: string;
  external_payload?: string;
  // Note: admin_invites and member_invites are typically stored in a separate join table or derived
}

/**
 * Represents a GroupInvite record as stored in the SQLite database.
 * This should match the `group_invites` table schema.
 */
interface GroupInviteDbRow {
  id: string; // GroupInviteID without prefix
  group_id: string; // GroupID without prefix
  inviter_user_id: string; // UserID without prefix
  invitee_type: string; // 'USER', 'PLACEHOLDER', 'PUBLIC'
  invitee_id?: string; // UserID (without prefix) or PlaceholderID (without prefix), NULL if public
  role: string; // 'ADMIN', 'MEMBER'
  note: string;
  active_from: number;
  expires_at: number;
  created_at: number;
  last_modified_at: number;
  redeem_code?: string;
  from_placeholder_invitee?: string;
  external_id?: string;
  external_payload?: string;
}

/**
 * Helper to extract plain ID from prefixed UserID string for DB queries.
 * Handles cases where the ID might not have the prefix (e.g., direct from DB).
 */
export function extractPlainUserId(prefixedUserId: UserID): string {
  if (prefixedUserId.startsWith(IDPrefixEnum.User)) {
    return prefixedUserId.substring(IDPrefixEnum.User.length);
  }
  return prefixedUserId; // Return as is if no prefix (e.g., if it's already a plain UUID from DB)
}

/**
 * Helper to extract plain ID from prefixed GroupID string for DB queries.
 */
export function extractPlainGroupId(prefixedGroupId: GroupID): string {
  if (prefixedGroupId.startsWith(IDPrefixEnum.Group)) {
    return prefixedGroupId.substring(IDPrefixEnum.Group.length);
  }
  return prefixedGroupId;
}

/**
 * Helper to extract plain ID from prefixed GroupInviteID string for DB queries.
 */
export function extractPlainGroupInviteId(
  prefixedGroupInviteId: string
): string {
  if (prefixedGroupInviteId.startsWith(IDPrefixEnum.GroupInvite)) {
    return prefixedGroupInviteId.substring(IDPrefixEnum.GroupInvite.length);
  }
  return prefixedGroupInviteId;
}

/**
 * Helper to extract plain ID from prefixed PlaceholderPermissionGranteeID string for DB queries.
 */
export function extractPlainPlaceholderGranteeId(
  prefixedPlaceholderId: string
): string {
  if (
    prefixedPlaceholderId.startsWith(IDPrefixEnum.PlaceholderPermissionGrantee)
  ) {
    return prefixedPlaceholderId.substring(
      IDPrefixEnum.PlaceholderPermissionGrantee.length
    );
  }
  return prefixedPlaceholderId;
}

/**
 * Retrieves a group by its ID.
 * @param groupId The ID of the group (prefixed).
 * @param orgId The organization ID (drive ID) to query the correct database.
 * @returns The Group object if found, otherwise undefined.
 */
export async function getGroupById(
  groupId: GroupID,
  orgId: string
): Promise<Group | undefined> {
  const plainGroupId = extractPlainGroupId(groupId); // Use helper to get plain ID
  const query = `
      SELECT
        id, name, owner_user_id, avatar, private_note, public_note,
        created_at, last_modified_at, drive_id, endpoint_url,
        external_id, external_payload
      FROM groups
      WHERE id = ?;
    `;
  const rows = await db.queryDrive(orgId, query, [plainGroupId]);

  if (rows.length === 0) {
    return undefined;
  }

  const row = rows[0] as GroupDbRow;

  return {
    id: `${IDPrefixEnum.Group}${row.id}` as GroupID, // Reconstruct prefixed ID
    name: row.name,
    owner: `${IDPrefixEnum.User}${row.owner_user_id}` as UserID, // Reconstruct prefixed UserID
    avatar: row.avatar || "",
    private_note: row.private_note,
    public_note: row.public_note,
    admin_invites: [], // As per your comment, these are placeholders or need separate fetching
    member_invites: [], // As per your comment, these are placeholders or need separate fetching
    created_at: row.created_at,
    last_modified_at: row.last_modified_at,
    drive_id: row.drive_id,
    endpoint_url: row.endpoint_url as URLEndpoint,
    labels: [], // Labels are handled via a join table, fetch separately if needed
    external_id: row.external_id,
    external_payload: row.external_payload,
  };
}

/**
 * Checks if a user is an admin of a specific group.
 * @param userId The ID of the user (prefixed).
 * @param groupId The ID of the group (prefixed).
 * @param orgId The organization ID (drive ID) to query the correct database.
 * @returns True if the user is an admin, false otherwise.
 */
export async function isGroupAdmin(
  userId: UserID,
  groupId: GroupID,
  orgId: string
): Promise<boolean> {
  const group = await getGroupById(groupId, orgId);
  if (!group) {
    return false;
  }

  // 1. Check if user is the owner of the group
  if (group.owner === userId) {
    return true;
  }

  // 2. Check admin invites
  const currentTime = Date.now();
  const plainUserId = extractPlainUserId(userId);
  const plainGroupId = extractPlainGroupId(groupId);

  const adminInviteQuery = `
      SELECT
        gi.id, gi.group_id, gi.inviter_user_id, gi.invitee_type, gi.invitee_id,
        gi.role, gi.note, gi.active_from, gi.expires_at,
        gi.created_at, gi.last_modified_at, gi.redeem_code,
        gi.from_placeholder_invitee, gi.external_id, gi.external_payload
      FROM group_invites gi
      WHERE gi.group_id = ?
        AND gi.role = 'ADMIN'
        AND gi.invitee_type = 'USER'
        AND gi.invitee_id = ?;
    `;
  const adminInviteRows = await db.queryDrive(orgId, adminInviteQuery, [
    plainGroupId,
    plainUserId,
  ]);

  for (const row of adminInviteRows) {
    const invite = row as GroupInviteDbRow;
    if (
      invite.active_from <= currentTime &&
      (invite.expires_at <= 0 || invite.expires_at > currentTime)
    ) {
      return true;
    }
  }

  return false;
}

/**
 * Retrieves a group invite by its ID.
 * @param inviteId The ID of the group invite (prefixed).
 * @param orgId The organization ID (drive ID) to query the correct database.
 * @returns The GroupInvite object if found, otherwise undefined.
 */
export async function getGroupInviteById(
  inviteId: GroupInviteID,
  orgId: string
): Promise<GroupInvite | undefined> {
  const plainInviteId = extractPlainGroupInviteId(inviteId);
  const query = `
      SELECT
        id, group_id, inviter_id, invitee_type, invitee_id, role, note,
        active_from, expires_at, created_at, last_modified_at,
        redeem_code, from_placeholder_invitee, external_id, external_payload
      FROM group_invites
      WHERE id = ?;
    `;
  const rows = await db.queryDrive(orgId, query, [plainInviteId]);

  if (rows.length === 0) {
    return undefined;
  }

  const row = rows[0] as GroupInviteDbRow;

  // Map invitee_id based on invitee_type
  let inviteeFullId: GroupInviteeID;
  if (row.invitee_type === "USER" && row.invitee_id) {
    inviteeFullId = `${IDPrefixEnum.User}${row.invitee_id}` as UserID;
  } else if (row.invitee_type === "PLACEHOLDER" && row.invitee_id) {
    inviteeFullId = `${IDPrefixEnum.PlaceholderGroupInviteeID}${row.invitee_id}`;
  } else if (row.invitee_type === "PUBLIC") {
    inviteeFullId = "PUBLIC";
  } else {
    // Fallback or error, depending on expected strictness
    console.warn(
      `Unexpected invitee_type or missing invitee_id: ${row.invitee_type}, ID: ${row.invitee_id}`
    );
    inviteeFullId = "PUBLIC"; // Default to public for safety
  }

  return {
    id: `${IDPrefixEnum.GroupInvite}${row.id}` as GroupInviteID,
    group_id: `${IDPrefixEnum.Group}${row.group_id}` as GroupID,
    inviter_id: `${IDPrefixEnum.User}${row.inviter_user_id}` as UserID,
    invitee_id: inviteeFullId,
    role: row.role as GroupRole,
    note: row.note,
    active_from: row.active_from,
    expires_at: row.expires_at,
    created_at: row.created_at,
    last_modified_at: row.last_modified_at,
    redeem_code: row.redeem_code,
    from_placeholder_invitee: row.from_placeholder_invitee,
    labels: [],
    external_id: row.external_id,
    external_payload: row.external_payload,
  };
}

/**
 * Checks if a user is a member of a local group. This includes both admins and regular members.
 *
 * IMPORTANT: This function assumes the group is "local" (i.e., on the same drive).
 * It does NOT make HTTP calls to external drives.
 * @param userId The ID of the user (prefixed).
 * @param group The Group object to check membership against.
 * @param orgId The organization ID (drive ID) to query the correct database.
 * @returns True if the user is a member of the group, false otherwise.
 */
export async function isUserOnLocalGroup(
  userId: UserID,
  group: Group,
  orgId: string
): Promise<boolean> {
  // 1. Check if user is the owner
  if (group.owner === userId) {
    return true;
  }

  // 2. Check all member invites
  const currentTime = Date.now();
  const plainUserId = extractPlainUserId(userId);
  const plainGroupId = extractPlainGroupId(group.id);

  const userInvitesQuery = `
      SELECT
        gi.id, gi.group_id, gi.inviter_user_id, gi.invitee_type, gi.invitee_id,
        gi.role, gi.note, gi.active_from, gi.expires_at,
        gi.created_at, gi.last_modified_at, gi.redeem_code,
        gi.from_placeholder_invitee, gi.external_id, gi.external_payload
      FROM group_invites gi
      WHERE gi.group_id = ?
        AND gi.invitee_type = 'USER'
        AND gi.invitee_id = ?;
    `;

  const inviteRows = await db.queryDrive(orgId, userInvitesQuery, [
    plainGroupId,
    plainUserId,
  ]);

  for (const row of inviteRows) {
    const invite = row as GroupInviteDbRow;
    if (
      invite.active_from <= currentTime &&
      (invite.expires_at <= 0 || invite.expires_at > currentTime)
    ) {
      return true;
    }
  }

  return false;
}

/**
 * Checks if a user is a member of any group, including local and potentially external groups.
 * @param userId The ID of the user (prefixed).
 * @param groupId The ID of the group (prefixed).
 * @param orgId The organization ID (drive ID) to query the correct database.
 * @returns True if the user is a member of the group, false otherwise.
 */
export async function isUserInGroup(
  userId: UserID,
  groupId: GroupID,
  orgId: string
): Promise<boolean> {
  const group = await getGroupById(groupId, orgId);
  if (!group) {
    return false; // Group not found
  }

  const localDriveInfo = await db.queryDrive(
    orgId,
    `SELECT url_endpoint FROM about_drive LIMIT 1;`
  );
  const localDriveEndpoint =
    localDriveInfo.length > 0 ? localDriveInfo[0].url_endpoint : "";

  if (group.endpoint_url === localDriveEndpoint) {
    // If it's our own drive's group, use local validation
    return isUserOnLocalGroup(userId, group, orgId);
  } else {
    // It's an external group, make HTTP call to their validate endpoint
    const validationUrl = `${group.endpoint_url.replace(
      /\/+$/,
      ""
    )}/groups/validate`;

    try {
      const response = await fetch(validationUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          group_id: extractPlainGroupId(groupId), // Send plain ID for external API
          user_id: extractPlainUserId(userId), // Send plain ID for external API
        }),
      });

      if (!response.ok) {
        console.error(
          `External group validation failed with status: ${response.status}`
        );
        return false;
      }

      const result =
        (await response.json()) as unknown as IResponseValidateGroupMember;
      return result.ok.data.is_member === true;
    } catch (e) {
      console.error(`External group validation request failed: ${e}`);
      return false;
    }
  }
}
