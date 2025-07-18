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
  GroupRole,
  GroupInviteeTypeEnum, // Assuming this type exists for external group validation response
} from "@officexapp/types"; // Adjust this path to your actual types
import { db, dbHelpers } from "../../services/database"; // Adjust this path to your database service
import { getDriveOwnerId } from "../../routes/v1/types";
import { v4 as uuidv4 } from "uuid";

/**
 * Represents a Group record as stored in the SQLite database.
 * This should match the `groups` table schema.
 */
interface GroupDbRow {
  id: string; // GroupID (full prefixed string)
  name: string;
  owner: string; // UserID (full prefixed string)
  avatar?: string;
  private_note?: string;
  public_note?: string;
  created_at: number;
  last_modified_at: number;
  drive_id: string; // DriveID (full prefixed string)
  endpoint_url: string; // URLEndpoint
  external_id?: string;
  external_payload?: string;
}

/**
 * Represents a GroupInvite record as stored in the SQLite database.
 * This should match the `group_invites` table schema.
 */
interface GroupInviteDbRow {
  id: string; // GroupInviteID (full prefixed string)
  group_id: string; // GroupID (full prefixed string)
  inviter_id: string; // UserID (full prefixed string)
  invitee_type: string; // 'USER', 'PLACEHOLDER', 'PUBLIC'
  invitee_id?: string; // UserID (full prefixed string) or PlaceholderID (full prefixed string), 'PUBLIC' string
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
 * Retrieves a group by its ID.
 * @param groupId The ID of the group (prefixed).
 * @param orgId The organization ID (drive ID) to query the correct database.
 * @returns The Group object if found, otherwise undefined.
 */
export async function getGroupById(
  groupId: GroupID,
  orgId: string
): Promise<Group | undefined> {
  // Use the full prefixed ID directly for the query
  const query = `
        SELECT
          id, name, owner, avatar, private_note, public_note,
          created_at, last_modified_at, drive_id, endpoint_url,
          external_id, external_payload
        FROM groups
        WHERE id = ?;
    `;
  const rows = await db.queryDrive(orgId, query, [groupId]);

  if (rows.length === 0) {
    return undefined;
  }

  const row = rows[0] as GroupDbRow;

  // Fetch admin and member invite IDs separately from group_invites table
  const adminInvitesQuery = `
        SELECT id FROM group_invites
        WHERE group_id = ? AND role = ?
          AND active_from <= ? AND (expires_at <= 0 OR expires_at > ?);
    `;
  const memberInvitesQuery = `
        SELECT id FROM group_invites
        WHERE group_id = ?
          AND active_from <= ? AND (expires_at <= 0 OR expires_at > ?);
    `;
  const currentTime = Date.now();

  const adminInviteRows = await db.queryDrive(orgId, adminInvitesQuery, [
    groupId,
    GroupRole.ADMIN,
    currentTime,
    currentTime,
  ]);
  const memberInviteRows = await db.queryDrive(orgId, memberInvitesQuery, [
    groupId,
    currentTime,
    currentTime,
  ]);

  const admin_invites = adminInviteRows.map(
    (r: { id: string }) => r.id as GroupInviteID
  );
  const member_invites = memberInviteRows.map(
    (r: { id: string }) => r.id as GroupInviteID
  );

  return {
    id: row.id as GroupID, // Already prefixed from DB per your clarification
    name: row.name,
    owner: row.owner as UserID, // Already prefixed from DB per your clarification
    avatar: row.avatar || "",
    private_note: row.private_note,
    public_note: row.public_note,
    admin_invites: admin_invites, // Populated from group_invites
    member_invites: member_invites, // Populated from group_invites
    created_at: row.created_at,
    last_modified_at: row.last_modified_at,
    drive_id: row.drive_id,
    endpoint_url: row.endpoint_url as URLEndpoint,
    labels: [], // As requested, labels are ignored and blank array
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

  // 2. Check active admin invites for the user
  const currentTime = Date.now();

  const adminInviteQuery = `
        SELECT gi.id
        FROM group_invites gi
        WHERE gi.group_id = ?
          AND gi.role = ?
          AND gi.invitee_id = ?
          AND gi.invitee_type = ?
          AND gi.active_from <= ?
          AND (gi.expires_at <= 0 OR gi.expires_at > ?);
    `;
  const adminInviteRows = await db.queryDrive(orgId, adminInviteQuery, [
    groupId,
    GroupRole.ADMIN,
    userId,
    GroupInviteeTypeEnum.USER,
    currentTime,
    currentTime,
  ]);

  return adminInviteRows.length > 0;
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
  const query = `
        SELECT
          id, group_id, inviter_id, invitee_type, invitee_id, role, note,
          active_from, expires_at, created_at, last_modified_at,
          redeem_code, from_placeholder_invitee, external_id, external_payload
        FROM group_invites
        WHERE id = ?;
    `;
  const rows = await db.queryDrive(orgId, query, [inviteId]);

  if (rows.length === 0) {
    return undefined;
  }

  const row = rows[0] as GroupInviteDbRow;

  // Map invitee_id based on invitee_type
  let inviteeFullId: GroupInviteeID;
  switch (row.invitee_type) {
    case GroupInviteeTypeEnum.USER:
      inviteeFullId = row.invitee_id as UserID;
      break;
    case GroupInviteeTypeEnum.PLACEHOLDER_GROUP_INVITEE:
      inviteeFullId = row.invitee_id as `PlaceholderGroupInviteeID_${string}`;
      break;
    case GroupInviteeTypeEnum.PUBLIC:
      inviteeFullId = GroupInviteeTypeEnum.PUBLIC;
      break;
    default:
      console.warn(
        `Unexpected invitee_type: ${row.invitee_type}. Defaulting to PUBLIC.`
      );
      inviteeFullId = GroupInviteeTypeEnum.PUBLIC; // Fallback for unexpected types
  }

  return {
    id: row.id as GroupInviteID,
    group_id: row.group_id as GroupID,
    inviter_id: row.inviter_id as UserID,
    invitee_id: inviteeFullId,
    role: row.role as GroupRole,
    note: row.note,
    active_from: row.active_from,
    expires_at: row.expires_at,
    created_at: row.created_at,
    last_modified_at: row.last_modified_at,
    redeem_code: row.redeem_code,
    from_placeholder_invitee: row.from_placeholder_invitee,
    labels: [], // As requested, labels are ignored and blank array
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

  // 2. Check active member invites (includes admins)
  const currentTime = Date.now();

  const userInvitesQuery = `
        SELECT gi.id
        FROM group_invites gi
        WHERE gi.group_id = ?
          AND gi.invitee_id = ?
          AND gi.invitee_type = ?
          AND gi.active_from <= ?
          AND (gi.expires_at <= 0 OR gi.expires_at > ?);
    `;

  const inviteRows = await db.queryDrive(orgId, userInvitesQuery, [
    group.id,
    userId,
    GroupInviteeTypeEnum.USER,
    currentTime,
    currentTime,
  ]);

  return inviteRows.length > 0;
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
          group_id: groupId, // Send prefixed ID for external API as assumed
          user_id: userId, // Send prefixed ID for external API as assumed
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

/**
 * Adds a user as a member to a specific group.
 * If the user is already a member, it does nothing.
 * If the user is an admin, their role remains admin.
 * @param groupId The ID of the group (prefixed).
 * @param userId The ID of the user (prefixed) to add.
 * @param role The role to assign to the user (MEMBER or ADMIN).
 * @param inviterId The ID of the user who is performing the addition (for invite record).
 * @param orgId The organization ID (drive ID).
 * @returns True if the member was added or already exists as a member/admin, false otherwise (e.g., group not found).
 */
export async function addGroupMember(
  groupId: GroupID,
  userId: UserID,
  role: GroupRole, // Added role parameter
  inviterId: UserID, // User performing the action
  orgId: string
): Promise<boolean> {
  const now = Date.now();

  try {
    // Check if the group exists
    const groupExists = await getGroupById(groupId, orgId);
    if (!groupExists) {
      console.warn(`Group not found: ${groupId}`);
      return false;
    }

    await dbHelpers.transaction("drive", orgId, (database) => {
      // Create or update a group_invites record for this user with the specified role.
      const existingInvite = database
        .prepare(
          `SELECT id, role FROM group_invites
           WHERE group_id = ? AND invitee_id = ? AND invitee_type = ?`
        )
        .get(groupId, userId, GroupInviteeTypeEnum.USER) as
        | GroupInviteDbRow
        | undefined;

      if (!existingInvite) {
        // No existing invite for this user in this group, create a new one
        const inviteId = `${IDPrefixEnum.GroupInvite}${uuidv4()}`;
        database
          .prepare(
            `INSERT INTO group_invites (
              id, group_id, inviter_id, invitee_id, invitee_type, role, note,
              active_from, expires_at, created_at, last_modified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
          )
          .run(
            inviteId,
            groupId,
            inviterId,
            userId,
            GroupInviteeTypeEnum.USER,
            role, // Use the provided role
            `Added as ${role} by ${inviterId}`,
            now, // Active immediately
            -1, // Never expires
            now,
            now
          );
      } else if (existingInvite.role !== role) {
        // If an invite exists but with a different role, update it
        database
          .prepare(
            `UPDATE group_invites SET role = ?, last_modified_at = ? WHERE id = ?`
          )
          .run(role, now, existingInvite.id);
      }
      // If role is the same, do nothing (already a member with that role)
    });

    return true;
  } catch (error) {
    console.error(`Error adding member ${userId} to group ${groupId}:`, error);
    return false;
  }
}

/**
 * Removes a user from a specific group.
 * This will remove their entry from `group_invites`.
 * @param groupId The ID of the group (prefixed).
 * @param userId The ID of the user (prefixed) to remove.
 * @param orgId The organization ID (drive ID).
 * @returns True if the member was removed or didn't exist, false otherwise (e.g., group not found).
 */
export async function removeMemberFromGroup(
  groupId: GroupID,
  userId: UserID,
  orgId: string
): Promise<boolean> {
  try {
    // Check if the group exists
    const groupExists = await getGroupById(groupId, orgId);
    if (!groupExists) {
      console.warn(`Group not found: ${groupId}`);
      return false;
    }

    await dbHelpers.transaction("drive", orgId, (database) => {
      // Delete associated group_invites for this user and group
      const stmt = database.prepare(
        `DELETE FROM group_invites WHERE group_id = ? AND invitee_id = ? AND invitee_type = ?`
      );
      stmt.run(groupId, userId, GroupInviteeTypeEnum.USER);
    });

    return true;
  } catch (error) {
    console.error(
      `Error removing member ${userId} from group ${groupId}:`,
      error
    );
    return false;
  }
}

/**
 * Promotes a user to an admin role within a group.
 * If the user is not a member, they are added as an admin.
 * @param groupId The ID of the group (prefixed).
 * @param userId The ID of the user (prefixed) to promote.
 * @param inviterId The ID of the user who is performing the promotion.
 * @param orgId The organization ID (drive ID).
 * @returns True if the user was made an admin or was already an admin, false otherwise.
 */
export async function addAdminToGroup(
  groupId: GroupID,
  userId: UserID,
  inviterId: UserID, // User performing the action
  orgId: string
): Promise<boolean> {
  return addGroupMember(groupId, userId, GroupRole.ADMIN, inviterId, orgId);
}

/**
 * Demotes a user from an admin role to a regular member role within a group.
 * If the user is not an admin (e.g., already a member or not in the group), it does nothing.
 * The user remains a member.
 * @param groupId The ID of the group (prefixed).
 * @param userId The ID of the user (prefixed) to demote.
 * @param inviterId The ID of the user who is performing the demotion.
 * @param orgId The organization ID (drive ID).
 * @returns True if the user was demoted to member or was not an admin, false otherwise.
 */
export async function removeAdminFromGroup(
  groupId: GroupID,
  userId: UserID,
  inviterId: UserID, // User performing the action (not directly used in query but for context)
  orgId: string
): Promise<boolean> {
  const group = await getGroupById(groupId, orgId);
  if (!group) {
    console.warn(`Group not found: ${groupId}`);
    return false;
  }

  // Prevent demoting the group owner
  if (group.owner === userId) {
    console.warn(`Cannot demote group owner ${userId} from group ${groupId}.`);
    return false;
  }

  return addGroupMember(groupId, userId, GroupRole.MEMBER, inviterId, orgId);
}
