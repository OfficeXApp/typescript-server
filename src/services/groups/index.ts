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
  id: string; // GroupID without prefix (just UUID part)
  name: string;
  owner: string; // UserID without prefix (just UUID part)
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
  inviter_id: string; // UserID without prefix
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
 * Retrieves a group by its ID.
 * @param groupId The ID of the group (prefixed).
 * @param orgId The organization ID (drive ID) to query the correct database.
 * @returns The Group object if found, otherwise undefined.
 */
export async function getGroupById(
  groupId: GroupID,
  orgId: string
): Promise<Group | undefined> {
  const plainGroupId = groupId; // Use helper to get plain ID
  const query = `
      SELECT
        id, name, owner, avatar, private_note, public_note,
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
    owner: `${IDPrefixEnum.User}${row.owner}` as UserID, // Reconstruct prefixed UserID
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
  const plainUserId = userId;
  const plainGroupId = groupId;

  const adminInviteQuery = `
      SELECT
        gi.id, gi.group_id, gi.inviter_id, gi.invitee_type, gi.invitee_id,
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
  const plainInviteId = inviteId;
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
    inviter_id: `${IDPrefixEnum.User}${row.inviter_id}` as UserID,
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
  const plainUserId = userId;
  const plainGroupId = group.id;

  const userInvitesQuery = `
      SELECT
        gi.id, gi.group_id, gi.inviter_id, gi.invitee_type, gi.invitee_id,
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
          group_id: groupId, // Send plain ID for external API
          user_id: userId, // Send plain ID for external API
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
 * @param inviterId The ID of the user who is performing the addition (for invite record).
 * @param orgId The organization ID (drive ID).
 * @returns True if the member was added or already exists as a member/admin, false otherwise (e.g., group not found).
 */
export async function addMemberToGroup(
  groupId: GroupID,
  userId: UserID,
  inviterId: UserID, // User performing the action
  orgId: string
): Promise<boolean> {
  const plainGroupId = groupId;
  const plainUserId = userId;
  const plainInviterId = inviterId;
  const now = Date.now();

  try {
    // Check if the group exists
    const groupExists = await getGroupById(groupId, orgId);
    if (!groupExists) {
      console.warn(`Group not found: ${groupId}`);
      return false;
    }

    await dbHelpers.transaction("drive", orgId, (database) => {
      // 1. Add/Update contact_groups entry: If user is not in contact_groups, add them as MEMBER.
      //    If they are already ADMIN, do not demote them.
      //    If they are already MEMBER, do nothing.
      database
        .prepare(
          `INSERT INTO contact_groups (user_id, group_id, role)
           VALUES (?, ?, ?)
           ON CONFLICT(user_id, group_id) DO UPDATE SET
             role = COALESCE(
               (SELECT role FROM contact_groups WHERE user_id = ? AND group_id = ?),
               'MEMBER'
             )
           WHERE role IS NOT 'ADMIN';` // Don't change role if already ADMIN
        )
        .run(
          plainUserId,
          plainGroupId,
          GroupRole.MEMBER,
          plainUserId,
          plainGroupId
        );

      // 2. Create or update a group_invites record for this user with MEMBER role.
      //    This is important for tracking how members joined and for system permissions.
      //    We only create an invite if one for this user/group doesn't exist,
      //    or if an existing one is for a different role (e.g., a pending admin invite).
      const existingInvite = database
        .prepare(
          `SELECT id, role FROM group_invites
           WHERE group_id = ? AND invitee_id = ? AND invitee_type = 'USER'`
        )
        .get(plainGroupId, plainUserId) as GroupInviteDbRow | undefined;

      if (!existingInvite) {
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
            plainGroupId,
            plainInviterId,
            plainUserId,
            GroupInviteeTypeEnum.USER,
            GroupRole.MEMBER,
            `Added as member by ${plainInviterId}`,
            now, // Active immediately
            -1, // Never expires
            now,
            now
          );
      } else if (existingInvite.role === GroupRole.ADMIN) {
        // If an ADMIN invite exists, ensure the contact_groups role matches
        database
          .prepare(
            `UPDATE contact_groups SET role = ? WHERE user_id = ? AND group_id = ?`
          )
          .run(GroupRole.ADMIN, plainUserId, plainGroupId);
      } else if (existingInvite.role === GroupRole.MEMBER) {
        // If a MEMBER invite exists, ensure the contact_groups role matches
        database
          .prepare(
            `UPDATE contact_groups SET role = ? WHERE user_id = ? AND group_id = ?`
          )
          .run(GroupRole.MEMBER, plainUserId, plainGroupId);
      }
    });

    return true;
  } catch (error) {
    console.error(`Error adding member ${userId} to group ${groupId}:`, error);
    return false;
  }
}

/**
 * Removes a user from a specific group.
 * This will remove their entry from `contact_groups` and associated `group_invites`.
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
  const plainGroupId = groupId;
  const plainUserId = userId;

  try {
    // Check if the group exists
    const groupExists = await getGroupById(groupId, orgId);
    if (!groupExists) {
      console.warn(`Group not found: ${groupId}`);
      return false;
    }

    await dbHelpers.transaction("drive", orgId, (database) => {
      // 1. Delete from contact_groups
      database
        .prepare(
          `DELETE FROM contact_groups WHERE group_id = ? AND user_id = ?`
        )
        .run(plainGroupId, plainUserId);

      // 2. Delete associated group_invites for this user and group
      database
        .prepare(
          `DELETE FROM group_invites WHERE group_id = ? AND invitee_id = ? AND invitee_type = 'USER'`
        )
        .run(plainGroupId, plainUserId);
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
  const plainGroupId = groupId;
  const plainUserId = userId;
  const plainInviterId = inviterId;
  const now = Date.now();

  try {
    // Check if the group exists
    const groupExists = await getGroupById(groupId, orgId);
    if (!groupExists) {
      console.warn(`Group not found: ${groupId}`);
      return false;
    }

    await dbHelpers.transaction("drive", orgId, (database) => {
      // 1. Add/Update contact_groups entry to ADMIN role.
      //    If the user is not in contact_groups, they are inserted as ADMIN.
      //    If they are already MEMBER, their role is updated to ADMIN.
      //    If they are already ADMIN, their role remains ADMIN.
      database
        .prepare(
          `INSERT INTO contact_groups (user_id, group_id, role)
           VALUES (?, ?, ?)
           ON CONFLICT(user_id, group_id) DO UPDATE SET role = ?;`
        )
        .run(plainUserId, plainGroupId, GroupRole.ADMIN, GroupRole.ADMIN);

      // 2. Create or update a group_invites record for this user with ADMIN role.
      const existingInvite = database
        .prepare(
          `SELECT id FROM group_invites
           WHERE group_id = ? AND invitee_id = ? AND invitee_type = 'USER'`
        )
        .get(plainGroupId, plainUserId) as GroupInviteDbRow | undefined;

      if (existingInvite) {
        // Update existing invite to ADMIN role
        database
          .prepare(
            `UPDATE group_invites SET role = ?, last_modified_at = ? WHERE id = ?`
          )
          .run(GroupRole.ADMIN, now, existingInvite.id);
      } else {
        // Create new invite as ADMIN
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
            plainGroupId,
            plainInviterId,
            plainUserId,
            GroupInviteeTypeEnum.USER,
            GroupRole.ADMIN,
            `Promoted to admin by ${plainInviterId}`,
            now, // Active immediately
            -1, // Never expires
            now,
            now
          );
      }
    });

    return true;
  } catch (error) {
    console.error(`Error adding admin ${userId} to group ${groupId}:`, error);
    return false;
  }
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
  inviterId: UserID, // User performing the action
  orgId: string
): Promise<boolean> {
  const plainGroupId = groupId;
  const plainUserId = userId;
  const plainInviterId = inviterId;
  const now = Date.now();

  try {
    // Check if the group exists
    const groupExists = await getGroupById(groupId, orgId);
    if (!groupExists) {
      console.warn(`Group not found: ${groupId}`);
      return false;
    }

    // Prevent demoting the group owner
    if (groupExists.owner === userId) {
      console.warn(
        `Cannot demote group owner ${userId} from group ${groupId}.`
      );
      return false;
    }

    await dbHelpers.transaction("drive", orgId, (database) => {
      // 1. Update contact_groups entry to MEMBER role.
      //    Only update if the current role is ADMIN.
      database
        .prepare(
          `UPDATE contact_groups SET role = ? WHERE user_id = ? AND group_id = ? AND role = ?`
        )
        .run(GroupRole.MEMBER, plainUserId, plainGroupId, GroupRole.ADMIN);

      // 2. Update existing group_invites record for this user to MEMBER role.
      //    If no invite exists, nothing needs to be done here for the invite,
      //    as the goal is to remove admin status, not remove from group entirely.
      database
        .prepare(
          `UPDATE group_invites SET role = ?, last_modified_at = ?
           WHERE group_id = ? AND invitee_id = ? AND invitee_type = 'USER' AND role = ?`
        )
        .run(GroupRole.MEMBER, now, plainGroupId, plainUserId, GroupRole.ADMIN);
    });

    return true;
  } catch (error) {
    console.error(
      `Error removing admin ${userId} from group ${groupId}:`,
      error
    );
    return false;
  }
}
