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
  const query = `
    SELECT id, name, owner, avatar, private_note, public_note,
           created_at, last_modified_at, drive_id, endpoint_url,
           external_id, external_payload
    FROM groups
    WHERE id = ?;
  `;
  const rows = await db.queryDrive(orgId, query, [groupId]);

  if (rows.length === 0) {
    return undefined;
  }

  const row = rows[0];

  // Fetch admin and member invite IDs separately
  const adminInvites = (
    await db.queryDrive(
      orgId,
      "SELECT id FROM group_invites WHERE group_id = ? AND role = 'ADMIN'",
      [groupId]
    )
  ).map((r) => r.id);

  const memberInvites = (
    await db.queryDrive(
      orgId,
      "SELECT id FROM group_invites WHERE group_id = ?",
      [groupId]
    )
  ).map((r) => r.id);

  return {
    id: row.id as GroupID,
    name: row.name,
    owner: row.owner as UserID,
    avatar: row.avatar || "",
    private_note: row.private_note,
    public_note: row.public_note,
    admin_invites: adminInvites, // Correctly fetched
    member_invites: memberInvites, // Correctly fetched
    created_at: row.created_at,
    last_modified_at: row.last_modified_at,
    drive_id: row.drive_id,
    endpoint_url: row.endpoint_url as URLEndpoint,
    labels: [], // Labels should be fetched from their junction table if needed
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
  const groupOwnerResult = await db.queryDrive(
    orgId,
    "SELECT owner FROM groups WHERE id = ?",
    [groupId]
  );
  if (groupOwnerResult.length > 0 && groupOwnerResult[0].owner === userId) {
    return true;
  }

  const currentTime = Date.now();
  const adminInviteQuery = `
    SELECT 1 FROM group_invites 
    WHERE group_id = ? 
      AND invitee_id = ? 
      AND role = 'ADMIN'
      AND invitee_type = 'USER'
      AND active_from <= ? 
      AND (expires_at <= 0 OR expires_at > ?)
    LIMIT 1;
  `;
  const adminInviteRows = await db.queryDrive(orgId, adminInviteQuery, [
    groupId,
    userId,
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
    inviteeFullId = `${row.invitee_id}` as UserID;
  } else if (row.invitee_type === "PLACEHOLDER" && row.invitee_id) {
    inviteeFullId = `${row.invitee_id}`;
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
    id: `${row.id}` as GroupInviteID,
    group_id: `${row.group_id}` as GroupID,
    inviter_id: `${row.inviter_id}` as UserID,
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
  const groupResult = await db.queryDrive(
    orgId,
    "SELECT owner, endpoint_url FROM groups WHERE id = ?",
    [groupId]
  );
  if (groupResult.length === 0) return false;
  const group = groupResult[0];

  // 1. Check if user is the owner
  if (group.owner === userId) return true;

  const localDriveInfo = await db.queryDrive(
    orgId,
    `SELECT url_endpoint FROM about_drive LIMIT 1;`
  );
  const localDriveEndpoint =
    localDriveInfo.length > 0 ? localDriveInfo[0].url_endpoint : "";

  // Check if it's a local group
  if (group.endpoint_url === localDriveEndpoint) {
    const currentTime = Date.now();
    const inviteQuery = `
      SELECT 1 FROM group_invites 
      WHERE group_id = ? 
        AND invitee_id = ? 
        AND invitee_type = 'USER'
        AND active_from <= ? 
        AND (expires_at <= 0 OR expires_at > ?)
      LIMIT 1;
    `;
    const inviteRows = await db.queryDrive(orgId, inviteQuery, [
      groupId,
      userId,
      currentTime,
      currentTime,
    ]);
    return inviteRows.length > 0;
  } else {
    // External group validation via HTTP call
    const validationUrl = `${group.endpoint_url.replace(
      /\/+$/,
      ""
    )}/groups/validate`;
    try {
      const response = await fetch(validationUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ group_id: groupId, user_id: userId }),
      });

      if (!response.ok) {
        console.error(
          `External group validation failed with status: ${response.status}`
        );
        return false;
      }
      const result = (await response.json()) as IResponseValidateGroupMember; // Assuming IResponseValidateGroupMember is the correct type for the entire response
      return result.ok?.data.is_member === true;
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
  const group = await getGroupById(groupId, orgId);
  if (!group) {
    return false; // Group doesn't exist
  }

  // Check if the user is already in the group (as admin or member)
  const isAlreadyMember = await isUserInGroup(userId, groupId, orgId);
  if (isAlreadyMember) {
    return true; // Already a member, nothing to do
  }

  // User is not in the group, so add them as a member by creating an invite
  const inviteId = `${IDPrefixEnum.GroupInvite}${uuidv4()}` as GroupInviteID;
  const currentTime = Date.now();

  const newInvite: GroupInviteDbRow = {
    id: inviteId,
    group_id: groupId,
    inviter_id: inviterId,
    invitee_type: "USER",
    invitee_id: userId,
    role: "MEMBER",
    note: "Added directly to group",
    active_from: currentTime,
    expires_at: 0, // Never expires
    created_at: currentTime,
    last_modified_at: currentTime,
  };

  const insertQuery = `
    INSERT INTO group_invites (id, group_id, inviter_id, invitee_type, invitee_id, role, note, active_from, expires_at, created_at, last_modified_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
  `;

  try {
    await db.runDrive(orgId, insertQuery, [
      newInvite.id,
      newInvite.group_id,
      newInvite.inviter_id,
      newInvite.invitee_type,
      newInvite.invitee_id,
      newInvite.role,
      newInvite.note,
      newInvite.active_from,
      newInvite.expires_at,
      newInvite.created_at,
      newInvite.last_modified_at,
    ]);
    return true;
  } catch (error) {
    console.error(`Failed to add member to group ${groupId}:`, error);
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
  // First, check if the group exists to avoid doing partial work
  const group = await getGroupById(groupId, orgId);
  if (!group) {
    console.warn(
      `Attempted to remove member from non-existent group: ${groupId}`
    );
    return false; // Or true, depending on desired idempotency
  }

  // Remove all invites for this user to this group, regardless of role
  // This covers both 'MEMBER' and 'ADMIN' roles.
  const deleteInvitesQuery = `
    DELETE FROM group_invites
    WHERE group_id = ? AND invitee_id = ? AND invitee_type = 'USER';
  `;

  try {
    await db.runDrive(orgId, deleteInvitesQuery, [groupId, userId]);
    return true;
  } catch (error) {
    console.error(
      `Failed to remove member ${userId} from group ${groupId}:`,
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
