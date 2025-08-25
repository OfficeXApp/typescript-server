// src/rest/group_invites/handler.ts

import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import {
  GroupInvite,
  GroupInviteFE,
  ISuccessResponse,
  IRequestCreateGroupInvite,
  IRequestUpdateGroupInvite,
  IRequestDeleteGroupInvite,
  IResponseDeleteGroupInvite,
  IRequestRedeemGroupInvite,
  IResponseRedeemGroupInvite,
  IDPrefixEnum,
  GroupInviteID,
  GroupID,
  UserID,
  GroupRole,
  SystemResourceID,
  SystemTableValueEnum,
  SystemPermissionType,
  DriveID,
  GroupInviteeTypeEnum,
  SortDirection,
  IResponseGetGroupInvite,
  IResponseListGroupInvites,
  IResponseCreateGroupInvite,
  IResponseUpdateGroupInvite,
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import { authenticateRequest } from "../../../../services/auth";
import { createApiResponse, OrgIdParams } from "../../types";
import {
  checkSystemPermissions,
  // hasSystemManagePermission, // Not used directly for core logic
} from "../../../../services/permissions/system";
import {
  getGroupById,
  getGroupInviteById,
  isGroupAdmin,
  addGroupMember,
  removeMemberFromGroup,
  isUserInGroup, // Added for consistency
} from "../../../../services/groups";
import { claimUUID, isUUIDClaimed } from "../../../../services/external";
import { trackEvent } from "../../../../services/analytics";

interface GetGroupInviteParams extends OrgIdParams {
  invite_id: string;
}

interface ListGroupInvitesBody {
  group_id: string;
  filters?: string;
  page_size?: number;
  direction?: "ASC" | "DESC";
  cursor?: string;
}

async function validateCreateRequest(
  body: IRequestCreateGroupInvite,
  orgID: DriveID
): Promise<{ valid: boolean; error?: string }> {
  if (body.id) {
    const is_claimed = await isUUIDClaimed(body.id, orgID);
    if (is_claimed) {
      return {
        valid: false,
        error: "UUID is already claimed",
      };
    }
  }

  if (!body.group_id || !body.group_id.startsWith(IDPrefixEnum.Group)) {
    return { valid: false, error: "Group ID must start with GroupID_" };
  }

  if (
    body.invitee_id &&
    !body.invitee_id.startsWith(IDPrefixEnum.User) &&
    body.invitee_id !== GroupInviteeTypeEnum.PUBLIC &&
    !body.invitee_id.startsWith(IDPrefixEnum.PlaceholderGroupInviteeID)
  ) {
    return {
      valid: false,
      error:
        "Invitee ID must start with UserID_, PlaceholderGroupInviteeID_, or be PUBLIC",
    };
  }

  if (body.note && body.note.length > 8192) {
    return { valid: false, error: "Note must be less than 8192 characters" };
  }

  return { valid: true };
}

export async function getGroupInviteHandler(
  request: FastifyRequest<{
    Params: GetGroupInviteParams;
    Reply: IResponseGetGroupInvite;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const requesterApiKey = await authenticateRequest(
      request,
      "drive",
      request.params.org_id
    );
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const inviteId = request.params.invite_id as GroupInviteID;
    const currentUserId = requesterApiKey.user_id;
    const orgId = request.params.org_id as DriveID;

    // Get the invite
    const invite = await getGroupInviteById(inviteId, orgId);

    if (!invite) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group invite not found",
        })
      );
    }

    // Get group information
    const group = await getGroupById(invite.group_id, orgId);

    if (!group) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Associated group not found",
        })
      );
    }

    // PERMIT: Check if requester is the org owner, inviter, invitee, or has VIEW permission on this specific group invite record
    const isOrgOwner =
      (
        await db.queryDrive(orgId, "SELECT owner_id FROM about_drive LIMIT 1")
      )[0]?.owner_id === currentUserId;

    const isInviter = currentUserId === invite.inviter_id;
    const isInvitee = currentUserId === invite.invitee_id; // Check for direct invitee match

    // Check if the current user is an admin of the group related to this invite
    const isGroupAdminStatus = await isGroupAdmin(
      currentUserId,
      invite.group_id,
      orgId
    );

    const canViewInviteViaPermissions = (
      await checkSystemPermissions({
        resourceTable: `TABLE_${SystemTableValueEnum.GROUPS}`,
        resourceId: `${invite.group_id}` as SystemResourceID,
        granteeId: currentUserId,
        orgId: orgId,
      })
    ).includes(SystemPermissionType.VIEW);

    if (
      !isOrgOwner &&
      !isInviter &&
      !isInvitee &&
      !isGroupAdminStatus && // Added: Group admins can view invites
      !canViewInviteViaPermissions
    ) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Forbidden: Not authorized to view this invite",
        })
      );
    }

    // Get group and invitee info for FE
    const groupInfo = await db.queryDrive(
      orgId,
      "SELECT name, avatar FROM groups WHERE id = ?",
      [invite.group_id]
    );

    let inviteeName = "";
    let inviteeAvatar = undefined;

    if (invite.invitee_id && invite.invitee_id.startsWith(IDPrefixEnum.User)) {
      const inviteeInfo = await db.queryDrive(
        orgId,
        "SELECT name, avatar FROM contacts WHERE id = ?",
        [invite.invitee_id]
      );
      if (inviteeInfo.length > 0) {
        inviteeName = inviteeInfo[0].name;
        inviteeAvatar = inviteeInfo[0].avatar;
      } else {
        inviteeName = `Unknown User (${invite.invitee_id})`;
      }
    } else if (invite.invitee_id === GroupInviteeTypeEnum.PUBLIC) {
      inviteeName = "Public";
    } else if (
      invite.invitee_id &&
      invite.invitee_id.startsWith(IDPrefixEnum.PlaceholderGroupInviteeID)
    ) {
      inviteeName = "Awaiting Anon";
    }

    // PERMIT: Get permission previews for the current user on this group invite record
    const permissionPreviews = await checkSystemPermissions({
      resourceTable: `TABLE_${SystemTableValueEnum.GROUPS}`,
      resourceId: `${invite.group_id}` as SystemResourceID,
      granteeId: requesterApiKey.user_id,
      orgId: orgId,
    });

    const inviteFE: GroupInviteFE = {
      ...invite,
      group_name: groupInfo[0]?.name || "",
      group_avatar: groupInfo[0]?.avatar,
      invitee_id: invite.invitee_id, // Ensure it's the original string ID
      invitee_name: inviteeName,
      invitee_avatar: inviteeAvatar,
      permission_previews: permissionPreviews,
    };

    return reply.status(200).send(createApiResponse(inviteFE));
  } catch (error) {
    request.log.error("Error in getGroupInviteHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function listGroupInvitesHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: ListGroupInvitesBody;
    Reply: IResponseListGroupInvites;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const requesterApiKey = await authenticateRequest(
      request,
      "drive",
      request.params.org_id
    );
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const body = request.body;
    const pageSize = body.page_size || 50;
    const direction = body.direction || SortDirection.DESC;
    const currentUserId = requesterApiKey.user_id;
    const orgId = request.params.org_id as DriveID;

    // Check if group exists
    const group = await getGroupById(body.group_id as GroupID, orgId);

    if (!group) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group not found",
        })
      );
    }

    const permissions = await checkSystemPermissions({
      resourceTable: `TABLE_${SystemTableValueEnum.GROUPS}`,
      resourceId: group.id,
      granteeId: currentUserId,
      orgId: orgId,
    });
    const isPartOfGroup = await isUserInGroup(currentUserId, group.id, orgId);

    if (!permissions.includes(SystemPermissionType.VIEW) && !isPartOfGroup) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Forbidden: Not authorized to list group invites",
        })
      );
    }

    // Build query with cursor-based pagination
    // Modified JOIN to contacts to use `gi.invitee_id` as `c.id`
    let query = `
        SELECT gi.*, g.name as group_name, g.avatar as group_avatar,
              c.name as invitee_name, c.avatar as invitee_avatar
        FROM group_invites gi
        JOIN groups g ON gi.group_id = g.id
        LEFT JOIN contacts c ON gi.invitee_id = c.id AND gi.invitee_type = ? -- Only join if it's a USER type invitee
        WHERE gi.group_id = ?
      `;
    const params: any[] = [GroupInviteeTypeEnum.USER, body.group_id];

    if (body.cursor) {
      query += ` AND gi.created_at ${direction === "ASC" ? ">" : "<"} ?`;
      params.push(body.cursor);
    }

    query += ` ORDER BY gi.created_at ${direction} LIMIT ?`;
    params.push(pageSize + 1);

    const invites = await db.queryDrive(orgId, query, params);

    const hasMore = invites.length > pageSize;
    if (hasMore) {
      invites.pop();
    }

    // Get total count
    const countResult = await db.queryDrive(
      orgId,
      "SELECT COUNT(*) as total FROM group_invites WHERE group_id = ?",
      [body.group_id]
    );
    const total = countResult[0]?.total || 0;

    const invitesFE: GroupInviteFE[] = await Promise.all(
      invites.map(async (invite: any) => {
        let inviteeName = "";
        let inviteeAvatar = undefined;

        if (invite.invitee_name) {
          // Already fetched from JOIN
          inviteeName = invite.invitee_name;
          inviteeAvatar = invite.invitee_avatar;
        } else if (invite.invitee_type === GroupInviteeTypeEnum.PUBLIC) {
          inviteeName = "Public";
        } else if (
          invite.invitee_type === GroupInviteeTypeEnum.PLACEHOLDER_GROUP_INVITEE
        ) {
          inviteeName = "Awaiting Anon";
        } else {
          // Fallback for any other unexpected type or missing name
          inviteeName = invite.invitee_id;
        }

        // PERMIT: Get permission previews for the current user on each group invite record
        const permissionPreviews = Array.from(
          new Set<SystemPermissionType>([
            ...permissions,
            SystemPermissionType.VIEW,
          ])
        );

        return {
          id: invite.id,
          group_id: invite.group_id,
          inviter_id: invite.inviter_id,
          invitee_id: invite.invitee_id, // Use the full string ID from DB
          role: invite.role,
          note: invite.note,
          active_from: invite.active_from,
          expires_at: invite.expires_at,
          created_at: invite.created_at,
          last_modified_at: invite.last_modified_at,
          from_placeholder_invitee: invite.from_placeholder_invitee,
          labels: [], // Labels explicitly ignored
          redeem_code: invite.redeem_code,
          external_id: invite.external_id,
          external_payload: invite.external_payload,
          group_name: invite.group_name || "",
          group_avatar: invite.group_avatar,
          invitee_name: inviteeName,
          invitee_avatar: inviteeAvatar,
          permission_previews: permissionPreviews,
        };
      })
    );

    const nextCursor =
      hasMore && invites.length > 0
        ? invites[invites.length - 1].created_at.toString()
        : null;

    return reply.status(200).send(
      createApiResponse({
        items: invitesFE,
        page_size: pageSize,
        total,
        direction,
        cursor: nextCursor,
      })
    );
  } catch (error) {
    request.log.error("Error in listGroupInvitesHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function createGroupInviteHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestCreateGroupInvite;
    Reply: IResponseCreateGroupInvite;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const requesterApiKey = await authenticateRequest(
      request,
      "drive",
      request.params.org_id
    );
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const body = request.body;
    const currentUserId = requesterApiKey.user_id;
    const orgId = request.params.org_id as DriveID;

    // Validate request
    const validation = await validateCreateRequest(body, orgId);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    // Check if group exists
    const group = await getGroupById(body.group_id as GroupID, orgId);

    if (!group) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group not found",
        })
      );
    }

    // PERMIT: Check if requester is the org owner, a group admin, or has INVITE permission on the group record.
    const isOrgOwner =
      (
        await db.queryDrive(orgId, "SELECT owner_id FROM about_drive LIMIT 1")
      )[0]?.owner_id === currentUserId;

    const isGroupAdminStatus = await isGroupAdmin(
      currentUserId,
      group.id,
      orgId
    );

    const canInviteToGroupViaPermissions = (
      await checkSystemPermissions({
        resourceTable: `TABLE_${SystemTableValueEnum.GROUPS}`,
        resourceId: `${group.id}` as SystemResourceID,
        granteeId: requesterApiKey.user_id,
        orgId: orgId,
      })
    ).includes(SystemPermissionType.INVITE);

    if (!isOrgOwner && !isGroupAdminStatus && !canInviteToGroupViaPermissions) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Forbidden: Not allowed to create invites for this group",
        })
      );
    }

    const now = Date.now();
    const inviteId = body.id || `${IDPrefixEnum.GroupInvite}${uuidv4()}`;

    // Handle invitee_id - could be user, placeholder, or public
    let inviteeIdString:
      | GroupInviteeTypeEnum
      | UserID
      | `PlaceholderGroupInviteeID_${string}`;
    let dbInviteeId: string | null; // Value to store in group_invites.invitee_id
    let dbInviteeType: string; // Value to store in group_invites.invitee_type
    let redeemCode = undefined;
    let fromPlaceholder = undefined;

    if (!body.invitee_id) {
      // Create placeholder invite if invitee_id is not provided
      const placeholderId = `${IDPrefixEnum.PlaceholderGroupInviteeID}${uuidv4()}`;
      inviteeIdString = placeholderId as `PlaceholderGroupInviteeID_${string}`;
      dbInviteeId = placeholderId;
      dbInviteeType = GroupInviteeTypeEnum.PLACEHOLDER_GROUP_INVITEE;
      redeemCode = `REDEEM_${Date.now()}`; // Generate a redeem code for placeholder invites
    } else if (body.invitee_id === GroupInviteeTypeEnum.PUBLIC) {
      inviteeIdString = GroupInviteeTypeEnum.PUBLIC;
      dbInviteeId = GroupInviteeTypeEnum.PUBLIC; // Store "PUBLIC" string in invitee_id column
      dbInviteeType = GroupInviteeTypeEnum.PUBLIC;
      redeemCode = GroupInviteeTypeEnum.PUBLIC; // Public invites have a static redeem code
    } else if (body.invitee_id.startsWith(IDPrefixEnum.User)) {
      inviteeIdString = body.invitee_id as UserID;
      dbInviteeId = body.invitee_id;
      dbInviteeType = GroupInviteeTypeEnum.USER;
    } else if (
      body.invitee_id.startsWith(IDPrefixEnum.PlaceholderGroupInviteeID)
    ) {
      inviteeIdString =
        body.invitee_id as `PlaceholderGroupInviteeID_${string}`;
      dbInviteeId = body.invitee_id;
      dbInviteeType = GroupInviteeTypeEnum.PLACEHOLDER_GROUP_INVITEE;
    } else {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Invalid invitee_id format",
        })
      );
    }

    const invite: GroupInvite = {
      id: inviteId as GroupInviteID,
      group_id: body.group_id as GroupID,
      inviter_id: requesterApiKey.user_id,
      invitee_id: inviteeIdString,
      role: body.role || GroupRole.MEMBER,
      note: body.note || "",
      active_from: body.active_from || 0,
      expires_at: body.expires_at || -1,
      created_at: now,
      last_modified_at: now,
      redeem_code: redeemCode,
      from_placeholder_invitee: fromPlaceholder,
      labels: [], // Labels explicitly ignored
      external_id: body.external_id,
      external_payload: body.external_payload,
    };

    // Insert invite using transaction
    await dbHelpers.transaction("drive", orgId, (database) => {
      claimUUID(database, invite.id);

      const stmt = database.prepare(
        `INSERT INTO group_invites (
            id, group_id, inviter_id, invitee_id, invitee_type, role, note,
            active_from, expires_at, created_at, last_modified_at,
            redeem_code, from_placeholder_invitee, external_id, external_payload
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );

      stmt.run(
        invite.id,
        invite.group_id,
        invite.inviter_id,
        dbInviteeId, // Store the determined invitee_id (full string or "PUBLIC")
        dbInviteeType, // Store the determined invitee_type
        invite.role,
        invite.note,
        invite.active_from,
        invite.expires_at,
        invite.created_at,
        invite.last_modified_at,
        invite.redeem_code,
        invite.from_placeholder_invitee,
        invite.external_id,
        invite.external_payload
      );
    });

    // Get group info for FE response
    const groupInfo = await db.queryDrive(
      orgId,
      "SELECT name, avatar FROM groups WHERE id = ?",
      [body.group_id]
    );

    let inviteeName = "";
    let inviteeAvatar = undefined;

    if (inviteeIdString && inviteeIdString.startsWith(IDPrefixEnum.User)) {
      const inviteeInfo = await db.queryDrive(
        orgId,
        "SELECT name, avatar FROM contacts WHERE id = ?",
        [inviteeIdString]
      );
      if (inviteeInfo.length > 0) {
        inviteeName = inviteeInfo[0].name;
        inviteeAvatar = inviteeInfo[0].avatar;
      } else {
        inviteeName = `Unknown User (${inviteeIdString})`;
      }
    } else if (inviteeIdString === GroupInviteeTypeEnum.PUBLIC) {
      inviteeName = "Public";
    } else if (
      inviteeIdString.startsWith(IDPrefixEnum.PlaceholderGroupInviteeID)
    ) {
      inviteeName = "Awaiting Anon";
    }

    // PERMIT: Get permission previews for the current user on the newly created invite record
    const permissionPreviews = await checkSystemPermissions({
      resourceTable: `TABLE_${SystemTableValueEnum.GROUPS}`,
      granteeId: requesterApiKey.user_id,
      orgId: orgId,
    });

    const inviteFE: GroupInviteFE = {
      ...invite,
      invitee_id: inviteeIdString as string, // Ensure it's the original string ID for FE
      group_name: groupInfo[0]?.name || "",
      group_avatar: groupInfo[0]?.avatar,
      invitee_name: inviteeName,
      invitee_avatar: inviteeAvatar,
      permission_previews: permissionPreviews,
    };

    trackEvent("create_group_invite", {
      group_id: group.id,
      invite_id: invite.id,
      invitee_id: inviteeIdString,
      drive_id: orgId,
    });

    return reply.status(200).send(createApiResponse(inviteFE));
  } catch (error) {
    request.log.error("Error in createGroupInviteHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function updateGroupInviteHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestUpdateGroupInvite;
    Reply: IResponseUpdateGroupInvite;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const requesterApiKey = await authenticateRequest(
      request,
      "drive",
      request.params.org_id
    );
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const body = request.body;
    const currentUserId = requesterApiKey.user_id;
    const orgId = request.params.org_id as DriveID;

    // Get existing invite
    const invite = await getGroupInviteById(body.id as GroupInviteID, orgId);

    if (!invite) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group invite not found",
        })
      );
    }

    // Get group information
    const group = await getGroupById(invite.group_id, orgId);

    if (!group) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Associated group not found",
        })
      );
    }

    // PERMIT: Check if requester is the org owner, inviter, a group admin, or has EDIT permission on the group invite record
    const isOrgOwner =
      (
        await db.queryDrive(orgId, "SELECT owner_id FROM about_drive LIMIT 1")
      )[0]?.owner_id === currentUserId;

    const isInviter = currentUserId === invite.inviter_id;

    const isGroupAdminStatus = await isGroupAdmin(
      currentUserId,
      group.id,
      orgId
    );

    const canEditInviteViaPermissions = (
      await checkSystemPermissions({
        resourceTable: `TABLE_${SystemTableValueEnum.GROUPS}`,
        resourceId: `${invite.group_id}` as SystemResourceID,
        granteeId: requesterApiKey.user_id,
        orgId: orgId,
      })
    ).includes(SystemPermissionType.EDIT);

    if (
      !isOrgOwner &&
      !isInviter &&
      !isGroupAdminStatus && // Added: Group admins can edit invites
      !canEditInviteViaPermissions
    ) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Forbidden: Not authorized to edit this invite",
        })
      );
    }

    // Build update query
    const updates: string[] = [];
    const values: any[] = [];

    if (body.role !== undefined) {
      updates.push("role = ?");
      values.push(body.role);
    }
    if (body.active_from !== undefined) {
      updates.push("active_from = ?");
      values.push(body.active_from);
    }
    if (body.expires_at !== undefined) {
      updates.push("expires_at = ?");
      values.push(body.expires_at);
    }
    if (body.note !== undefined) {
      updates.push("note = ?");
      values.push(body.note);
    }
    if (body.external_id !== undefined) {
      updates.push("external_id = ?");
      values.push(body.external_id);
    }
    if (body.external_payload !== undefined) {
      updates.push("external_payload = ?");
      values.push(body.external_payload);
    }

    if (updates.length === 0) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "No fields to update",
        })
      );
    }

    updates.push("last_modified_at = ?");
    values.push(Date.now());
    values.push(invite.id);

    // Update in transaction
    await dbHelpers.transaction("drive", orgId, (database) => {
      const stmt = database.prepare(
        `UPDATE group_invites SET ${updates.join(", ")} WHERE id = ?`
      );
      stmt.run(...values);
    });

    // Get updated invite with additional info
    const updatedInvite = await getGroupInviteById(invite.id, orgId);

    if (!updatedInvite) {
      return reply.status(500).send(
        createApiResponse(undefined, {
          code: 500,
          message: "Failed to retrieve updated invite",
        })
      );
    }

    // PERMIT: Get permission previews for the current user on the updated invite record
    const permissionPreviews = await checkSystemPermissions({
      resourceTable: `TABLE_${SystemTableValueEnum.GROUPS}`,
      resourceId: `${updatedInvite.group_id}` as SystemResourceID,
      granteeId: requesterApiKey.user_id,
      orgId: orgId,
    });

    let inviteeName = "";
    let inviteeAvatar = undefined;
    if (updatedInvite.invitee_id.startsWith(IDPrefixEnum.User)) {
      const contactInfo = await db.queryDrive(
        orgId,
        "SELECT name, avatar FROM contacts WHERE id = ?",
        [updatedInvite.invitee_id]
      );
      if (contactInfo.length > 0) {
        inviteeName = contactInfo[0].name;
        inviteeAvatar = contactInfo[0].avatar;
      } else {
        inviteeName = `Unknown User (${updatedInvite.invitee_id})`;
      }
    } else if (updatedInvite.invitee_id === GroupInviteeTypeEnum.PUBLIC) {
      inviteeName = "Public";
    } else if (
      updatedInvite.invitee_id.startsWith(
        IDPrefixEnum.PlaceholderGroupInviteeID
      )
    ) {
      inviteeName = "Awaiting Anon";
    }

    const inviteFE: GroupInviteFE = {
      ...updatedInvite,
      group_name: group.name,
      group_avatar: group.avatar,
      invitee_id: updatedInvite.invitee_id, // Ensure it's the original string ID
      invitee_name: inviteeName,
      invitee_avatar: inviteeAvatar,
      permission_previews: permissionPreviews,
    };

    return reply.status(200).send(createApiResponse(inviteFE));
  } catch (error) {
    request.log.error("Error in updateGroupInviteHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function deleteGroupInviteHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestDeleteGroupInvite;
    Reply: IResponseDeleteGroupInvite;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const requesterApiKey = await authenticateRequest(
      request,
      "drive",
      request.params.org_id
    );
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const body = request.body;
    const currentUserId = requesterApiKey.user_id;
    const orgId = request.params.org_id as DriveID;

    // Get invite to check permissions
    const invite = await getGroupInviteById(body.id as GroupInviteID, orgId);

    if (!invite) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group invite not found",
        })
      );
    }

    // Get group information
    const group = await getGroupById(invite.group_id, orgId);

    if (!group) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Associated group not found",
        })
      );
    }

    // PERMIT: Check if requester is the org owner, inviter, a group admin, or has DELETE permission on the group invite record
    const isOrgOwner =
      (
        await db.queryDrive(orgId, "SELECT owner_id FROM about_drive LIMIT 1")
      )[0]?.owner_id === currentUserId;

    const isInviter = currentUserId === invite.inviter_id;

    const isGroupAdminStatus = await isGroupAdmin(
      currentUserId,
      group.id,
      orgId
    );

    const canDeleteInviteViaPermissions = (
      await checkSystemPermissions({
        resourceTable: `TABLE_${SystemTableValueEnum.GROUPS}`,
        resourceId: `${invite.group_id}` as SystemResourceID,
        granteeId: requesterApiKey.user_id,
        orgId: orgId,
      })
    ).includes(SystemPermissionType.DELETE);

    if (
      !isOrgOwner &&
      !isInviter &&
      !isGroupAdminStatus && // Added: Group admins can delete invites
      !canDeleteInviteViaPermissions
    ) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Forbidden: Not allowed to delete this invite",
        })
      );
    }

    // Delete invite and remove user from group if needed (if it was a direct user invite)
    await dbHelpers.transaction("drive", orgId, (database) => {
      // If the invite was a direct user invite, remove the user from the group.
      // This implicitly handles both member and admin roles, as group_invites is the source of truth.
      if (invite.invitee_id.startsWith(IDPrefixEnum.User)) {
        database
          .prepare(
            `DELETE FROM group_invites WHERE group_id = ? AND invitee_id = ? AND invitee_type = ?`
          )
          .run(invite.group_id, invite.invitee_id, GroupInviteeTypeEnum.USER);
      } else {
        // For placeholder or public invites, just delete the invite record itself.
        database
          .prepare("DELETE FROM group_invites WHERE id = ?")
          .run(invite.id);
      }

      // Also remove any system permissions associated with this invite ID as a resource.
      // This is for permissions explicitly granted to the invite record itself.
      database
        .prepare("DELETE FROM permissions_system WHERE resource_identifier = ?")
        .run(invite.id);
    });

    const deletedData: IResponseDeleteGroupInvite = {
      ok: {
        data: {
          id: body.id,
          deleted: true,
        },
      },
    };

    return reply.status(200).send(deletedData);
  } catch (error) {
    request.log.error("Error in deleteGroupInviteHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function redeemGroupInviteHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestRedeemGroupInvite;
    Reply: IResponseRedeemGroupInvite;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const requesterApiKey = await authenticateRequest(
      request,
      "drive",
      request.params.org_id
    );
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const body = request.body;
    const currentUserId = requesterApiKey.user_id;
    const orgId = request.params.org_id as DriveID;

    // Get the invite
    const invite = await getGroupInviteById(
      body.invite_id as GroupInviteID,
      orgId
    );

    if (!invite) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group invite not found",
        })
      );
    }

    // Validate redeem code
    if (!invite.redeem_code || invite.redeem_code !== body.redeem_code) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Invalid redeem code. It may already been redeemed.",
        })
      );
    }

    // PERMIT: User attempting to redeem must be the intended invitee if it's a specific user invite.
    // For public and placeholder invites, anyone can attempt to redeem.
    if (
      invite.invitee_id.startsWith(IDPrefixEnum.User) &&
      invite.invitee_id !== currentUserId
    ) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Forbidden: This invite is not for you",
        })
      );
    }

    const now = Date.now();

    // The user to be added to the group is the currentUserId (from API key)
    const newMemberId = currentUserId;

    // Handle different invite types
    if (invite.invitee_id === GroupInviteeTypeEnum.PUBLIC) {
      // For public invites, create a new invite for the specific user
      // and add them to the group with the original invite's role.
      const newInviteId = `${IDPrefixEnum.GroupInvite}${uuidv4()}`;

      await dbHelpers.transaction("drive", orgId, (database) => {
        // Create new invite for the specific user
        const stmt = database.prepare(
          `INSERT INTO group_invites (
              id, group_id, inviter_id, invitee_id, invitee_type, role, note,
              active_from, expires_at, created_at, last_modified_at,
              redeem_code, from_placeholder_invitee, external_id, external_payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
        );

        const userNote = body.note
          ? `Note from User: ${body.note}, Prior Original Note: ${invite.note}`
          : invite.note;

        stmt.run(
          newInviteId,
          invite.group_id,
          invite.inviter_id,
          newMemberId,
          GroupInviteeTypeEnum.USER,
          invite.role, // Maintain the original role from the public invite
          userNote,
          invite.active_from,
          invite.expires_at,
          now,
          now,
          null, // Clear redeem code for the new specific invite
          invite.id, // Reference the original public invite as from_placeholder_invitee
          invite.external_id,
          invite.external_payload
        );

        // Ensure a contact exists for the user, creating one with default values if not.
        // This is crucial since UserIDs can exist outside the contacts table.
        const contactExistsStmt = database.prepare(
          `SELECT id FROM contacts WHERE id = ?`
        );
        const existingContact = contactExistsStmt.get(newMemberId);

        if (!existingContact) {
          const createContactStmt = database.prepare(
            `INSERT INTO contacts (
                id, name, evm_public_address, icp_principal, created_at, last_online_ms
            ) VALUES (?, ?, ?, ?, ?, ?)`
          );
          createContactStmt.run(
            newMemberId, // id
            newMemberId, // name (default to user_id)
            "", // evm_public_address (default empty)
            newMemberId.replace("UserID_", ""), // icp_principal (default empty)
            now, // created_at
            now // last_online_ms
          );
        }
      });

      // Get the new invite with additional info
      const newInvite = await getGroupInviteById(
        newInviteId as GroupInviteID,
        orgId
      );

      if (!newInvite) {
        return reply.status(500).send(
          createApiResponse(undefined, {
            code: 500,
            message: "Failed to retrieve new invite after public redemption",
          })
        );
      }

      let inviteeName = "";
      let inviteeAvatar = undefined;
      if (newInvite.invitee_id.startsWith(IDPrefixEnum.User)) {
        const contactInfo = await db.queryDrive(
          orgId,
          "SELECT name, avatar FROM contacts WHERE id = ?",
          [newInvite.invitee_id]
        );
        if (contactInfo.length > 0) {
          inviteeName = contactInfo[0].name;
          inviteeAvatar = contactInfo[0].avatar;
        } else {
          inviteeName = `Unknown User (${newInvite.invitee_id})`;
        }
      }

      const responseData = {
        invite: {
          ...newInvite,
        },
      };

      trackEvent("redeem_group_invite", {
        group_id: invite.group_id,
        invite_id: invite.id,
        invitee_id: currentUserId,
        drive_id: orgId,
      });

      return reply.status(200).send(createApiResponse(responseData));
    } else if (
      invite.invitee_id.startsWith(IDPrefixEnum.PlaceholderGroupInviteeID)
    ) {
      // For placeholder invites, update the existing invite
      if (invite.from_placeholder_invitee) {
        return reply.status(400).send(
          createApiResponse(undefined, {
            code: 400,
            message: "Invite has already been redeemed",
          })
        );
      }

      await dbHelpers.transaction("drive", orgId, (database) => {
        // Update invite
        const userNote = body.note
          ? `Note from User: ${body.note}, Prior Original Note: ${invite.note}`
          : invite.note;

        const stmt = database.prepare(
          `UPDATE group_invites
               SET invitee_id = ?, invitee_type = ?, role = ?, note = ?,
                   last_modified_at = ?, redeem_code = NULL, from_placeholder_invitee = ?
               WHERE id = ?`
        );
        stmt.run(
          newMemberId, // New UserID
          GroupInviteeTypeEnum.USER,
          invite.role, // Maintain the original role from the placeholder invite
          userNote,
          now,
          invite.invitee_id, // Store the original placeholder ID here
          invite.id
        );

        // Ensure a contact exists for the user, creating one with default values if not.
        const contactExistsStmt = database.prepare(
          `SELECT id FROM contacts WHERE id = ?`
        );
        const existingContact = contactExistsStmt.get(newMemberId);

        if (!existingContact) {
          const createContactStmt = database.prepare(
            `INSERT INTO contacts (
                id, name, evm_public_address, icp_principal, created_at, last_online_ms
            ) VALUES (?, ?, ?, ?, ?, ?)`
          );
          createContactStmt.run(
            newMemberId, // id
            newMemberId, // name (default to user_id)
            "", // evm_public_address
            newMemberId.replace("UserID_", ""), // icp_principal
            now, // created_at
            now // last_online_ms
          );
        }
      });

      // Get updated invite
      const updatedInvite = await getGroupInviteById(invite.id, orgId);

      if (!updatedInvite) {
        return reply.status(500).send(
          createApiResponse(undefined, {
            code: 500,
            message:
              "Failed to retrieve updated invite after placeholder redemption",
          })
        );
      }

      let inviteeName = "";
      let inviteeAvatar = undefined;
      if (updatedInvite.invitee_id.startsWith(IDPrefixEnum.User)) {
        const contactInfo = await db.queryDrive(
          orgId,
          "SELECT name, avatar FROM contacts WHERE id = ?",
          [updatedInvite.invitee_id]
        );
        if (contactInfo.length > 0) {
          inviteeName = contactInfo[0].name;
          inviteeAvatar = contactInfo[0].avatar;
        } else {
          inviteeName = `Unknown User (${updatedInvite.invitee_id})`;
        }
      }

      const responseData = {
        invite: {
          ...updatedInvite,
        },
      };

      trackEvent("redeem_group_invite", {
        group_id: invite.group_id,
        invite_id: invite.id,
        invitee_id: currentUserId,
        drive_id: orgId,
      });

      return reply.status(200).send(createApiResponse(responseData));
    } else if (invite.invitee_id.startsWith(IDPrefixEnum.User)) {
      // If it's a direct user invite, and the user matches the API key,
      // the "redemption" means confirming their membership.
      // The `invitee_id` in the DB already holds the UserID.
      // No change to the invite record itself, but ensure contact exists.
      await dbHelpers.transaction("drive", orgId, (database) => {
        // Ensure a contact exists for the user, creating one with default values if not.
        const contactExistsStmt = database.prepare(
          `SELECT id FROM contacts WHERE id = ?`
        );
        const existingContact = contactExistsStmt.get(newMemberId);

        if (!existingContact) {
          const createContactStmt = database.prepare(
            `INSERT INTO contacts (
                id, name, evm_public_address, icp_principal, created_at, last_online_ms
            ) VALUES (?, ?, ?, ?, ?, ?)`
          );
          createContactStmt.run(
            newMemberId, // id
            newMemberId, // name (default to user_id)
            "", // evm_public_address
            newMemberId.replace("UserID_", ""), // icp_principal
            now, // created_at
            now // last_online_ms
          );
        }
      });

      // Get updated invite (no change to invite record itself, just re-fetch for response)
      const redeemedInvite = await getGroupInviteById(invite.id, orgId);

      if (!redeemedInvite) {
        return reply.status(500).send(
          createApiResponse(undefined, {
            code: 500,
            message: "Failed to retrieve invite after user redemption",
          })
        );
      }

      let inviteeName = "";
      let inviteeAvatar = undefined;
      if (redeemedInvite.invitee_id.startsWith(IDPrefixEnum.User)) {
        const contactInfo = await db.queryDrive(
          orgId,
          "SELECT name, avatar FROM contacts WHERE id = ?",
          [redeemedInvite.invitee_id]
        );
        if (contactInfo.length > 0) {
          inviteeName = contactInfo[0].name;
          inviteeAvatar = contactInfo[0].avatar;
        } else {
          inviteeName = `Unknown User (${redeemedInvite.invitee_id})`;
        }
      }

      const responseData = {
        invite: {
          ...redeemedInvite,
        },
      };

      trackEvent("redeem_group_invite", {
        group_id: invite.group_id,
        invite_id: invite.id,
        invitee_id: currentUserId,
        drive_id: orgId,
      });
      return reply.status(200).send(createApiResponse(responseData));
    } else {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Invite is not a public, placeholder, or direct user invite for redemption",
        })
      );
    }
  } catch (error) {
    request.log.error("Error in redeemGroupInviteHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}
