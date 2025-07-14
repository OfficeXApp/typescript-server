import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import {
  GroupInvite,
  GroupInviteFE,
  ApiResponse,
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
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import { authenticateRequest } from "../../../../services/auth";
import { createApiResponse, OrgIdParams } from "../../types";
import {
  checkSystemPermissions,
  hasSystemManagePermission,
} from "../../../../services/permissions/system";
import {
  getGroupById,
  getGroupInviteById,
  isGroupAdmin,
} from "../../../../services/groups";

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

function validateCreateRequest(body: IRequestCreateGroupInvite): {
  valid: boolean;
  error?: string;
} {
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
  request: FastifyRequest<{ Params: GetGroupInviteParams }>,
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
      )[0]?.owner_id === currentUserId; // Org owner check
    const isInviter = currentUserId === invite.inviter_id;
    const isInvitee = currentUserId === invite.invitee_id;
    const canViewInviteViaPermissions = (
      await checkSystemPermissions(
        invite.id as SystemResourceID, // GroupInviteID is a SystemRecordIDEnum
        currentUserId,
        orgId
      )
    ).includes(SystemPermissionType.VIEW);

    if (
      !isOrgOwner &&
      !isInviter &&
      !isInvitee &&
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
      }
    } else if (invite.invitee_id === "PUBLIC") {
      inviteeName = "Public";
    } else if (
      invite.invitee_id &&
      invite.invitee_id.startsWith(IDPrefixEnum.PlaceholderGroupInviteeID)
    ) {
      inviteeName = "Awaiting Anon";
    }

    // PERMIT: Get permission previews for the current user on this group invite record
    const permissionPreviews = await checkSystemPermissions(
      invite.id as SystemResourceID,
      currentUserId,
      orgId
    );

    const inviteFE: GroupInviteFE = {
      ...invite,
      group_name: groupInfo[0]?.name || "",
      group_avatar: groupInfo[0]?.avatar,
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
        message: "Internal server error",
      })
    );
  }
}

export async function listGroupInvitesHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: ListGroupInvitesBody }>,
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
    const direction = body.direction || "DESC";
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

    // PERMIT: Check if requester is the org owner, or has VIEW permission on the Group Invites table.
    const isOrgOwner =
      (
        await db.queryDrive(orgId, "SELECT owner_id FROM about_drive LIMIT 1")
      )[0]?.owner_id === currentUserId; // Org owner check
    const canViewInvitesTable = (
      await checkSystemPermissions(
        `TABLE_${SystemTableValueEnum.INBOX}` as SystemResourceID, // Invites are typically linked to Inbox/notifications in Rust
        currentUserId,
        orgId
      )
    ).includes(SystemPermissionType.VIEW);

    if (!isOrgOwner && !canViewInvitesTable) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Forbidden: Not authorized to list group invites",
        })
      );
    }

    // Build query with cursor-based pagination
    let query = `
        SELECT gi.*, g.name as group_name, g.avatar as group_avatar,
               c.name as invitee_name, c.avatar as invitee_avatar,
               gi.invitee_type, gi.invitee_id
        FROM group_invites gi
        JOIN groups g ON gi.group_id = g.id
        LEFT JOIN contacts c ON gi.invitee_id = c.id
        WHERE gi.group_id = ?
      `;
    const params: any[] = [body.group_id];

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

        // Handle invitee_id and invitee_type from DB rows
        if (invite.invitee_type === "USER" && invite.invitee_id) {
          const inviteeInfo = await db.queryDrive(
            orgId,
            "SELECT name, avatar FROM contacts WHERE id = ?",
            [invite.invitee_id]
          );
          if (inviteeInfo.length > 0) {
            inviteeName = inviteeInfo[0].name;
            inviteeAvatar = inviteeInfo[0].avatar;
          }
        } else if (invite.invitee_type === "PUBLIC") {
          inviteeName = "Public";
        } else if (invite.invitee_type === "PLACEHOLDER") {
          inviteeName = "Awaiting Anon";
        }

        // PERMIT: Get permission previews for the current user on each group invite record
        const permissionPreviews = await checkSystemPermissions(
          invite.id as SystemResourceID,
          currentUserId,
          orgId
        );

        return {
          id: invite.id,
          group_id: invite.group_id,
          inviter_id: invite.inviter_id,
          invitee_id:
            invite.invitee_type === "USER" && invite.invitee_id
              ? invite.invitee_id
              : invite.invitee_type, // Map back to GranteeID string
          role: invite.role,
          note: invite.note,
          active_from: invite.active_from,
          expires_at: invite.expires_at,
          created_at: invite.created_at,
          last_modified_at: invite.last_modified_at,
          from_placeholder_invitee: invite.from_placeholder_invitee,
          labels: [],
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
        ? invites[invites.length - 1].created_at
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
        message: "Internal server error",
      })
    );
  }
}

export async function createGroupInviteHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestCreateGroupInvite;
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
    const validation = validateCreateRequest(body);
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

    // PERMIT: Check if requester is the org owner, or has INVITE permission on the group record.
    const isOrgOwner =
      (
        await db.queryDrive(orgId, "SELECT owner_id FROM about_drive LIMIT 1")
      )[0]?.owner_id === currentUserId; // Org owner check
    const canInviteToGroupViaPermissions = (
      await checkSystemPermissions(
        group.id as SystemResourceID,
        currentUserId,
        orgId
      )
    ).includes(SystemPermissionType.INVITE);

    if (!isOrgOwner && !canInviteToGroupViaPermissions) {
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
    let inviteeId = body.invitee_id;
    let redeemCode = undefined;
    let fromPlaceholder = undefined;
    let inviteeType: "USER" | "PLACEHOLDER" | "PUBLIC";

    if (!inviteeId) {
      // Create placeholder invite if invitee_id is not provided
      const placeholderId = `${IDPrefixEnum.PlaceholderGroupInviteeID}${uuidv4()}`;
      inviteeId = placeholderId;
      redeemCode = `REDEEM_${Date.now()}`; // Generate a redeem code for placeholder invites
      inviteeType = "PLACEHOLDER";
    } else if (inviteeId === "PUBLIC") {
      redeemCode = "PUBLIC"; // Public invites have a static redeem code
      inviteeType = "PUBLIC";
    } else if (inviteeId.startsWith(IDPrefixEnum.User)) {
      inviteeType = "USER";
    } else if (inviteeId.startsWith(IDPrefixEnum.PlaceholderGroupInviteeID)) {
      inviteeType = "PLACEHOLDER";
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
      invitee_id: inviteeId,
      role: body.role || GroupRole.MEMBER,
      note: body.note || "",
      active_from: body.active_from || 0,
      expires_at: body.expires_at || -1,
      created_at: now,
      last_modified_at: now,
      redeem_code: redeemCode,
      from_placeholder_invitee: fromPlaceholder,
      labels: [],
      external_id: body.external_id,
      external_payload: body.external_payload,
    };

    // Insert invite using transaction
    await dbHelpers.transaction("drive", orgId, (database) => {
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
        inviteeType === "USER" ? invite.invitee_id : null, // Store actual invitee_id only for USER type, otherwise NULL
        inviteeType,
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

      // If it's a direct user invite, add them to the contact_groups table immediately
      if (inviteeType === "USER" && invite.invitee_id) {
        const memberStmt = database.prepare(
          `INSERT OR IGNORE INTO contact_groups (user_id, group_id, role) VALUES (?, ?, ?)`
        );
        memberStmt.run(invite.invitee_id, invite.group_id, invite.role);
      }
    });

    // Get group info for FE response
    const groupInfo = await db.queryDrive(
      orgId,
      "SELECT name, avatar FROM groups WHERE id = ?",
      [body.group_id]
    );

    let inviteeName = "";
    let inviteeAvatar = undefined;

    if (inviteeId && inviteeId.startsWith(IDPrefixEnum.User)) {
      const inviteeInfo = await db.queryDrive(
        orgId,
        "SELECT name, avatar FROM contacts WHERE id = ?",
        [inviteeId]
      );
      if (inviteeInfo.length > 0) {
        inviteeName = inviteeInfo[0].name;
        inviteeAvatar = inviteeInfo[0].avatar;
      }
    } else if (inviteeId === "PUBLIC") {
      inviteeName = "Public";
    } else if (inviteeId.startsWith(IDPrefixEnum.PlaceholderGroupInviteeID)) {
      inviteeName = "Awaiting Anon";
    }

    // PERMIT: Get permission previews for the current user on the newly created invite record
    const permissionPreviews = await checkSystemPermissions(
      invite.id as SystemResourceID,
      currentUserId,
      orgId
    );

    const inviteFE: GroupInviteFE = {
      ...invite,
      invitee_id: inviteeId,
      group_name: groupInfo[0]?.name || "",
      group_avatar: groupInfo[0]?.avatar,
      invitee_name: inviteeName,
      invitee_avatar: inviteeAvatar,
      permission_previews: permissionPreviews,
    };

    return reply.status(200).send(createApiResponse(inviteFE));
  } catch (error) {
    request.log.error("Error in createGroupInviteHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function updateGroupInviteHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestUpdateGroupInvite;
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

    // PERMIT: Check if requester is the org owner, inviter, or has EDIT permission on the group invite record
    const isOrgOwner =
      (
        await db.queryDrive(orgId, "SELECT owner_id FROM about_drive LIMIT 1")
      )[0]?.owner_id === currentUserId; // Org owner check
    const isInviter = currentUserId === invite.inviter_id;
    const canEditInviteViaPermissions = (
      await checkSystemPermissions(
        invite.id as SystemResourceID,
        currentUserId,
        orgId
      )
    ).includes(SystemPermissionType.EDIT);

    if (!isOrgOwner && !isInviter && !canEditInviteViaPermissions) {
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

      // If the role is being updated and it's a direct user invite, update contact_groups
      if (
        body.role !== undefined &&
        invite.invitee_id &&
        invite.invitee_id.startsWith(IDPrefixEnum.User)
      ) {
        const updateMemberRoleStmt = database.prepare(
          `UPDATE contact_groups SET role = ? WHERE user_id = ? AND group_id = ?`
        );
        updateMemberRoleStmt.run(body.role, invite.invitee_id, invite.group_id);
      }
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
    const permissionPreviews = await checkSystemPermissions(
      updatedInvite.id as SystemResourceID,
      currentUserId,
      orgId
    );

    const inviteFE: GroupInviteFE = {
      ...updatedInvite,
      group_name: group.name,
      invitee_id: updatedInvite.invitee_id || "PUBLIC",
      invitee_name: updatedInvite.invitee_id.startsWith(
        GroupInviteeTypeEnum.USER
      )
        ? "Public"
        : updatedInvite.invitee_id.startsWith(
              GroupInviteeTypeEnum.PLACEHOLDER_GROUP_INVITEE
            )
          ? "Awaiting Anon"
          : updatedInvite.invitee_id.startsWith(
                GroupInviteeTypeEnum.PLACEHOLDER_GROUP_INVITEE
              )
            ? "Placeholder"
            : "",
      permission_previews: permissionPreviews,
    };

    return reply.status(200).send(createApiResponse(inviteFE));
  } catch (error) {
    request.log.error("Error in updateGroupInviteHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function deleteGroupInviteHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestDeleteGroupInvite;
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

    // PERMIT: Check if requester is the org owner, inviter, or has DELETE permission on the group invite record
    const isOrgOwner =
      (
        await db.queryDrive(orgId, "SELECT owner_id FROM about_drive LIMIT 1")
      )[0]?.owner_id === currentUserId; // Org owner check
    const isInviter = currentUserId === invite.inviter_id;
    const canDeleteInviteViaPermissions = (
      await checkSystemPermissions(
        invite.id as SystemResourceID,
        currentUserId,
        orgId
      )
    ).includes(SystemPermissionType.DELETE);

    if (!isOrgOwner && !isInviter && !canDeleteInviteViaPermissions) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Forbidden: Not allowed to delete this invite",
        })
      );
    }

    // Delete invite and remove user from group if needed
    await dbHelpers.transaction("drive", orgId, (database) => {
      // Delete the invite
      database.prepare("DELETE FROM group_invites WHERE id = ?").run(invite.id);

      // Remove user from group if they were added via a direct user invite
      if (
        invite.invitee_id &&
        invite.invitee_id.startsWith(IDPrefixEnum.User)
      ) {
        database
          .prepare(
            "DELETE FROM contact_groups WHERE user_id = ? AND group_id = ?"
          )
          .run(invite.invitee_id, invite.group_id);
      }
      // Also remove any system permissions associated with this invite ID as a resource
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

    return reply.status(200).send(createApiResponse(deletedData));
  } catch (error) {
    request.log.error("Error in deleteGroupInviteHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function redeemGroupInviteHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestRedeemGroupInvite;
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
          message: "Invalid redeem code",
        })
      );
    }

    // PERMIT: User attempting to redeem must be the intended invitee if it's a specific user invite.
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

    // PERMIT: Check if user has INVITE permission on the group record (to join it)
    const canJoinGroupViaPermissions = (
      await checkSystemPermissions(
        invite.group_id as SystemResourceID,
        currentUserId,
        orgId
      )
    ).includes(SystemPermissionType.INVITE);

    if (!canJoinGroupViaPermissions) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Forbidden: Not allowed to join this group",
        })
      );
    }

    const now = Date.now();

    // Handle different invite types
    if (invite.invitee_id.startsWith(GroupInviteeTypeEnum.PUBLIC)) {
      // For public invites, create a new invite for the specific user
      const newInviteId = `${IDPrefixEnum.GroupInvite}${uuidv4()}`;

      await dbHelpers.transaction("drive", orgId, (database) => {
        // Create new invite
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
          body.user_id,
          "USER",
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

        // Add user to group
        const memberStmt = database.prepare(
          `INSERT OR IGNORE INTO contact_groups (user_id, group_id, role) VALUES (?, ?, ?)`
        );
        memberStmt.run(body.user_id, invite.group_id, invite.role);
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

      // PERMIT: Get permission previews for the current user on the newly redeemed invite record
      const permissionPreviews = await checkSystemPermissions(
        newInvite.id as SystemResourceID,
        currentUserId,
        orgId
      );

      const responseData: IResponseRedeemGroupInvite = {
        ok: {
          data: {
            invite: {
              ...newInvite,
              invitee_id: newInvite.invitee_id || "PUBLIC",
            },
          },
        },
      };

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
          body.user_id,
          "USER",
          invite.role, // Maintain the original role from the placeholder invite
          userNote,
          now,
          invite.invitee_id, // Store the original placeholder ID here
          invite.id
        );

        // Add user to group
        const memberStmt = database.prepare(
          `INSERT OR IGNORE INTO contact_groups (user_id, group_id, role) VALUES (?, ?, ?)`
        );
        memberStmt.run(body.user_id, invite.group_id, invite.role);
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

      // PERMIT: Get permission previews for the current user on the updated invite record
      const permissionPreviews = await checkSystemPermissions(
        updatedInvite.id as SystemResourceID,
        currentUserId,
        orgId
      );

      const responseData: IResponseRedeemGroupInvite = {
        ok: {
          data: {
            invite: {
              ...updatedInvite,
              invitee_id: updatedInvite.invitee_id || "PUBLIC",
            },
          },
        },
      };

      return reply.status(200).send(createApiResponse(responseData));
    } else if (invite.invitee_id.startsWith(IDPrefixEnum.User)) {
      // If it's a direct user invite, and the user matches, just ensure they are in contact_groups
      // (This should already be handled during invite creation, but good for idempotency or
      // if invite was deleted from contact_groups without invite being deleted)
      await dbHelpers.transaction("drive", orgId, (database) => {
        const memberStmt = database.prepare(
          `INSERT OR IGNORE INTO contact_groups (user_id, group_id, role) VALUES (?, ?, ?)`
        );
        memberStmt.run(invite.invitee_id, invite.group_id, invite.role);
      });

      // Get updated invite (no change to invite record itself, just confirmation of membership)
      const redeemedInvite = await getGroupInviteById(invite.id, orgId);

      if (!redeemedInvite) {
        return reply.status(500).send(
          createApiResponse(undefined, {
            code: 500,
            message: "Failed to retrieve invite after user redemption",
          })
        );
      }

      const permissionPreviews = await checkSystemPermissions(
        redeemedInvite.id as SystemResourceID,
        currentUserId,
        orgId
      );

      const responseData: IResponseRedeemGroupInvite = {
        ok: {
          data: {
            invite: {
              ...redeemedInvite,
              invitee_id: redeemedInvite.invitee_id,
            },
          },
        },
      };
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
        message: "Internal server error",
      })
    );
  }
}
