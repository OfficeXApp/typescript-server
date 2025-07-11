import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import {
  GroupInvite,
  GroupInviteFE,
  FactoryApiResponse,
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
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import { authenticateRequest } from "../../../../services/auth";
import { OrgIdParams } from "../../types";

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

function createApiResponse<T>(
  data?: T,
  error?: { code: number; message: string }
): FactoryApiResponse<T> {
  return {
    status: error ? "error" : "success",
    data,
    error,
    timestamp: Date.now(),
  };
}

function validateCreateRequest(body: IRequestCreateGroupInvite): {
  valid: boolean;
  error?: string;
} {
  if (!body.group_id || !body.group_id.startsWith("GroupID_")) {
    return { valid: false, error: "Group ID must start with GroupID_" };
  }

  if (
    body.invitee_id &&
    !body.invitee_id.startsWith("UserID_") &&
    body.invitee_id !== "PUBLIC"
  ) {
    return {
      valid: false,
      error: "Invitee ID must start with UserID_ or be PUBLIC",
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

    const inviteId = request.params.invite_id;

    // Get the invite
    const invites = await db.queryDrive(
      request.params.org_id,
      "SELECT * FROM group_invites WHERE id = ?",
      [inviteId]
    );

    if (!invites || invites.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group invite not found",
        })
      );
    }

    const invite = invites[0] as GroupInvite;

    // Check if user is authorized
    const groups = await db.queryDrive(
      request.params.org_id,
      "SELECT owner_user_id FROM groups WHERE id = ?",
      [invite.group_id]
    );

    if (groups.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group not found",
        })
      );
    }

    const group = groups[0];
    const isAuthorized =
      requesterApiKey.user_id === group.owner_user_id ||
      requesterApiKey.user_id === invite.inviter_id ||
      requesterApiKey.user_id === invite.invitee_id;

    if (!isAuthorized) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    // Get group and invitee info for FE
    const groupInfo = await db.queryDrive(
      request.params.org_id,
      "SELECT name, avatar FROM groups WHERE id = ?",
      [invite.group_id]
    );

    let inviteeName = "";
    let inviteeAvatar = undefined;

    if (invite.invitee_id && invite.invitee_id !== "PUBLIC") {
      const inviteeInfo = await db.queryDrive(
        request.params.org_id,
        "SELECT name, avatar FROM contacts WHERE id = ?",
        [invite.invitee_id]
      );
      if (inviteeInfo.length > 0) {
        inviteeName = inviteeInfo[0].name;
        inviteeAvatar = inviteeInfo[0].avatar;
      }
    } else if (invite.invitee_id === "PUBLIC") {
      inviteeName = "Public";
    }

    const inviteFE: GroupInviteFE = {
      ...invite,
      group_name: groupInfo[0]?.name || "",
      group_avatar: groupInfo[0]?.avatar,
      invitee_name: inviteeName,
      invitee_avatar: inviteeAvatar,
      permission_previews: [], // TODO: Implement permission previews
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

    // Check if group exists and user has access
    const groups = await db.queryDrive(
      request.params.org_id,
      "SELECT owner_user_id FROM groups WHERE id = ?",
      [body.group_id]
    );

    if (groups.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group not found",
        })
      );
    }

    const group = groups[0];
    const isOwner = requesterApiKey.user_id === group.owner_user_id;

    // Check if user is a member
    const memberCheck = await db.queryDrive(
      request.params.org_id,
      `SELECT 1 FROM contact_groups WHERE user_id = ? AND group_id = ?`,
      [requesterApiKey.user_id, body.group_id]
    );
    const isMember = memberCheck.length > 0;

    if (!isOwner && !isMember) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    // Build query with cursor-based pagination
    let query = `
        SELECT gi.*, g.name as group_name, g.avatar as group_avatar,
               c.name as invitee_name, c.avatar as invitee_avatar
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

    const invites = await db.queryDrive(request.params.org_id, query, params);

    const hasMore = invites.length > pageSize;
    if (hasMore) {
      invites.pop();
    }

    // Get total count
    const countResult = await db.queryDrive(
      request.params.org_id,
      "SELECT COUNT(*) as total FROM group_invites WHERE group_id = ?",
      [body.group_id]
    );
    const total = countResult[0]?.total || 0;

    const invitesFE: GroupInviteFE[] = invites.map((invite: any) => ({
      id: invite.id,
      group_id: invite.group_id,
      inviter_id: invite.inviter_id,
      invitee_id: invite.invitee_id || "PUBLIC",
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
      invitee_name:
        invite.invitee_id === "PUBLIC" ? "Public" : invite.invitee_name || "",
      invitee_avatar: invite.invitee_avatar,
      permission_previews: [],
    }));

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

    // Check if group exists and user has permission
    const groups = await db.queryDrive(
      request.params.org_id,
      "SELECT owner_user_id FROM groups WHERE id = ?",
      [body.group_id]
    );

    if (groups.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group not found",
        })
      );
    }

    const group = groups[0];
    const isOwner = requesterApiKey.user_id === group.owner_user_id;

    // TODO: Check if requester is admin
    if (!isOwner) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const now = Date.now();
    const inviteId = body.id || `${IDPrefixEnum.GroupInvite}${uuidv4()}`;

    // Handle invitee_id - could be user, placeholder, or public
    let inviteeId = body.invitee_id;
    let redeemCode = undefined;
    let fromPlaceholder = undefined;

    if (!inviteeId) {
      // Create placeholder invite
      const placeholderId = `${IDPrefixEnum.PlaceholderGroupInviteeID}${uuidv4()}`;
      inviteeId = placeholderId;
      redeemCode = `REDEEM_${Date.now()}`;
    } else if (inviteeId === "PUBLIC") {
      redeemCode = "PUBLIC";
    }

    const invite: GroupInvite = {
      id: inviteId as GroupInviteID,
      group_id: body.group_id as GroupID,
      inviter_id: requesterApiKey.user_id,
      invitee_id: inviteeId as UserID,
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
    await dbHelpers.transaction("drive", request.params.org_id, (database) => {
      const stmt = database.prepare(
        `INSERT INTO group_invites (
            id, group_id, inviter_id, invitee_id, invitee_type, role, note,
            active_from, expires_at, created_at, last_modified_at,
            redeem_code, from_placeholder_invitee, external_id, external_payload
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );

      const inviteeType =
        inviteeId === "PUBLIC"
          ? "PUBLIC"
          : inviteeId.startsWith(IDPrefixEnum.PlaceholderGroupInviteeID)
            ? "PLACEHOLDER"
            : "USER";

      stmt.run(
        invite.id,
        invite.group_id,
        invite.inviter_id,
        inviteeId === "PUBLIC" ? null : invite.invitee_id,
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

      // If it's a user invite, add them to the group
      if (inviteeType === "USER" && invite.invitee_id) {
        const memberStmt = database.prepare(
          `INSERT OR IGNORE INTO contact_groups (user_id, group_id) VALUES (?, ?)`
        );
        memberStmt.run(invite.invitee_id, invite.group_id);
      }
    });

    // Get group info for FE response
    const groupInfo = await db.queryDrive(
      request.params.org_id,
      "SELECT name, avatar FROM groups WHERE id = ?",
      [body.group_id]
    );

    let inviteeName = "";
    let inviteeAvatar = undefined;

    if (
      inviteeId &&
      inviteeId !== "PUBLIC" &&
      !inviteeId.startsWith(IDPrefixEnum.PlaceholderGroupInviteeID)
    ) {
      const inviteeInfo = await db.queryDrive(
        request.params.org_id,
        "SELECT name, avatar FROM contacts WHERE id = ?",
        [inviteeId]
      );
      if (inviteeInfo.length > 0) {
        inviteeName = inviteeInfo[0].name;
        inviteeAvatar = inviteeInfo[0].avatar;
      }
    } else if (inviteeId === "PUBLIC") {
      inviteeName = "Public";
    }

    const inviteFE: GroupInviteFE = {
      ...invite,
      invitee_id: inviteeId,
      group_name: groupInfo[0]?.name || "",
      group_avatar: groupInfo[0]?.avatar,
      invitee_name: inviteeName,
      invitee_avatar: inviteeAvatar,
      permission_previews: [],
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

    // Get existing invite
    const invites = await db.queryDrive(
      request.params.org_id,
      "SELECT * FROM group_invites WHERE id = ?",
      [body.id]
    );

    if (!invites || invites.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group invite not found",
        })
      );
    }

    const invite = invites[0] as GroupInvite;

    // Check permissions
    const groups = await db.queryDrive(
      request.params.org_id,
      "SELECT owner_user_id FROM groups WHERE id = ?",
      [invite.group_id]
    );

    if (groups.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group not found",
        })
      );
    }

    const group = groups[0];
    const isAuthorized =
      requesterApiKey.user_id === group.owner_user_id ||
      requesterApiKey.user_id === invite.inviter_id;

    if (!isAuthorized) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
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
    values.push(body.id);

    // Update in transaction
    await dbHelpers.transaction("drive", request.params.org_id, (database) => {
      const stmt = database.prepare(
        `UPDATE group_invites SET ${updates.join(", ")} WHERE id = ?`
      );
      stmt.run(...values);
    });

    // Get updated invite with additional info
    const updatedInvites = await db.queryDrive(
      request.params.org_id,
      `SELECT gi.*, g.name as group_name, g.avatar as group_avatar,
                c.name as invitee_name, c.avatar as invitee_avatar
         FROM group_invites gi
         JOIN groups g ON gi.group_id = g.id
         LEFT JOIN contacts c ON gi.invitee_id = c.id
         WHERE gi.id = ?`,
      [body.id]
    );

    const updatedInvite = updatedInvites[0];

    const inviteFE: GroupInviteFE = {
      ...updatedInvite,
      invitee_id: updatedInvite.invitee_id || "PUBLIC",
      invitee_name:
        updatedInvite.invitee_id === null
          ? "Public"
          : updatedInvite.invitee_name || "",
      permission_previews: [],
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

    // Get invite to check permissions
    const invites = await db.queryDrive(
      request.params.org_id,
      "SELECT * FROM group_invites WHERE id = ?",
      [body.id]
    );

    if (!invites || invites.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group invite not found",
        })
      );
    }

    const invite = invites[0] as GroupInvite;

    // Check permissions
    const groups = await db.queryDrive(
      request.params.org_id,
      "SELECT owner_user_id FROM groups WHERE id = ?",
      [invite.group_id]
    );

    if (groups.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group not found",
        })
      );
    }

    const group = groups[0];
    const isAuthorized =
      requesterApiKey.user_id === group.owner_user_id ||
      requesterApiKey.user_id === invite.inviter_id;

    if (!isAuthorized) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    // Delete invite and remove user from group if needed
    await dbHelpers.transaction("drive", request.params.org_id, (database) => {
      // Delete the invite
      database.prepare("DELETE FROM group_invites WHERE id = ?").run(body.id);

      // Remove user from group if they were added
      if (
        invite.invitee_id &&
        !invite.invitee_id.startsWith(IDPrefixEnum.PlaceholderGroupInviteeID)
      ) {
        database
          .prepare(
            "DELETE FROM contact_groups WHERE user_id = ? AND group_id = ?"
          )
          .run(invite.invitee_id, invite.group_id);
      }
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

    // Get the invite
    const invites = await db.queryDrive(
      request.params.org_id,
      "SELECT * FROM group_invites WHERE id = ?",
      [body.invite_id]
    );

    if (!invites || invites.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group invite not found",
        })
      );
    }

    const invite = invites[0];

    // Validate redeem code
    if (!invite.redeem_code || invite.redeem_code !== body.redeem_code) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Invalid redeem code",
        })
      );
    }

    const now = Date.now();

    // Handle different invite types
    if (invite.invitee_type === "PUBLIC") {
      // For public invites, create a new invite for the specific user
      const newInviteId = `${IDPrefixEnum.GroupInvite}${uuidv4()}`;

      await dbHelpers.transaction(
        "drive",
        request.params.org_id,
        (database) => {
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
            GroupRole.MEMBER,
            userNote,
            invite.active_from,
            invite.expires_at,
            now,
            now,
            null,
            invite.id,
            invite.external_id,
            invite.external_payload
          );

          // Add user to group
          const memberStmt = database.prepare(
            `INSERT OR IGNORE INTO contact_groups (user_id, group_id) VALUES (?, ?)`
          );
          memberStmt.run(body.user_id, invite.group_id);
        }
      );

      // Get the new invite with additional info
      const newInvites = await db.queryDrive(
        request.params.org_id,
        `SELECT gi.*, g.name as group_name, g.avatar as group_avatar,
                  c.name as invitee_name, c.avatar as invitee_avatar
           FROM group_invites gi
           JOIN groups g ON gi.group_id = g.id
           LEFT JOIN contacts c ON gi.invitee_id = c.id
           WHERE gi.id = ?`,
        [newInviteId]
      );

      const newInvite = newInvites[0];

      const responseData: IResponseRedeemGroupInvite = {
        ok: {
          data: {
            invite: {
              ...newInvite,
              invitee_id: newInvite.invitee_id || "PUBLIC",
              invitee_name: newInvite.invitee_name || "",
              permission_previews: [],
            },
          },
        },
      };

      return reply.status(200).send(createApiResponse(responseData));
    } else if (invite.invitee_type === "PLACEHOLDER") {
      // For placeholder invites, update the existing invite
      if (invite.from_placeholder_invitee) {
        return reply.status(400).send(
          createApiResponse(undefined, {
            code: 400,
            message: "Invite has already been redeemed",
          })
        );
      }

      await dbHelpers.transaction(
        "drive",
        request.params.org_id,
        (database) => {
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
            GroupRole.MEMBER,
            userNote,
            now,
            invite.invitee_id,
            body.invite_id
          );

          // Add user to group
          const memberStmt = database.prepare(
            `INSERT OR IGNORE INTO contact_groups (user_id, group_id) VALUES (?, ?)`
          );
          memberStmt.run(body.user_id, invite.group_id);
        }
      );

      // Get updated invite
      const updatedInvites = await db.queryDrive(
        request.params.org_id,
        `SELECT gi.*, g.name as group_name, g.avatar as group_avatar,
                  c.name as invitee_name, c.avatar as invitee_avatar
           FROM group_invites gi
           JOIN groups g ON gi.group_id = g.id
           LEFT JOIN contacts c ON gi.invitee_id = c.id
           WHERE gi.id = ?`,
        [body.invite_id]
      );

      const updatedInvite = updatedInvites[0];

      const responseData: IResponseRedeemGroupInvite = {
        ok: {
          data: {
            invite: {
              ...updatedInvite,
              invitee_id: updatedInvite.invitee_id || "PUBLIC",
              invitee_name: updatedInvite.invitee_name || "",
              permission_previews: [],
            },
          },
        },
      };

      return reply.status(200).send(createApiResponse(responseData));
    } else {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Invite is not a public or placeholder invite",
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
