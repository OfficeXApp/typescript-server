import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import {
  Group,
  GroupFE,
  FactoryApiResponse,
  IRequestCreateGroup,
  IRequestUpdateGroup,
  IRequestDeleteGroup,
  IResponseDeleteGroup,
  IRequestValidateGroupMember,
  IResponseValidateGroupMember,
  IDPrefixEnum,
  GroupID,
  UserID,
  DriveID,
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import { authenticateRequest } from "../../../../services/auth";
import { OrgIdParams } from "../../types";

interface GetGroupParams extends OrgIdParams {
  group_id: string;
}

interface ListGroupsBody {
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

function validateCreateRequest(body: IRequestCreateGroup): {
  valid: boolean;
  error?: string;
} {
  if (!body.name || body.name.length > 256) {
    return {
      valid: false,
      error: "Name is required and must be less than 256 characters",
    };
  }

  if (body.public_note && body.public_note.length > 8192) {
    return {
      valid: false,
      error: "Public note must be less than 8192 characters",
    };
  }

  if (body.private_note && body.private_note.length > 8192) {
    return {
      valid: false,
      error: "Private note must be less than 8192 characters",
    };
  }

  if (body.endpoint_url && body.endpoint_url.length > 4096) {
    return {
      valid: false,
      error: "Endpoint URL must be less than 4096 characters",
    };
  }

  return { valid: true };
}

function validateUpdateRequest(body: IRequestUpdateGroup): {
  valid: boolean;
  error?: string;
} {
  if (!body.id || !body.id.startsWith("GroupID_")) {
    return { valid: false, error: "Group ID must start with GroupID_" };
  }

  if (body.name !== undefined && body.name.length > 256) {
    return { valid: false, error: "Name must be less than 256 characters" };
  }

  if (body.public_note !== undefined && body.public_note.length > 8192) {
    return {
      valid: false,
      error: "Public note must be less than 8192 characters",
    };
  }

  if (body.private_note !== undefined && body.private_note.length > 8192) {
    return {
      valid: false,
      error: "Private note must be less than 8192 characters",
    };
  }

  if (body.endpoint_url !== undefined && body.endpoint_url.length > 4096) {
    return {
      valid: false,
      error: "Endpoint URL must be less than 4096 characters",
    };
  }

  return { valid: true };
}

export async function getGroupHandler(
  request: FastifyRequest<{ Params: GetGroupParams }>,
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

    const groupId = request.params.group_id;

    // Get the group
    const groups = await db.queryDrive(
      request.params.org_id,
      "SELECT * FROM groups WHERE id = ?",
      [groupId]
    );

    if (!groups || groups.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group not found",
        })
      );
    }

    const group = groups[0] as Group;

    // TODO: PERMIT Check permissions - for now just check if user is owner or member
    const isOwner = requesterApiKey.user_id === group.owner;

    // Check if user is a member of the group
    const memberCheck = await db.queryDrive(
      request.params.org_id,
      `SELECT 1 FROM contact_groups WHERE user_id = ? AND group_id = ?`,
      [requesterApiKey.user_id, groupId]
    );
    const isMember = memberCheck.length > 0;

    if (!isOwner && !isMember) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    // Get group members for the FE response
    const members = await db.queryDrive(
      request.params.org_id,
      `SELECT c.*, gi.id as invite_id, gi.role, gi.note as invite_note
       FROM contact_groups cg
       JOIN contacts c ON cg.user_id = c.id
       LEFT JOIN group_invites gi ON gi.group_id = cg.group_id AND gi.invitee_id = c.id
       WHERE cg.group_id = ?`,
      [groupId]
    );

    const groupFE: GroupFE = {
      ...group,
      member_previews: members.map((m) => ({
        user_id: m.id,
        name: m.name,
        avatar: m.avatar,
        note: m.invite_note,
        group_id: groupId,
        is_admin: m.role === "ADMIN",
        invite_id: m.invite_id || "",
        last_online_ms: m.last_online_ms || 0,
      })),
      permission_previews: [], // TODO: REDACT Implement permission previews
    };

    return reply.status(200).send(createApiResponse(groupFE));
  } catch (error) {
    request.log.error("Error in getGroupHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function listGroupsHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: ListGroupsBody }>,
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

    const body = request.body || {};
    const pageSize = body.page_size || 50;
    const direction = body.direction || "DESC";
    const cursor = body.cursor;

    // Build query with cursor-based pagination
    let query = `
      SELECT g.*, 
             (SELECT COUNT(*) FROM contact_groups cg WHERE cg.group_id = g.id) as member_count
      FROM groups g
      WHERE 1=1
    `;
    const params: any[] = [];

    if (cursor) {
      query += ` AND g.created_at ${direction === "ASC" ? ">" : "<"} ?`;
      params.push(cursor);
    }

    query += ` ORDER BY g.created_at ${direction} LIMIT ?`;
    params.push(pageSize + 1); // Get one extra to check if there are more

    const groups = await db.queryDrive(request.params.org_id, query, params);

    const hasMore = groups.length > pageSize;
    if (hasMore) {
      groups.pop(); // Remove the extra item
    }

    // Get total count
    const countResult = await db.queryDrive(
      request.params.org_id,
      "SELECT COUNT(*) as total FROM groups"
    );
    const total = countResult[0]?.total || 0;

    // Convert to GroupFE format
    const groupsFE = await Promise.all(
      groups.map(async (group: Group) => {
        // Get member previews for each group
        const members = await db.queryDrive(
          request.params.org_id,
          `SELECT c.*, gi.id as invite_id, gi.role, gi.note as invite_note
         FROM contact_groups cg
         JOIN contacts c ON cg.user_id = c.id
         LEFT JOIN group_invites gi ON gi.group_id = cg.group_id AND gi.invitee_id = c.id
         WHERE cg.group_id = ?
         LIMIT 5`, // Limit member previews for list view
          [group.id]
        );

        return {
          ...group,
          member_previews: members.map((m) => ({
            user_id: m.id,
            name: m.name,
            avatar: m.avatar,
            note: m.invite_note,
            group_id: group.id,
            is_admin: m.role === "ADMIN",
            invite_id: m.invite_id || "",
            last_online_ms: m.last_online_ms || 0,
          })),
          permission_previews: [], // TODO: REDACT Implement permission previews
        };
      })
    );

    const nextCursor =
      hasMore && groups.length > 0
        ? groups[groups.length - 1].created_at
        : null;

    return reply.status(200).send(
      createApiResponse({
        items: groupsFE,
        page_size: pageSize,
        total,
        direction,
        cursor: nextCursor,
      })
    );
  } catch (error) {
    request.log.error("Error in listGroupsHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function createGroupHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestCreateGroup }>,
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

    // TODO: PERMIT Check create permissions

    const now = Date.now();
    const groupId = body.id || `${IDPrefixEnum.Group}${uuidv4()}`;

    // Get drive info for defaults
    const driveInfo = await db.queryDrive(
      request.params.org_id,
      "SELECT * FROM about_drive LIMIT 1"
    );
    const driveData = driveInfo[0];

    const group: Group = {
      id: groupId as GroupID,
      name: body.name,
      owner: requesterApiKey.user_id,
      avatar: body.avatar || "",
      private_note: body.private_note,
      public_note: body.public_note,
      created_at: now,
      last_modified_at: now,
      drive_id: request.params.org_id as DriveID,
      endpoint_url: body.endpoint_url || driveData?.url_endpoint || "",
      labels: [],
      external_id: body.external_id,
      external_payload: body.external_payload,
      admin_invites: [],
      member_invites: [],
    };

    // TODO: GROUP Refactor this to include the groupinvite in admins []
    // Insert group using transaction
    await dbHelpers.transaction("drive", request.params.org_id, (database) => {
      const stmt = database.prepare(
        `INSERT INTO groups (
          id, name, owner, avatar, private_note, public_note,
          created_at, last_modified_at, drive_id, endpoint_url,
          external_id, external_payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );
      stmt.run(
        group.id,
        group.name,
        group.owner,
        group.avatar,
        group.private_note,
        group.public_note,
        group.created_at,
        group.last_modified_at,
        group.drive_id,
        group.endpoint_url,
        group.external_id,
        group.external_payload
      );

      // Add owner as member
      const memberStmt = database.prepare(
        `INSERT INTO contact_groups (user_id, group_id) VALUES (?, ?)`
      );
      memberStmt.run(requesterApiKey.user_id, groupId);
    });

    const groupFE: GroupFE = {
      ...group,
      member_previews: [
        {
          user_id: requesterApiKey.user_id as UserID,
          name: "", // TODO: REDACT Get from contacts
          avatar: undefined,
          note: "Owner",
          group_id: groupId as GroupID,
          is_admin: true,
          invite_id: "",
          last_online_ms: now,
        },
      ],
      permission_previews: [],
    };

    return reply.status(200).send(createApiResponse(groupFE));
  } catch (error) {
    request.log.error("Error in createGroupHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function updateGroupHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestUpdateGroup }>,
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
    const validation = validateUpdateRequest(body);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    // Get existing group
    const groups = await db.queryDrive(
      request.params.org_id,
      "SELECT * FROM groups WHERE id = ?",
      [body.id]
    );

    if (!groups || groups.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group not found",
        })
      );
    }

    const group = groups[0] as Group;

    // Check permissions
    if (requesterApiKey.user_id !== group.owner) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    // Build update query dynamically
    const updates: string[] = [];
    const values: any[] = [];

    if (body.name !== undefined) {
      updates.push("name = ?");
      values.push(body.name);
    }
    if (body.avatar !== undefined) {
      updates.push("avatar = ?");
      values.push(body.avatar);
    }
    if (body.public_note !== undefined) {
      updates.push("public_note = ?");
      values.push(body.public_note);
    }
    if (body.private_note !== undefined) {
      updates.push("private_note = ?");
      values.push(body.private_note);
    }
    if (body.endpoint_url !== undefined) {
      updates.push("endpoint_url = ?");
      values.push(body.endpoint_url);
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
        `UPDATE groups SET ${updates.join(", ")} WHERE id = ?`
      );
      stmt.run(...values);
    });

    // Get updated group with member previews
    const updatedGroups = await db.queryDrive(
      request.params.org_id,
      "SELECT * FROM groups WHERE id = ?",
      [body.id]
    );

    const updatedGroup = updatedGroups[0] as Group;

    // Get member previews
    const members = await db.queryDrive(
      request.params.org_id,
      `SELECT c.*, gi.id as invite_id, gi.role, gi.note as invite_note
       FROM contact_groups cg
       JOIN contacts c ON cg.user_id = c.id
       LEFT JOIN group_invites gi ON gi.group_id = cg.group_id AND gi.invitee_id = c.id
       WHERE cg.group_id = ?`,
      [body.id]
    );

    const groupFE: GroupFE = {
      ...updatedGroup,
      member_previews: members.map((m) => ({
        user_id: m.id,
        name: m.name,
        avatar: m.avatar,
        note: m.invite_note,
        group_id: body.id as GroupID,
        is_admin: m.role === "ADMIN",
        invite_id: m.invite_id || "",
        last_online_ms: m.last_online_ms || 0,
      })),
      permission_previews: [],
    };

    return reply.status(200).send(createApiResponse(groupFE));
  } catch (error) {
    request.log.error("Error in updateGroupHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function deleteGroupHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestDeleteGroup }>,
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

    // Get the group to check permissions
    const groups = await db.queryDrive(
      request.params.org_id,
      "SELECT * FROM groups WHERE id = ?",
      [body.id]
    );

    if (!groups || groups.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group not found",
        })
      );
    }

    const group = groups[0] as Group;

    // Check permissions
    if (requesterApiKey.user_id !== group.owner) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    // Delete group and related data in transaction
    await dbHelpers.transaction("drive", request.params.org_id, (database) => {
      // Delete group invites
      database
        .prepare("DELETE FROM group_invites WHERE group_id = ?")
        .run(body.id);

      // Delete group members
      database
        .prepare("DELETE FROM contact_groups WHERE group_id = ?")
        .run(body.id);

      // Delete group labels
      database
        .prepare("DELETE FROM group_labels WHERE group_id = ?")
        .run(body.id);

      // Delete the group
      database.prepare("DELETE FROM groups WHERE id = ?").run(body.id);
    });

    const deletedData: IResponseDeleteGroup = {
      ok: {
        data: {
          id: body.id as GroupID,
          deleted: true,
        },
      },
    };

    return reply.status(200).send(createApiResponse(deletedData));
  } catch (error) {
    request.log.error("Error in deleteGroupHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function validateGroupMemberHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestValidateGroupMember;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    // This endpoint doesn't require authentication as it's used for cross-drive validation
    const body = request.body;

    // Check if group exists
    const groups = await db.queryDrive(
      request.params.org_id,
      "SELECT id FROM groups WHERE id = ?",
      [body.group_id]
    );

    if (!groups || groups.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group not found",
        })
      );
    }

    // Check if user is a member
    const membership = await db.queryDrive(
      request.params.org_id,
      "SELECT 1 FROM contact_groups WHERE user_id = ? AND group_id = ?",
      [body.user_id, body.group_id]
    );

    const isMember = membership.length > 0;

    const responseData: IResponseValidateGroupMember = {
      ok: {
        data: {
          is_member: isMember,
          group_id: body.group_id as GroupID,
          user_id: body.user_id as UserID,
        },
      },
    };

    if (isMember) {
      return reply.status(200).send(createApiResponse(responseData));
    } else {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "User is not a member of this group",
        })
      );
    }
  } catch (error) {
    request.log.error("Error in validateGroupMemberHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}
