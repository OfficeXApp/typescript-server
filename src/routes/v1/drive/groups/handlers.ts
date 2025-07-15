import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import {
  Group,
  GroupFE,
  ApiResponse,
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
  SystemResourceID,
  SystemTableValueEnum,
  SystemPermissionType,
  GroupRole,
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
  isGroupAdmin,
  isUserOnLocalGroup,
} from "../../../../services/groups"; // isGroupAdmin will still be used for displaying roles.

interface GetGroupParams extends OrgIdParams {
  group_id: string;
}

interface ListGroupsBody {
  filters?: string;
  page_size?: number;
  direction?: "ASC" | "DESC";
  cursor?: string;
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
  if (!body.id || !body.id.startsWith(IDPrefixEnum.Group)) {
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

    const groupId = request.params.group_id as GroupID;
    const currentUserId = requesterApiKey.user_id;
    const orgId = request.params.org_id as DriveID;

    // Get the group
    const group = await getGroupById(groupId, orgId);

    if (!group) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group not found",
        })
      );
    }

    // PERMIT: Check if the user is the org owner, or has VIEW permission on this specific group record.
    const isOrgOwner =
      (
        await db.queryDrive(orgId, "SELECT owner_id FROM about_drive LIMIT 1")
      )[0]?.owner_id === currentUserId; // Org owner check
    const canViewGroupViaPermissions = (
      await checkSystemPermissions(
        groupId as SystemResourceID, // GroupID is a SystemRecordIDEnum
        currentUserId,
        orgId
      )
    ).includes(SystemPermissionType.VIEW);

    // Authorization: Org owner always has access, otherwise check specific permission
    if (!isOrgOwner && !canViewGroupViaPermissions) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    // Get group members for the FE response
    const members = await db.queryDrive(
      orgId,
      `SELECT c.*, cg.role, gi.id as invite_id, gi.note as invite_note
       FROM contact_groups cg
       JOIN contacts c ON cg.user_id = c.id
       LEFT JOIN group_invites gi ON gi.group_id = cg.group_id AND gi.invitee_id = c.id AND gi.invitee_type = 'USER'
       WHERE cg.group_id = ?`,
      [groupId]
    );

    // PERMIT: Get permission previews for the current user on this group record
    const permissionPreviews = await checkSystemPermissions(
      groupId as SystemResourceID, // GroupID is a SystemRecordIDEnum
      currentUserId,
      orgId
    );

    const groupFE: GroupFE = {
      ...group,
      member_previews: members.map((m) => ({
        user_id: m.id,
        name: m.name,
        avatar: m.avatar,
        note: m.invite_note, // Note from the invite, if available
        group_id: groupId,
        is_admin: m.role === GroupRole.ADMIN, // Use role from contact_groups
        invite_id: m.invite_id || "",
        last_online_ms: m.last_online_ms || 0,
      })),
      permission_previews: permissionPreviews, // PERMIT: Populate permission previews
    };

    return reply.status(200).send(createApiResponse(groupFE));
  } catch (error) {
    request.log.error("Error in getGroupHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
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
    const currentUserId = requesterApiKey.user_id;
    const orgId = request.params.org_id as DriveID;

    // PERMIT: Check if the user is the org owner, or has VIEW permission on the Groups table.
    const isOrgOwner =
      (
        await db.queryDrive(orgId, "SELECT owner_id FROM about_drive LIMIT 1")
      )[0]?.owner_id === currentUserId; // Org owner check
    const canViewGroupsTable = (
      await checkSystemPermissions(
        `TABLE_${SystemTableValueEnum.GROUPS}` as SystemResourceID,
        currentUserId,
        orgId
      )
    ).includes(SystemPermissionType.VIEW);

    if (!isOrgOwner && !canViewGroupsTable) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Forbidden: Not authorized to list groups",
        })
      );
    }

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

    const groups = await db.queryDrive(orgId, query, params);

    const hasMore = groups.length > pageSize;
    if (hasMore) {
      groups.pop(); // Remove the extra item
    }

    // Get total count
    const countResult = await db.queryDrive(
      orgId,
      "SELECT COUNT(*) as total FROM groups"
    );
    const total = countResult[0]?.total || 0;

    // Convert to GroupFE format
    const groupsFE = await Promise.all(
      groups.map(async (group: Group) => {
        // Get member previews for each group
        const members = await db.queryDrive(
          orgId,
          `SELECT c.*, cg.role, gi.id as invite_id, gi.note as invite_note
         FROM contact_groups cg
         JOIN contacts c ON cg.user_id = c.id
         LEFT JOIN group_invites gi ON gi.group_id = cg.group_id AND gi.invitee_id = c.id AND gi.invitee_type = 'USER'
         WHERE cg.group_id = ?
         LIMIT 5`, // Limit member previews for list view
          [group.id]
        );

        // PERMIT: Get permission previews for the current user on each group record
        const permissionPreviews = await checkSystemPermissions(
          group.id as SystemResourceID, // GroupID is a SystemRecordIDEnum
          currentUserId,
          orgId
        );

        return {
          ...group,
          member_previews: members.map((m) => ({
            user_id: m.id,
            name: m.name,
            avatar: m.avatar,
            note: m.invite_note,
            group_id: group.id,
            is_admin: m.role === GroupRole.ADMIN,
            invite_id: m.invite_id || "",
            last_online_ms: m.last_online_ms || 0,
          })),
          permission_previews: permissionPreviews, // PERMIT: Populate permission previews
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
        message: `Internal server error - ${error}`,
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

    // PERMIT: Check create permissions for the Groups table
    const isOrgOwner =
      (
        await db.queryDrive(orgId, "SELECT owner_id FROM about_drive LIMIT 1")
      )[0]?.owner_id === currentUserId; // Org owner check
    const canCreateGroup = (
      await checkSystemPermissions(
        `TABLE_${SystemTableValueEnum.GROUPS}` as SystemResourceID,
        currentUserId,
        orgId
      )
    ).includes(SystemPermissionType.CREATE);

    if (!isOrgOwner && !canCreateGroup) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Forbidden: Not allowed to create groups",
        })
      );
    }

    const now = Date.now();
    const groupId = body.id || `${IDPrefixEnum.Group}${uuidv4()}`;

    // Get drive info for defaults
    const driveInfo = await db.queryDrive(
      orgId,
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
      drive_id: orgId,
      endpoint_url: body.endpoint_url || driveData?.url_endpoint || "",
      labels: [],
      external_id: body.external_id,
      external_payload: body.external_payload,
      admin_invites: [],
      member_invites: [],
    };

    // Insert group using transaction
    await dbHelpers.transaction("drive", orgId, (database) => {
      // 1. Insert the group itself
      const groupStmt = database.prepare(
        `INSERT INTO groups ( 
          id, name, owner, avatar, private_note, public_note,
          created_at, last_modified_at, drive_id, endpoint_url,
          external_id, external_payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );
      groupStmt.run(
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

      // 2. Add owner to contact_groups with ADMIN role
      const contactGroupStmt = database.prepare(
        `INSERT INTO contact_groups (user_id, group_id, role) VALUES (?, ?, ?)`
      );
      contactGroupStmt.run(requesterApiKey.user_id, groupId, GroupRole.ADMIN);

      // 3. Create an initial invite record for the owner (as admin)
      const ownerInviteId = `${IDPrefixEnum.GroupInvite}${uuidv4()}`;
      const inviteStmt = database.prepare(
        `INSERT INTO group_invites (
          id, group_id, inviter_id, invitee_id, invitee_type, role, note,
          active_from, expires_at, created_at, last_modified_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );
      inviteStmt.run(
        ownerInviteId,
        groupId,
        requesterApiKey.user_id, // Inviter is the owner
        requesterApiKey.user_id,
        "USER",
        GroupRole.ADMIN, // Owner is an admin by default
        "Initial owner invite",
        0, // active from now
        -1, // never expires
        now,
        now
      );

      // 4. PERMIT: Grant system permissions to the GROUP itself (as a grantee)
      //    on its own record. This is how members of this group will inherit
      //    management capabilities.
      const groupSelfPermissionId = `${IDPrefixEnum.SystemPermission}${uuidv4()}`;
      const groupSelfPermissionStmt = database.prepare(
        `INSERT INTO permissions_system (
          id, resource_type, resource_identifier, grantee_type, grantee_id,
          granted_by, begin_date_ms, expiry_date_ms, note,
          created_at, last_modified_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );
      groupSelfPermissionStmt.run(
        groupSelfPermissionId,
        "Record", // Resource is a record
        groupId, // The group itself is the resource
        "Group", // Grantee type is Group
        groupId, // The group itself is the grantee
        requesterApiKey.user_id,
        0, // Active immediately
        -1, // Never expires
        `Default manage permissions for group ${group.name}`,
        now,
        now
      );

      // 5. PERMIT: Insert the specific permission types for the group itself
      //    This grants the "admin" level access to anyone in this group.
      const permissionTypes = [
        SystemPermissionType.VIEW,
        SystemPermissionType.EDIT,
        SystemPermissionType.DELETE,
        SystemPermissionType.INVITE,
        SystemPermissionType.CREATE, // To manage sub-resources if this were a directory
      ];
      const permissionTypeStmt = database.prepare(
        `INSERT INTO permissions_system_types (permission_id, permission_type) VALUES (?, ?)`
      );
      permissionTypes.forEach((type) => {
        permissionTypeStmt.run(groupSelfPermissionId, type);
      });
    });

    // Get owner's contact info for member_previews
    const ownerContact = await db.queryDrive(
      orgId,
      "SELECT name, avatar FROM contacts WHERE id = ?",
      [requesterApiKey.user_id]
    );

    const groupFE: GroupFE = {
      ...group,
      member_previews: [
        {
          user_id: requesterApiKey.user_id as UserID,
          name: ownerContact[0]?.name || "",
          avatar: ownerContact[0]?.avatar,
          note: "Group Creator",
          group_id: groupId as GroupID,
          is_admin: true,
          invite_id: "", // This would need to be fetched if we want the actual invite ID here
          last_online_ms: now,
        },
      ],
      // PERMIT: Get permission previews for the current user on the newly created group record
      permission_previews: await checkSystemPermissions(
        groupId as SystemResourceID,
        currentUserId,
        orgId
      ),
    };

    return reply.status(200).send(createApiResponse(groupFE));
  } catch (error) {
    request.log.error("Error in createGroupHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
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
    const currentUserId = requesterApiKey.user_id;
    const orgId = request.params.org_id as DriveID;

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
    const group = await getGroupById(body.id as GroupID, orgId);

    if (!group) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group not found",
        })
      );
    }

    // PERMIT: Check permissions - user must be the org owner, or have EDIT permission on the group record
    const isOrgOwner =
      (
        await db.queryDrive(orgId, "SELECT owner_id FROM about_drive LIMIT 1")
      )[0]?.owner_id === currentUserId; // Org owner check
    const canEditGroupViaPermissions = (
      await checkSystemPermissions(
        group.id as SystemResourceID,
        currentUserId,
        orgId
      )
    ).includes(SystemPermissionType.EDIT);

    if (!isOrgOwner && !canEditGroupViaPermissions) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Forbidden: Not allowed to edit this group",
        })
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
    values.push(group.id); // Use the validated group.id

    // Update in transaction
    await dbHelpers.transaction("drive", orgId, (database) => {
      const stmt = database.prepare(
        `UPDATE groups SET ${updates.join(", ")} WHERE id = ?`
      );
      stmt.run(...values);
    });

    // Get updated group with member previews
    const updatedGroup = await getGroupById(group.id, orgId);

    if (!updatedGroup) {
      // Should not happen after a successful update
      return reply.status(500).send(
        createApiResponse(undefined, {
          code: 500,
          message: "Failed to retrieve updated group",
        })
      );
    }

    // Get member previews
    const members = await db.queryDrive(
      orgId,
      `SELECT c.*, cg.role, gi.id as invite_id, gi.note as invite_note
       FROM contact_groups cg
       JOIN contacts c ON cg.user_id = c.id
       LEFT JOIN group_invites gi ON gi.group_id = cg.group_id AND gi.invitee_id = c.id AND gi.invitee_type = 'USER'
       WHERE cg.group_id = ?`,
      [group.id]
    );

    // PERMIT: Get permission previews for the current user on the updated group record
    const permissionPreviews = await checkSystemPermissions(
      updatedGroup.id as SystemResourceID,
      currentUserId,
      orgId
    );

    const groupFE: GroupFE = {
      ...updatedGroup,
      member_previews: members.map((m) => ({
        user_id: m.id,
        name: m.name,
        avatar: m.avatar,
        note: m.invite_note,
        group_id: group.id as GroupID,
        is_admin: m.role === GroupRole.ADMIN,
        invite_id: m.invite_id || "",
        last_online_ms: m.last_online_ms || 0,
      })),
      permission_previews: permissionPreviews,
    };

    return reply.status(200).send(createApiResponse(groupFE));
  } catch (error) {
    request.log.error("Error in updateGroupHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
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
    const currentUserId = requesterApiKey.user_id;
    const orgId = request.params.org_id as DriveID;

    // Get the group to check permissions
    const group = await getGroupById(body.id as GroupID, orgId);

    if (!group) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Group not found",
        })
      );
    }

    // PERMIT: Check permissions - user must be the org owner, or have DELETE permission on the group record
    const isOrgOwner =
      (
        await db.queryDrive(orgId, "SELECT owner_id FROM about_drive LIMIT 1")
      )[0]?.owner_id === currentUserId; // Org owner check
    const canDeleteGroupViaPermissions = (
      await checkSystemPermissions(
        group.id as SystemResourceID,
        currentUserId,
        orgId
      )
    ).includes(SystemPermissionType.DELETE);

    if (!isOrgOwner && !canDeleteGroupViaPermissions) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Forbidden: Not allowed to delete this group",
        })
      );
    }

    // Delete group and related data in transaction
    await dbHelpers.transaction("drive", orgId, (database) => {
      // Delete group invites
      database
        .prepare("DELETE FROM group_invites WHERE group_id = ?")
        .run(group.id);

      // Delete group members
      database
        .prepare("DELETE FROM contact_groups WHERE group_id = ?")
        .run(group.id);

      // Delete group labels (assuming group_labels uses plain group_id)
      database
        .prepare("DELETE FROM group_labels WHERE group_id = ?")
        .run(group.id);

      // Delete the group
      database.prepare("DELETE FROM groups WHERE id = ?").run(group.id);

      // Also remove any system permissions associated with this group ID as a resource
      database
        .prepare("DELETE FROM permissions_system WHERE resource_identifier = ?")
        .run(group.id);
      // And remove any system permissions where this group ID was the grantee
      database
        .prepare(
          "DELETE FROM permissions_system WHERE grantee_id = ? AND grantee_type = 'Group'"
        )
        .run(group.id);
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
        message: `Internal server error - ${error}`,
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

    // Check if user is a member
    // Using isUserOnLocalGroup is more accurate as this is a local validation endpoint
    const isMember = await isUserOnLocalGroup(body.user_id, group, orgId);

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
        message: `Internal server error - ${error}`,
      })
    );
  }
}
