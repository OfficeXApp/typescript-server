// src/rest/groups/handler.ts

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
  GroupMemberPreview,
  GroupInviteeTypeEnum,
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import { authenticateRequest } from "../../../../services/auth";
import { createApiResponse, OrgIdParams } from "../../types";
import {
  checkSystemPermissions,
  // hasSystemManagePermission, // Not used here directly for core logic
} from "../../../../services/permissions/system";
import {
  getGroupById,
  isGroupAdmin,
  isUserOnLocalGroup,
  getGroupInviteById, // Added for fetching invite details
  addGroupMember, // Use this for adding members/admins
  removeMemberFromGroup, // Use this for removing members
  addAdminToGroup, // New import
} from "../../../../services/groups";

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

    // PERMIT: Authorization logic
    // A user can view a group if they are:
    // 1. The organization owner.
    // 2. An admin of the group (derived from group_invites).
    // 3. A member of the group (derived from group_invites).
    // 4. Have explicit VIEW permission on the group record via permissions_system.
    // 5. Have explicit VIEW permission on the Groups table via permissions_system.

    const isOrgOwner =
      (
        await db.queryDrive(orgId, "SELECT owner_id FROM about_drive LIMIT 1")
      )[0]?.owner_id === currentUserId;

    const isGroupAdminStatus = await isGroupAdmin(
      currentUserId,
      groupId,
      orgId
    ); // Checks group_invites
    const isGroupMemberStatus = await isUserOnLocalGroup(
      currentUserId,
      group,
      orgId
    ); // Checks group_invites

    const canViewGroupRecordViaPermissions = (
      await checkSystemPermissions(
        groupId as SystemResourceID, // GroupID is a SystemRecordIDEnum
        currentUserId,
        orgId
      )
    ).includes(SystemPermissionType.VIEW);

    const canViewGroupsTableViaPermissions = (
      await checkSystemPermissions(
        `TABLE_${SystemTableValueEnum.GROUPS}` as SystemResourceID,
        currentUserId,
        orgId
      )
    ).includes(SystemPermissionType.VIEW);

    if (
      !isOrgOwner &&
      !isGroupAdminStatus &&
      !isGroupMemberStatus &&
      !canViewGroupRecordViaPermissions &&
      !canViewGroupsTableViaPermissions
    ) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    // Populate member_previews directly from group_invites
    const memberPreviews: GroupMemberPreview[] = [];
    for (const inviteId of [
      ...new Set([...group.admin_invites, ...group.member_invites]),
    ]) {
      const invite = await getGroupInviteById(inviteId, orgId);

      // Ensure invite exists and is not expired/inactive (already handled by getGroupById, but defensive check)
      if (!invite) {
        continue;
      }

      let inviteeName: string = ""; // Default to empty string as per Rust sample
      let inviteeAvatar: string | null = null; // Default to null as per Rust sample
      let lastOnlineMs: number = 0; // Default to 0 as per Rust sample

      // Determine name, avatar, last_online_ms based on invitee_id type
      if (invite.invitee_id.startsWith(IDPrefixEnum.User)) {
        const contactInfo = await db.queryDrive(
          orgId,
          "SELECT name, avatar, last_online_ms FROM contacts WHERE id = ?",
          [invite.invitee_id]
        );
        const contact = contactInfo[0];

        if (contact) {
          inviteeName = contact.name;
          inviteeAvatar = contact.avatar;
          lastOnlineMs = contact.last_online_ms;
        } else {
          // If contact not found for a UserID (Rust output shows empty name/null avatar)
          inviteeName = `Unknown User (${invite.invitee_id})`; // You can set this to "" if you prefer strict match to Rust for non-existent contacts
          inviteeAvatar = null;
          lastOnlineMs = 0;
        }
      } else if (invite.invitee_id === GroupInviteeTypeEnum.PUBLIC) {
        // For Public invites, name is "", avatar is null, last_online_ms is 0 as per Rust sample
        // These values are already the defaults, so no explicit assignment is strictly needed.
      } else if (
        invite.invitee_id.startsWith(IDPrefixEnum.PlaceholderGroupInviteeID)
      ) {
        // For Placeholder (magic link) invites, name is "", avatar is null, last_online_ms is 0 as per Rust sample
        // These values are already the defaults, so no explicit assignment is strictly needed.
      }

      memberPreviews.push({
        user_id: invite.invitee_id as UserID, // Assign the invitee_id directly, which can be UserID, PUBLIC, or PlaceholderGroupInviteeID
        name: inviteeName,
        note: invite.note,
        avatar: inviteeAvatar || undefined,
        group_id: invite.group_id,
        is_admin: invite.role === GroupRole.ADMIN,
        invite_id: invite.id,
        last_online_ms: lastOnlineMs,
      });
    } // End of for loop

    // PERMIT: Get permission previews for the current user on this group record
    const permissionPreviews = await checkSystemPermissions(
      groupId as SystemResourceID, // GroupID is a SystemRecordIDEnum
      currentUserId,
      orgId
    );

    const groupFE: GroupFE = {
      ...group,
      member_previews: memberPreviews,
      permission_previews: permissionPreviews,
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
      )[0]?.owner_id === currentUserId;

    const canViewGroupsTable = (
      await checkSystemPermissions(
        `TABLE_${SystemTableValueEnum.GROUPS}` as SystemResourceID,
        currentUserId,
        orgId
      )
    ).includes(SystemPermissionType.VIEW);

    if (!isOrgOwner && !canViewGroupsTable) {
      // If user doesn't have general table view permission,
      // we still need to check groups they are a member of.
      // This is implicit in the full Rust logic via checkSystemPermissions,
      // but for list, we perform a broader filter.
      // For now, if no table permission, only return groups they are a member of.
      let memberGroupsQuery = `
          SELECT DISTINCT g.*
          FROM groups g
          JOIN group_invites gi ON g.id = gi.group_id
          WHERE gi.invitee_id = ? AND gi.invitee_type = ?
          AND gi.active_from <= ? AND (gi.expires_at <= 0 OR gi.expires_at > ?)
        `;
      const memberGroupsParams: any[] = [
        currentUserId,
        GroupInviteeTypeEnum.USER,
        Date.now(),
        Date.now(),
      ];

      let memberGroupsCountQuery = `
        SELECT COUNT(DISTINCT g.id) AS total
        FROM groups g
        JOIN group_invites gi ON g.id = gi.group_id
        WHERE gi.invitee_id = ? AND gi.invitee_type = ?
        AND gi.active_from <= ? AND (gi.expires_at <= 0 OR gi.expires_at > ?)
      `;
      const memberGroupsCountParams: any[] = [
        currentUserId,
        GroupInviteeTypeEnum.USER,
        Date.now(),
        Date.now(),
      ];

      if (cursor) {
        memberGroupsQuery += ` AND g.created_at ${direction === "ASC" ? ">" : "<"} ?`;
        memberGroupsParams.push(cursor);
      }
      memberGroupsQuery += ` ORDER BY g.created_at ${direction} LIMIT ?`;
      memberGroupsParams.push(pageSize + 1);

      const memberGroups = await db.queryDrive(
        orgId,
        memberGroupsQuery,
        memberGroupsParams
      );
      const memberGroupsTotalResult = await db.queryDrive(
        orgId,
        memberGroupsCountQuery,
        memberGroupsCountParams
      );
      const memberGroupsTotal = memberGroupsTotalResult[0]?.total || 0;

      const hasMoreMemberGroups = memberGroups.length > pageSize;
      if (hasMoreMemberGroups) {
        memberGroups.pop();
      }

      const memberGroupsFE = await Promise.all(
        memberGroups.map(async (groupData: Group) => {
          const groupObj = await getGroupById(groupData.id, orgId); // Get full group object
          const memberPreviews: GroupMemberPreview[] = [];
          if (groupObj) {
            // Should always exist if retrieved from DB
            // Populate member previews from group_invites for this group
            for (const inviteId of [
              ...new Set([
                ...groupObj.admin_invites,
                ...groupObj.member_invites,
              ]),
            ]) {
              const invite = await getGroupInviteById(inviteId, orgId);

              if (!invite) {
                continue;
              }

              let inviteeName: string = "";
              let inviteeAvatar: string | null = null;
              let lastOnlineMs: number = 0;

              if (invite.invitee_id.startsWith(IDPrefixEnum.User)) {
                const contactInfo = await db.queryDrive(
                  orgId,
                  "SELECT name, avatar, last_online_ms FROM contacts WHERE id = ?",
                  [invite.invitee_id]
                );
                const contact = contactInfo[0];
                if (contact) {
                  inviteeName = contact.name;
                  inviteeAvatar = contact.avatar;
                  lastOnlineMs = contact.last_online_ms;
                } else {
                  inviteeName = `Unknown User (${invite.invitee_id})`;
                  inviteeAvatar = null;
                  lastOnlineMs = 0;
                }
              } else if (invite.invitee_id === GroupInviteeTypeEnum.PUBLIC) {
                // Handled by initial defaults
              } else if (
                invite.invitee_id.startsWith(
                  IDPrefixEnum.PlaceholderGroupInviteeID
                )
              ) {
                // Handled by initial defaults
              }

              memberPreviews.push({
                user_id: invite.invitee_id as UserID,
                name: inviteeName,
                note: invite.note,
                avatar: inviteeAvatar || undefined,
                group_id: invite.group_id,
                is_admin: invite.role === GroupRole.ADMIN,
                invite_id: invite.id,
                last_online_ms: lastOnlineMs,
              });
            }
          }

          const permissionPreviews = await checkSystemPermissions(
            groupData.id as SystemResourceID,
            currentUserId,
            orgId
          );

          return {
            ...groupData,
            member_previews: memberPreviews,
            permission_previews: permissionPreviews,
          };
        })
      );

      const nextCursorMemberGroups =
        hasMoreMemberGroups && memberGroups.length > 0
          ? memberGroups[memberGroups.length - 1].created_at.toString()
          : null;

      return reply.status(200).send(
        createApiResponse({
          items: memberGroupsFE,
          page_size: pageSize,
          total: memberGroupsTotal,
          direction,
          cursor: nextCursorMemberGroups,
        })
      );
    }

    // Full access (owner or has table permission)
    let query = `
      SELECT id, name, owner, avatar, public_note, private_note, created_at, last_modified_at, drive_id, endpoint_url, external_id, external_payload
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
      groups.map(async (groupData: Group) => {
        const groupObj = await getGroupById(groupData.id, orgId); // Get full group object
        const memberPreviews: GroupMemberPreview[] = [];
        if (groupObj) {
          for (const inviteId of [
            ...new Set([...groupObj.admin_invites, ...groupObj.member_invites]),
          ]) {
            const invite = await getGroupInviteById(inviteId, orgId);

            if (!invite) {
              continue;
            }

            let inviteeName: string = "";
            let inviteeAvatar: string | null = null;
            let lastOnlineMs: number = 0;

            if (invite.invitee_id.startsWith(IDPrefixEnum.User)) {
              const contactInfo = await db.queryDrive(
                orgId,
                "SELECT name, avatar, last_online_ms FROM contacts WHERE id = ?",
                [invite.invitee_id]
              );
              const contact = contactInfo[0];
              if (contact) {
                inviteeName = contact.name;
                inviteeAvatar = contact.avatar;
                lastOnlineMs = contact.last_online_ms;
              } else {
                inviteeName = `Unknown User (${invite.invitee_id})`;
                inviteeAvatar = null;
                lastOnlineMs = 0;
              }
            } else if (invite.invitee_id === GroupInviteeTypeEnum.PUBLIC) {
              // Handled by initial defaults
            } else if (
              invite.invitee_id.startsWith(
                IDPrefixEnum.PlaceholderGroupInviteeID
              )
            ) {
              // Handled by initial defaults
            }

            memberPreviews.push({
              user_id: invite.invitee_id as UserID,
              name: inviteeName,
              note: invite.note,
              avatar: inviteeAvatar || undefined,
              group_id: invite.group_id,
              is_admin: invite.role === GroupRole.ADMIN,
              invite_id: invite.id,
              last_online_ms: lastOnlineMs,
            });
          }
        }

        const permissionPreviews = await checkSystemPermissions(
          groupData.id as SystemResourceID,
          currentUserId,
          orgId
        );

        return {
          ...groupData,
          member_previews: memberPreviews,
          permission_previews: permissionPreviews,
        };
      })
    );

    const nextCursor =
      hasMore && groups.length > 0
        ? groups[groups.length - 1].created_at.toString()
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
      )[0]?.owner_id === currentUserId;
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
      "SELECT url_endpoint FROM about_drive LIMIT 1"
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
      labels: [], // Labels explicitly ignored
      external_id: body.external_id,
      external_payload: body.external_payload,
      admin_invites: [], // Will be populated by addGroupMember
      member_invites: [], // Will be populated by addGroupMember
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

      // 2. Grant system permissions to the GROUP itself (as a grantee) on its own record.
      // This is how members of this group will implicitly inherit capabilities.
      // This permission is for the group (as an entity) managing itself/being managed.
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

      // 3. Insert the specific permission types for the group itself (e.g., manage, view, edit)
      const groupPermissionTypes = [
        SystemPermissionType.VIEW,
        SystemPermissionType.EDIT,
        SystemPermissionType.DELETE,
        SystemPermissionType.INVITE,
        SystemPermissionType.CREATE,
      ];
      const permissionTypeStmt = database.prepare(
        `INSERT INTO permissions_system_types (permission_id, permission_type) VALUES (?, ?)`
      );
      groupPermissionTypes.forEach((type) => {
        permissionTypeStmt.run(groupSelfPermissionId, type);
      });
    });

    // 4. Add the creator as an admin member of the group
    await addAdminToGroup(group.id, currentUserId, currentUserId, orgId);

    // Get updated group with members and permissions
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

    const memberPreviews: GroupMemberPreview[] = [];
    for (const inviteId of [
      ...new Set([
        ...updatedGroup.admin_invites,
        ...updatedGroup.member_invites,
      ]),
    ]) {
      const invite = await getGroupInviteById(inviteId, orgId);
      if (invite && invite.invitee_id.startsWith(IDPrefixEnum.User)) {
        const contactInfo = await db.queryDrive(
          orgId,
          "SELECT name, avatar, last_online_ms FROM contacts WHERE id = ?",
          [invite.invitee_id]
        );
        const contact = contactInfo[0];
        if (contact) {
          memberPreviews.push({
            user_id: invite.invitee_id as UserID,
            name: contact.name,
            note: invite.note,
            avatar: contact.avatar,
            group_id: invite.group_id,
            is_admin: invite.role === GroupRole.ADMIN,
            invite_id: invite.id,
            last_online_ms: contact.last_online_ms,
          });
        } else {
          memberPreviews.push({
            user_id: invite.invitee_id as UserID,
            name: `Unknown User (${invite.invitee_id})`,
            note: invite.note,
            avatar: undefined,
            group_id: invite.group_id,
            is_admin: invite.role === GroupRole.ADMIN,
            invite_id: invite.id,
            last_online_ms: 0,
          });
        }
      }
    }

    // PERMIT: Get permission previews for the current user on the newly created group record
    const permissionPreviews = await checkSystemPermissions(
      group.id as SystemResourceID,
      currentUserId,
      orgId
    );

    const groupFE: GroupFE = {
      ...group,
      member_previews: memberPreviews,
      permission_previews: permissionPreviews,
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

    // PERMIT: Check permissions - user must be the org owner, a group admin, or have EDIT permission on the group record
    const isOrgOwner =
      (
        await db.queryDrive(orgId, "SELECT owner_id FROM about_drive LIMIT 1")
      )[0]?.owner_id === currentUserId;

    const isGroupAdminStatus = await isGroupAdmin(
      currentUserId,
      group.id,
      orgId
    );

    const canEditGroupViaPermissions = (
      await checkSystemPermissions(
        group.id as SystemResourceID,
        currentUserId,
        orgId
      )
    ).includes(SystemPermissionType.EDIT);

    if (!isOrgOwner && !isGroupAdminStatus && !canEditGroupViaPermissions) {
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

    const memberPreviews: GroupMemberPreview[] = [];
    for (const inviteId of [
      ...new Set([
        ...updatedGroup.admin_invites,
        ...updatedGroup.member_invites,
      ]),
    ]) {
      const invite = await getGroupInviteById(inviteId, orgId);
      if (invite && invite.invitee_id.startsWith(IDPrefixEnum.User)) {
        const contactInfo = await db.queryDrive(
          orgId,
          "SELECT name, avatar, last_online_ms FROM contacts WHERE id = ?",
          [invite.invitee_id]
        );
        const contact = contactInfo[0];
        if (contact) {
          memberPreviews.push({
            user_id: invite.invitee_id as UserID,
            name: contact.name,
            note: invite.note,
            avatar: contact.avatar,
            group_id: invite.group_id,
            is_admin: invite.role === GroupRole.ADMIN,
            invite_id: invite.id,
            last_online_ms: contact.last_online_ms,
          });
        } else {
          memberPreviews.push({
            user_id: invite.invitee_id as UserID,
            name: `Unknown User (${invite.invitee_id})`,
            note: invite.note,
            avatar: undefined,
            group_id: invite.group_id,
            is_admin: invite.role === GroupRole.ADMIN,
            invite_id: invite.id,
            last_online_ms: 0,
          });
        }
      }
    }

    // PERMIT: Get permission previews for the current user on the updated group record
    const permissionPreviews = await checkSystemPermissions(
      updatedGroup.id as SystemResourceID,
      currentUserId,
      orgId
    );

    const groupFE: GroupFE = {
      ...updatedGroup,
      member_previews: memberPreviews,
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

    // PERMIT: Check permissions - user must be the org owner, a group admin, or have DELETE permission on the group record
    const isOrgOwner =
      (
        await db.queryDrive(orgId, "SELECT owner_id FROM about_drive LIMIT 1")
      )[0]?.owner_id === currentUserId;

    const isGroupAdminStatus = await isGroupAdmin(
      currentUserId,
      group.id,
      orgId
    );

    const canDeleteGroupViaPermissions = (
      await checkSystemPermissions(
        group.id as SystemResourceID,
        currentUserId,
        orgId
      )
    ).includes(SystemPermissionType.DELETE);

    if (!isOrgOwner && !isGroupAdminStatus && !canDeleteGroupViaPermissions) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Forbidden: Not allowed to delete this group",
        })
      );
    }

    // Delete group and related data in transaction
    await dbHelpers.transaction("drive", orgId, (database) => {
      // Delete group invites (CASCADE from group in schema, but good to be explicit)
      database
        .prepare("DELETE FROM group_invites WHERE group_id = ?")
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

    // Check if user is a member using the refined isUserOnLocalGroup
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
