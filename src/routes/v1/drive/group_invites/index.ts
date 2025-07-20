// src/routes/v1/drive/group_invites/index.ts

import { FastifyPluginAsync } from "fastify";
import {
  getGroupInviteHandler,
  listGroupInvitesHandler,
  createGroupInviteHandler,
  updateGroupInviteHandler,
  deleteGroupInviteHandler,
  redeemGroupInviteHandler,
} from "./handlers";
import { driveRateLimitPreHandler } from "../../../../services/rate-limit"; // Import the preHandler
import { OrgIdParams } from "../../types";
import {
  GroupInviteID, // Assuming you have a GroupInviteID type
  IRequestCreateGroupInvite,
  IRequestDeleteGroupInvite,
  IRequestListGroupInvites,
  IRequestUpdateGroupInvite,
  IRequestRedeemGroupInvite,
} from "@officexapp/types"; // Adjust this path if your types are elsewhere

interface GetGroupInviteParams extends OrgIdParams {
  invite_id: GroupInviteID;
}

const groupInviteRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/drive/group_invites/get/:invite_id
  fastify.get<{ Params: GetGroupInviteParams }>(
    "/get/:invite_id",
    { preHandler: [driveRateLimitPreHandler] },
    getGroupInviteHandler
  );

  // POST /v1/drive/group_invites/list
  fastify.post<{ Params: OrgIdParams; Body: IRequestListGroupInvites }>(
    "/list",
    { preHandler: [driveRateLimitPreHandler] },
    listGroupInvitesHandler
  );

  // POST /v1/drive/group_invites/create
  fastify.post<{ Params: OrgIdParams; Body: IRequestCreateGroupInvite }>(
    "/create",
    { preHandler: [driveRateLimitPreHandler] },
    createGroupInviteHandler
  );

  // POST /v1/drive/group_invites/update
  fastify.post<{ Params: OrgIdParams; Body: IRequestUpdateGroupInvite }>(
    "/update",
    { preHandler: [driveRateLimitPreHandler] },
    updateGroupInviteHandler
  );

  // POST /v1/drive/group_invites/delete
  fastify.post<{ Params: OrgIdParams; Body: IRequestDeleteGroupInvite }>(
    "/delete",
    { preHandler: [driveRateLimitPreHandler] },
    deleteGroupInviteHandler
  );

  // POST /v1/drive/group_invites/redeem
  fastify.post<{ Params: OrgIdParams; Body: IRequestRedeemGroupInvite }>(
    "/redeem",
    { preHandler: [driveRateLimitPreHandler] },
    redeemGroupInviteHandler
  );
};

export default groupInviteRoutes;
