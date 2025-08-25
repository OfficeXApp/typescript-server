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
  IResponseRedeemGroupInvite,
  IResponseDeleteGroupInvite,
  IResponseUpdateGroupInvite,
  IResponseCreateGroupInvite,
  IResponseListGroupInvites,
  IResponseGetGroupInvite,
} from "@officexapp/types"; // Adjust this path if your types are elsewhere

interface GetGroupInviteParams extends OrgIdParams {
  invite_id: GroupInviteID;
}

const groupInviteRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/drive/group_invites/get/:invite_id
  fastify.get<{ Params: GetGroupInviteParams; Reply: IResponseGetGroupInvite }>(
    "/get/:invite_id",
    { preHandler: [driveRateLimitPreHandler] },
    getGroupInviteHandler
  );

  // POST /v1/drive/group_invites/list
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestListGroupInvites;
    Reply: IResponseListGroupInvites;
  }>(
    "/list",
    { preHandler: [driveRateLimitPreHandler] },
    listGroupInvitesHandler
  );

  // POST /v1/drive/group_invites/create
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestCreateGroupInvite;
    Reply: IResponseCreateGroupInvite;
  }>(
    "/create",
    { preHandler: [driveRateLimitPreHandler] },
    createGroupInviteHandler
  );

  // POST /v1/drive/group_invites/update
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestUpdateGroupInvite;
    Reply: IResponseUpdateGroupInvite;
  }>(
    "/update",
    { preHandler: [driveRateLimitPreHandler] },
    updateGroupInviteHandler
  );

  // POST /v1/drive/group_invites/delete
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestDeleteGroupInvite;
    Reply: IResponseDeleteGroupInvite;
  }>(
    "/delete",
    { preHandler: [driveRateLimitPreHandler] },
    deleteGroupInviteHandler
  );

  // POST /v1/drive/group_invites/redeem
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestRedeemGroupInvite;
    Reply: IResponseRedeemGroupInvite;
  }>(
    "/redeem",
    { preHandler: [driveRateLimitPreHandler] },
    redeemGroupInviteHandler
  );
};

export default groupInviteRoutes;
