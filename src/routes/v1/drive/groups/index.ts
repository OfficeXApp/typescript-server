// src/routes/v1/drive/groups/index.ts

import { FastifyPluginAsync } from "fastify";
import {
  getGroupHandler,
  listGroupsHandler,
  createGroupHandler,
  updateGroupHandler,
  deleteGroupHandler,
  validateGroupMemberHandler,
} from "./handlers";
import { driveRateLimitPreHandler } from "../../../../services/rate-limit"; // Import the preHandler
import { OrgIdParams } from "../../types";
import {
  GroupID, // Assuming you have a GroupID type
  IRequestCreateGroup,
  IRequestDeleteGroup,
  IRequestListGroups,
  IRequestUpdateGroup,
  IRequestValidateGroupMember,
} from "@officexapp/types"; // Adjust this path if your types are elsewhere

interface GetGroupParams extends OrgIdParams {
  group_id: GroupID;
}

const groupRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/drive/groups/get/:group_id
  fastify.get<{ Params: GetGroupParams }>(
    "/get/:group_id",
    { preHandler: [driveRateLimitPreHandler] },
    getGroupHandler
  );

  // POST /v1/drive/groups/list
  fastify.post<{ Params: OrgIdParams; Body: IRequestListGroups }>(
    "/list",
    { preHandler: [driveRateLimitPreHandler] },
    listGroupsHandler
  );

  // POST /v1/drive/groups/create
  fastify.post<{ Params: OrgIdParams; Body: IRequestCreateGroup }>(
    "/create",
    { preHandler: [driveRateLimitPreHandler] },
    createGroupHandler
  );

  // POST /v1/drive/groups/update
  fastify.post<{ Params: OrgIdParams; Body: IRequestUpdateGroup }>(
    "/update",
    { preHandler: [driveRateLimitPreHandler] },
    updateGroupHandler
  );

  // POST /v1/drive/groups/delete
  fastify.post<{ Params: OrgIdParams; Body: IRequestDeleteGroup }>(
    "/delete",
    { preHandler: [driveRateLimitPreHandler] },
    deleteGroupHandler
  );

  // POST /v1/drive/groups/validate
  fastify.post<{ Params: OrgIdParams; Body: IRequestValidateGroupMember }>(
    "/validate",
    { preHandler: [driveRateLimitPreHandler] },
    validateGroupMemberHandler
  );
};

export default groupRoutes;
