import { FastifyPluginAsync } from "fastify";
import {
  getGroupInviteHandler,
  listGroupInvitesHandler,
  createGroupInviteHandler,
  updateGroupInviteHandler,
  deleteGroupInviteHandler,
  redeemGroupInviteHandler,
} from "./handlers";

const groupInviteRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/drive/group_invites/get/:invite_id
  fastify.get("/get/:invite_id", getGroupInviteHandler);

  // POST /v1/drive/group_invites/list
  fastify.post("/list", listGroupInvitesHandler);

  // POST /v1/drive/group_invites/create
  fastify.post("/create", createGroupInviteHandler);

  // POST /v1/drive/group_invites/update
  fastify.post("/update", updateGroupInviteHandler);

  // POST /v1/drive/group_invites/delete
  fastify.post("/delete", deleteGroupInviteHandler);

  // POST /v1/drive/group_invites/redeem
  fastify.post("/redeem", redeemGroupInviteHandler);
};

export default groupInviteRoutes;
