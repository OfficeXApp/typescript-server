import { FastifyPluginAsync } from "fastify";
import {
  getGroupHandler,
  listGroupsHandler,
  createGroupHandler,
  updateGroupHandler,
  deleteGroupHandler,
  validateGroupMemberHandler,
} from "./handlers";

const groupRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/drive/groups/get/:group_id
  fastify.get("/get/:group_id", getGroupHandler);

  // POST /v1/drive/groups/list
  fastify.post("/list", listGroupsHandler);

  // POST /v1/drive/groups/create
  fastify.post("/create", createGroupHandler);

  // POST /v1/drive/groups/update
  fastify.post("/update", updateGroupHandler);

  // POST /v1/drive/groups/delete
  fastify.post("/delete", deleteGroupHandler);

  // POST /v1/drive/groups/validate
  fastify.post("/validate", validateGroupMemberHandler);
};

export default groupRoutes;
