import { FastifyPluginAsync } from "fastify";
import apiKeyRoutes from "./api_keys";
import contactsRoutes from "./contacts";
import disksRoutes from "./disks";
import groupRoutes from "./groups";
import groupInviteRoutes from "./group_invites";

const driveRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  fastify.register(apiKeyRoutes, { prefix: "/:org_id/api_keys" });
  fastify.register(contactsRoutes, { prefix: "/:org_id/contacts" });
  fastify.register(disksRoutes, { prefix: "/:org_id/disks" });
  fastify.register(groupRoutes, { prefix: "/:org_id/groups" });
  fastify.register(groupInviteRoutes, { prefix: "/:org_id/group_invites" });
};

export default driveRoutes;
