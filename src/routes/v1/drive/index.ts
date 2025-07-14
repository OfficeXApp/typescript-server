import { FastifyPluginAsync } from "fastify";
import apiKeyRoutes from "./api_keys";
import contactsRoutes from "./contacts";
import disksRoutes from "./disks";
import groupRoutes from "./groups";
import groupInviteRoutes from "./group_invites";
import labelsRoutes from "./labels";
import organizationRoutes from "./organization";
import permissionsRoutes from "./permissions";
import webhooksRoutes from "./webhooks";
import directoryRoutes from "./directory";

const driveRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  fastify.register(apiKeyRoutes, { prefix: "/:org_id/api_keys" });
  fastify.register(contactsRoutes, { prefix: "/:org_id/contacts" });
  fastify.register(directoryRoutes, { prefix: "/:org_id/directory" });
  fastify.register(disksRoutes, { prefix: "/:org_id/disks" });
  fastify.register(groupInviteRoutes, { prefix: "/:org_id/group_invites" });
  fastify.register(groupRoutes, { prefix: "/:org_id/groups" });
  fastify.register(labelsRoutes, { prefix: "/:org_id/labels" });
  fastify.register(organizationRoutes, { prefix: "/:org_id/organization" });
  fastify.register(permissionsRoutes, { prefix: "/:org_id/permissions" });
  fastify.register(webhooksRoutes, { prefix: "/:org_id/webhooks" });
};

export default driveRoutes;
