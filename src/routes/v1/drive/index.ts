import { FastifyPluginAsync, FastifyReply, FastifyRequest } from "fastify";
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
import drivesRoutes from "./drives";
import { getDriveDbPath } from "../../../services/database";
import fs from "fs";

const driveRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  const orgIdPreHandler = async (
    request: FastifyRequest,
    reply: FastifyReply
  ) => {
    const params = request.params as { org_id?: string };
    const orgId = params.org_id;

    if (!orgId) {
      throw fastify.httpErrors.badRequest("Organization ID is required.");
    }

    try {
      const dbFilePath = getDriveDbPath(orgId);
      if (!fs.existsSync(dbFilePath)) {
        throw fastify.httpErrors.notFound(
          `Organization database for ID '${orgId}' not found on this server.`
        );
      }

      return;
    } catch (error: any) {
      if (
        error.message &&
        typeof error.message === "string" &&
        error.message.includes("Invalid drive ID format")
      ) {
        throw fastify.httpErrors.badRequest(error.message);
      }
      console.error(
        `Unexpected error in orgIdPreHandler for orgId ${orgId}:`,
        error
      );
      throw error;
    }
  };
  fastify.addHook("preHandler", orgIdPreHandler);

  fastify.register(apiKeyRoutes, { prefix: "/:org_id/api_keys" });
  fastify.register(contactsRoutes, { prefix: "/:org_id/contacts" });
  fastify.register(directoryRoutes, { prefix: "/:org_id/directory" });
  fastify.register(disksRoutes, { prefix: "/:org_id/disks" });
  fastify.register(drivesRoutes, { prefix: "/:org_id/drives" });
  fastify.register(groupInviteRoutes, { prefix: "/:org_id/groups/invites" });
  fastify.register(groupRoutes, { prefix: "/:org_id/groups" });
  fastify.register(labelsRoutes, { prefix: "/:org_id/labels" });
  fastify.register(organizationRoutes, { prefix: "/:org_id/organization" });
  fastify.register(permissionsRoutes, { prefix: "/:org_id/permissions" });
  fastify.register(webhooksRoutes, { prefix: "/:org_id/webhooks" });
};

export default driveRoutes;
