import { FastifyPluginAsync } from "fastify";
import apiKeyRoutes from "./api_keys";

const driveRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  fastify.register(apiKeyRoutes, { prefix: "/:org_id/api_keys" });
};

export default driveRoutes;
