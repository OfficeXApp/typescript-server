import { FastifyPluginAsync } from "fastify";
import contactRoutes from "./contacts";

const driveRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // Register contacts routes with a dynamic prefix to pass down the org_id
  fastify.register(contactRoutes, { prefix: "/:org_id/contacts" });
};

export default driveRoutes;
