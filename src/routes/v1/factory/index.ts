import { FastifyPluginAsync } from "fastify";
import { snapshotFactoryHandler } from "./api_keys/handlers";

const factoryRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // snapshot
  fastify.get("/snapshot", snapshotFactoryHandler);
};

export default factoryRoutes;
