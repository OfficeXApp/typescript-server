import { FastifyPluginAsync } from "fastify";
import { snapshotFactoryHandler } from "./api_keys/handlers";
import giftcardSpawnOrgRoutes from "./spawnorg";
import { factoryRateLimitPreHandler } from "../../../services/rate-limit";

const factoryRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // snapshot
  fastify.get(
    "/snapshot",
    { preHandler: [factoryRateLimitPreHandler] },
    snapshotFactoryHandler
  );
  // giftcards
  fastify.register(giftcardSpawnOrgRoutes, { prefix: "/giftcards/spawnorg" });
};

export default factoryRoutes;
