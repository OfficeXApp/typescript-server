import { FastifyPluginAsync } from "fastify";
import {
  migrateFactoryHandler,
  snapshotFactoryHandler,
} from "./api_keys/handlers";
import giftcardSpawnOrgRoutes from "./spawnorg";
import { factoryRateLimitPreHandler } from "../../../services/rate-limit";
import { DriveID } from "@officexapp/types";

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
  // migrate
  fastify.post<{
    Body: {
      drives?: DriveID[];
    };
  }>(
    "/migrate",
    { preHandler: [factoryRateLimitPreHandler] },
    migrateFactoryHandler
  );
};

export default factoryRoutes;
