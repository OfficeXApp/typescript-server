import { FastifyPluginAsync } from "fastify";
import {
  migrateFactoryHandler,
  snapshotFactoryHandler,
} from "./api_keys/handlers";
import giftcardSpawnOrgRoutes from "./spawnorg";
import { factoryRateLimitPreHandler } from "../../../services/rate-limit";
import {
  DriveID,
  IRequestAutoLoginLink,
  IRequestGenerateCryptoIdentity,
} from "@officexapp/types";
import { generateCryptoIdentityHandler } from "../drive/contacts/handlers";

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
    "/helpers/migrate",
    { preHandler: [factoryRateLimitPreHandler] },
    migrateFactoryHandler
  );

  // POST /helpers/generate-crypto-identity
  fastify.post<{ Body: IRequestGenerateCryptoIdentity }>(
    `/helpers/generate-crypto-identity`,
    { preHandler: [factoryRateLimitPreHandler] }, // Add the preHandler here
    generateCryptoIdentityHandler
  );
};

export default factoryRoutes;
