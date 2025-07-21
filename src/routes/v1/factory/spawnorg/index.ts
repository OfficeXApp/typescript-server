import { FastifyPluginAsync } from "fastify";
import {
  getGiftcardSpawnOrgHandler,
  listGiftcardSpawnOrgsHandler,
  upsertGiftcardSpawnOrgHandler,
  deleteGiftcardSpawnOrgHandler,
  redeemGiftcardSpawnOrgHandler,
} from "./handlers";
import { factoryRateLimitPreHandler } from "../../../../services/rate-limit";
import {
  DeleteGiftcardSpawnOrgRequestBody,
  ListGiftcardSpawnOrgsRequestBody,
  RedeemGiftcardSpawnOrgData,
  UpsertGiftcardSpawnOrgRequestBody,
} from "@officexapp/types";

interface GetGiftcardSpawnOrgParams {
  giftcard_id: string;
}

const giftcardSpawnOrgRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/factory/giftcards/spawnorg/get/:giftcard_id
  fastify.get<{ Params: GetGiftcardSpawnOrgParams }>(
    "/get/:giftcard_id",
    getGiftcardSpawnOrgHandler
  );

  // POST /v1/factory/giftcards/spawnorg/list
  fastify.post<{ Body: ListGiftcardSpawnOrgsRequestBody }>(
    "/list",
    { preHandler: [factoryRateLimitPreHandler] },
    listGiftcardSpawnOrgsHandler
  );

  // POST /v1/factory/giftcards/spawnorg/upsert
  fastify.post<{ Body: UpsertGiftcardSpawnOrgRequestBody }>(
    "/upsert",
    { preHandler: [factoryRateLimitPreHandler] },
    upsertGiftcardSpawnOrgHandler
  );

  // POST /v1/factory/giftcards/spawnorg/delete
  fastify.post<{ Body: DeleteGiftcardSpawnOrgRequestBody }>(
    "/delete",
    { preHandler: [factoryRateLimitPreHandler] },
    deleteGiftcardSpawnOrgHandler
  );

  // POST /v1/factory/giftcards/spawnorg/redeem
  fastify.post<{ Body: RedeemGiftcardSpawnOrgData }>(
    "/redeem",
    { preHandler: [factoryRateLimitPreHandler] },
    redeemGiftcardSpawnOrgHandler
  );
};

export default giftcardSpawnOrgRoutes;
