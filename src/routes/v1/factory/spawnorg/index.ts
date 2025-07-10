import { FastifyPluginAsync } from "fastify";
import {
  getGiftcardSpawnOrgHandler,
  listGiftcardSpawnOrgsHandler,
  upsertGiftcardSpawnOrgHandler,
  deleteGiftcardSpawnOrgHandler,
  redeemGiftcardSpawnOrgHandler,
} from "./handlers";

const giftcardSpawnOrgRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/factory/giftcards/spawnorg/get/:giftcard_id
  fastify.get("/get/:giftcard_id", getGiftcardSpawnOrgHandler);

  // POST /v1/factory/giftcards/spawnorg/list
  fastify.post("/list", listGiftcardSpawnOrgsHandler);

  // POST /v1/factory/giftcards/spawnorg/upsert
  fastify.post("/upsert", upsertGiftcardSpawnOrgHandler);

  // POST /v1/factory/giftcards/spawnorg/delete
  fastify.post("/delete", deleteGiftcardSpawnOrgHandler);

  // POST /v1/factory/giftcards/spawnorg/redeem
  fastify.post("/redeem", redeemGiftcardSpawnOrgHandler);
};

export default giftcardSpawnOrgRoutes;
