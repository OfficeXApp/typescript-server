import { FastifyPluginAsync } from "fastify";
import {
  getGiftcardRefuelHandler,
  listGiftcardRefuelsHandler,
  upsertGiftcardRefuelHandler,
  deleteGiftcardRefuelHandler,
  redeemGiftcardRefuelHandler,
} from "./handlers";

const giftcardRefuelRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/factory/giftcards/refuel/get/:giftcard_id
  fastify.get("/get/:giftcard_id", getGiftcardRefuelHandler);

  // POST /v1/factory/giftcards/refuel/list
  fastify.post("/list", listGiftcardRefuelsHandler);

  // POST /v1/factory/giftcards/refuel/upsert
  fastify.post("/upsert", upsertGiftcardRefuelHandler);

  // POST /v1/factory/giftcards/refuel/delete
  fastify.post("/delete", deleteGiftcardRefuelHandler);

  // POST /v1/factory/giftcards/refuel/redeem
  fastify.post("/redeem", redeemGiftcardRefuelHandler);
};

export default giftcardRefuelRoutes;
