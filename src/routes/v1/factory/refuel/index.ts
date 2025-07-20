import { FastifyPluginAsync } from "fastify";
import {
  getGiftcardRefuelHandler,
  listGiftcardRefuelsHandler,
  upsertGiftcardRefuelHandler,
  deleteGiftcardRefuelHandler,
  redeemGiftcardRefuelHandler,
  GetGiftcardRefuelParams,
} from "./handlers";
import { factoryRateLimitPreHandler } from "../../../../services/rate-limit";
import {
  DeleteGiftcardRefuelRequestBody,
  ListGiftcardRefuelsRequestBody,
  RedeemGiftcardRefuelData,
  UpsertGiftcardRefuelRequestBody,
} from "@officexapp/types";

const giftcardRefuelRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/factory/giftcards/refuel/get/:giftcard_id
  fastify.get<{ Params: GetGiftcardRefuelParams }>(
    "/get/:giftcard_id",
    { preHandler: [factoryRateLimitPreHandler] },
    getGiftcardRefuelHandler
  );

  // POST /v1/factory/giftcards/refuel/list
  fastify.post<{ Body: ListGiftcardRefuelsRequestBody }>(
    "/list",
    { preHandler: [factoryRateLimitPreHandler] },
    listGiftcardRefuelsHandler
  );

  // POST /v1/factory/giftcards/refuel/upsert
  fastify.post<{ Body: UpsertGiftcardRefuelRequestBody }>(
    "/upsert",
    { preHandler: [factoryRateLimitPreHandler] },
    upsertGiftcardRefuelHandler
  );

  // POST /v1/factory/giftcards/refuel/delete
  fastify.post<{ Body: DeleteGiftcardRefuelRequestBody }>(
    "/delete",
    { preHandler: [factoryRateLimitPreHandler] },
    deleteGiftcardRefuelHandler
  );

  // POST /v1/factory/giftcards/refuel/redeem
  fastify.post<{ Body: RedeemGiftcardRefuelData }>(
    "/redeem",
    { preHandler: [factoryRateLimitPreHandler] },
    redeemGiftcardRefuelHandler
  );
};

export default giftcardRefuelRoutes;
