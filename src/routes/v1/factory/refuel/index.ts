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
  IRequestDeleteGiftcardRefuel,
  IRequestListGiftcardRefuels,
  IRedeemGiftcardRefuelData,
  IRequestUpsertGiftcardRefuel,
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
  fastify.post<{ Body: IRequestListGiftcardRefuels }>(
    "/list",
    { preHandler: [factoryRateLimitPreHandler] },
    listGiftcardRefuelsHandler
  );

  // POST /v1/factory/giftcards/refuel/upsert
  fastify.post<{ Body: IRequestUpsertGiftcardRefuel }>(
    "/upsert",
    { preHandler: [factoryRateLimitPreHandler] },
    upsertGiftcardRefuelHandler
  );

  // POST /v1/factory/giftcards/refuel/delete
  fastify.post<{ Body: IRequestDeleteGiftcardRefuel }>(
    "/delete",
    { preHandler: [factoryRateLimitPreHandler] },
    deleteGiftcardRefuelHandler
  );

  // POST /v1/factory/giftcards/refuel/redeem
  fastify.post<{ Body: IRedeemGiftcardRefuelData }>(
    "/redeem",
    { preHandler: [factoryRateLimitPreHandler] },
    redeemGiftcardRefuelHandler
  );
};

export default giftcardRefuelRoutes;
