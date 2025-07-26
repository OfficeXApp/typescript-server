import { FastifyPluginAsync } from "fastify";
import {
  getGiftcardSpawnOrgHandler,
  listGiftcardSpawnOrgsHandler,
  deleteGiftcardSpawnOrgHandler,
  redeemGiftcardSpawnOrgHandler,
  createGiftcardSpawnOrgHandler,
  updateGiftcardSpawnOrgHandler,
} from "./handlers";
import { factoryRateLimitPreHandler } from "../../../../services/rate-limit";
import {
  IRequestCreateGiftcardSpawnOrg,
  IRequestDeleteGiftcardSpawnOrg,
  IRequestListGiftcardSpawnOrgs,
  IRequestRedeemGiftcardSpawnOrg,
  IRequestUpdateGiftcardSpawnOrg,
  IRequestUpsertGiftcardSpawnOrg,
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
  fastify.post<{ Body: IRequestListGiftcardSpawnOrgs }>(
    "/list",
    { preHandler: [factoryRateLimitPreHandler] },
    listGiftcardSpawnOrgsHandler
  );

  // POST /v1/factory/giftcards/spawnorg/create
  fastify.post<{ Body: IRequestCreateGiftcardSpawnOrg }>(
    "/create",
    { preHandler: [factoryRateLimitPreHandler] },
    createGiftcardSpawnOrgHandler
  );

  // POST /v1/factory/giftcards/spawnorg/update
  fastify.post<{ Body: IRequestUpdateGiftcardSpawnOrg }>(
    "/update",
    { preHandler: [factoryRateLimitPreHandler] },
    updateGiftcardSpawnOrgHandler
  );

  // POST /v1/factory/giftcards/spawnorg/delete
  fastify.post<{ Body: IRequestDeleteGiftcardSpawnOrg }>(
    "/delete",
    { preHandler: [factoryRateLimitPreHandler] },
    deleteGiftcardSpawnOrgHandler
  );

  // POST /v1/factory/giftcards/spawnorg/redeem
  fastify.post<{ Body: IRequestRedeemGiftcardSpawnOrg }>(
    "/redeem",
    { preHandler: [factoryRateLimitPreHandler] },
    redeemGiftcardSpawnOrgHandler
  );
};

export default giftcardSpawnOrgRoutes;
