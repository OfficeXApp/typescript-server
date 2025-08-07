// src/routes/v1/drive/purchases/index.ts

import { FastifyPluginAsync } from "fastify";
import {
  getPurchaseHandler,
  listPurchasesHandler,
  createPurchaseHandler,
  updatePurchaseHandler,
  deletePurchaseHandler,
} from "./handlers";
import { driveRateLimitPreHandler } from "../../../../services/rate-limit";
import { OrgIdParams } from "../../types";
import {
  PurchaseID,
  IRequestCreatePurchase,
  IRequestDeletePurchase,
  IRequestGetPurchase,
  IRequestListPurchases,
  IRequestUpdatePurchase,
} from "@officexapp/types";

// Define interfaces for params and body
export interface GetPurchaseParams extends OrgIdParams {
  purchase_id: PurchaseID;
}

const purchasesRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/drive/purchases/get/:purchase_id
  fastify.get<{ Params: GetPurchaseParams }>(
    "/get/:purchase_id",
    { preHandler: [driveRateLimitPreHandler] },
    getPurchaseHandler
  );

  // POST /v1/drive/purchases/list
  fastify.post<{ Params: OrgIdParams; Body: IRequestListPurchases }>(
    "/list",
    { preHandler: [driveRateLimitPreHandler] },
    listPurchasesHandler
  );

  // POST /v1/drive/purchases/create
  fastify.post<{ Params: OrgIdParams; Body: IRequestCreatePurchase }>(
    "/create",
    { preHandler: [driveRateLimitPreHandler] },
    createPurchaseHandler
  );

  // POST /v1/drive/purchases/update
  fastify.post<{ Params: OrgIdParams; Body: IRequestUpdatePurchase }>(
    "/update",
    { preHandler: [driveRateLimitPreHandler] },
    updatePurchaseHandler
  );

  // POST /v1/drive/purchases/delete
  fastify.post<{ Params: OrgIdParams; Body: IRequestDeletePurchase }>(
    "/delete",
    { preHandler: [driveRateLimitPreHandler] },
    deletePurchaseHandler
  );
};

export default purchasesRoutes;
