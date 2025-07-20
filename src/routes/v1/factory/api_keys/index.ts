// src/routes/v1/factory/api_keys/index.ts
import { FastifyPluginAsync } from "fastify";
import {
  getApiKeyHandler,
  listApiKeysHandler,
  upsertApiKeyHandler,
  deleteApiKeyHandler,
  GetApiKeyParams,
  ListApiKeysParams,
} from "./handlers";
import { factoryRateLimitPreHandler } from "../../../../services/rate-limit";
import {
  FactoryDeleteApiKeyRequestBody,
  FactoryUpsertApiKeyRequestBody,
} from "@officexapp/types";

const apiKeyRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/factory/api_keys/get/:api_key_id
  fastify.get<{ Params: GetApiKeyParams }>(
    "/get/:api_key_id",
    { preHandler: [factoryRateLimitPreHandler] },
    getApiKeyHandler
  );

  // POST /v1/factory/api_keys/list/:user_id
  fastify.post<{ Params: ListApiKeysParams }>(
    "/list/:user_id",
    { preHandler: [factoryRateLimitPreHandler] },
    listApiKeysHandler
  );

  // POST /v1/factory/api_keys/upsert
  fastify.post<{ Body: FactoryUpsertApiKeyRequestBody }>(
    "/upsert",
    { preHandler: [factoryRateLimitPreHandler] },
    upsertApiKeyHandler
  );

  // POST /v1/factory/api_keys/delete
  fastify.post<{ Body: FactoryDeleteApiKeyRequestBody }>(
    "/delete",
    { preHandler: [factoryRateLimitPreHandler] },
    deleteApiKeyHandler
  );
};

export default apiKeyRoutes;
