// src/routes/v1/factory/api_keys/index.ts
import { FastifyPluginAsync } from "fastify";
import {
  getApiKeyHandler,
  listApiKeysHandler,
  deleteApiKeyHandler,
  GetApiKeyParams,
  ListApiKeysParams,
  createApiKeyHandler,
  updateApiKeyHandler,
} from "./handlers";
import { factoryRateLimitPreHandler } from "../../../../services/rate-limit";
import {
  IRequestFactoryCreateApiKey,
  IRequestFactoryDeleteApiKey,
  IRequestFactoryUpdateApiKey,
  IResponseCreateApiKey,
  IResponseDeleteApiKey,
  IResponseGetApiKey,
  IResponseListApiKeys,
  IResponseUpdateApiKey,
} from "@officexapp/types";

const apiKeyRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/factory/api_keys/get/:api_key_id
  fastify.get<{ Params: GetApiKeyParams; Reply: IResponseGetApiKey }>(
    "/get/:api_key_id",
    { preHandler: [factoryRateLimitPreHandler] },
    getApiKeyHandler
  );

  // POST /v1/factory/api_keys/list/:user_id
  fastify.post<{ Params: ListApiKeysParams; Reply: IResponseListApiKeys }>(
    "/list/:user_id",
    { preHandler: [factoryRateLimitPreHandler] },
    listApiKeysHandler
  );

  // POST /v1/factory/api_keys/create
  fastify.post<{
    Body: IRequestFactoryCreateApiKey;
    Reply: IResponseCreateApiKey;
  }>(
    "/create",
    { preHandler: [factoryRateLimitPreHandler] },
    createApiKeyHandler
  );

  // POST /v1/factory/api_keys/update
  fastify.post<{
    Body: IRequestFactoryUpdateApiKey;
    Reply: IResponseUpdateApiKey;
  }>(
    "/update",
    { preHandler: [factoryRateLimitPreHandler] },
    updateApiKeyHandler
  );

  // POST /v1/factory/api_keys/delete
  fastify.post<{
    Body: IRequestFactoryDeleteApiKey;
    Reply: IResponseDeleteApiKey;
  }>(
    "/delete",
    { preHandler: [factoryRateLimitPreHandler] },
    deleteApiKeyHandler
  );
};

export default apiKeyRoutes;
