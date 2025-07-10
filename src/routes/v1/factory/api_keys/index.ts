// src/routes/v1/factory/api_keys/index.ts
import { FastifyPluginAsync } from "fastify";
import {
  getApiKeyHandler,
  listApiKeysHandler,
  upsertApiKeyHandler,
  deleteApiKeyHandler,
} from "./handlers";

const apiKeyRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/factory/api_keys/get/:api_key_id
  fastify.get("/get/:api_key_id", getApiKeyHandler);

  // POST /v1/factory/api_keys/list/:user_id
  fastify.post("/list/:user_id", listApiKeysHandler);

  // POST /v1/factory/api_keys/upsert
  fastify.post("/upsert", upsertApiKeyHandler);

  // POST /v1/factory/api_keys/delete
  fastify.post("/delete", deleteApiKeyHandler);
};

export default apiKeyRoutes;
