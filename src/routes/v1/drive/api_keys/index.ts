// src/routes/v1/factory/api_keys/index.ts
import { FastifyPluginAsync } from "fastify";
import {
  getApiKeyHandler,
  listApiKeysHandler,
  createApiKeyHandler,
  updateApiKeyHandler,
  deleteApiKeyHandler,
  snapshotHandler,
} from "./handlers";

const apiKeyRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/factory/api_keys/get/:api_key_id
  fastify.get("/get/:api_key_id", getApiKeyHandler);

  // POST /v1/factory/api_keys/list/:user_id
  fastify.post("/list/:user_id", listApiKeysHandler);

  // POST /v1/factory/api_keys/create
  fastify.post("/create", createApiKeyHandler);

  // POST /v1/factory/api_keys/update
  fastify.post("/update", updateApiKeyHandler);

  // POST /v1/factory/api_keys/delete
  fastify.post("/delete", deleteApiKeyHandler);

  // GET /v1/factory/snapshot
  // Note: This route is at a different path level, so we register it separately
  fastify.register(
    async (fastify, opts) => {
      fastify.get("/snapshot", snapshotHandler);
    },
    { prefix: "/../.." }
  ); // Go up to /v1/factory level
};

export default apiKeyRoutes;
