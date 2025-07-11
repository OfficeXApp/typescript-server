// typescript-server/src/routes/v1/drive/webhooks/index.ts
import { FastifyPluginAsync } from "fastify";
import {
  getWebhookHandler,
  listWebhooksHandler,
  createWebhookHandler,
  updateWebhookHandler,
  deleteWebhookHandler,
} from "./handlers";

const webhookRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/drive/webhooks/get/:webhook_id
  fastify.get("/get/:webhook_id", getWebhookHandler);

  // POST /v1/drive/webhooks/list
  fastify.post("/list", listWebhooksHandler);

  // POST /v1/drive/webhooks/create
  fastify.post("/create", createWebhookHandler);

  // POST /v1/drive/webhooks/update
  fastify.post("/update", updateWebhookHandler);

  // POST /v1/drive/webhooks/delete
  fastify.post("/delete", deleteWebhookHandler);
};

export default webhookRoutes;
