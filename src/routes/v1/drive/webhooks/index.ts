// typescript-server/src/routes/v1/drive/webhooks/index.ts
import { FastifyPluginAsync } from "fastify";
import {
  getWebhookHandler,
  listWebhooksHandler,
  createWebhookHandler,
  updateWebhookHandler,
  deleteWebhookHandler,
} from "./handlers";
import { driveRateLimitPreHandler } from "../../../../services/rate-limit";
import {
  WebhookID,
  IRequestGetWebhook, // Used for getWebhookHandler's params
  IRequestListWebhooks,
  IRequestCreateWebhook,
  IRequestUpdateWebhook,
  IRequestDeleteWebhook,
} from "@officexapp/types"; // Adjust this path if your types are elsewhere
import { OrgIdParams } from "../../types";

// Interface for get webhook params that might include org_id from prefix
interface GetWebhookParams extends OrgIdParams {
  webhook_id: WebhookID;
}

const webhookRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/drive/webhooks/get/:webhook_id
  // The route path is just "/get/:webhook_id", implying org_id comes from a parent plugin prefix.
  fastify.get<{ Params: GetWebhookParams; Body: IRequestGetWebhook }>(
    "/get/:webhook_id",
    { preHandler: [driveRateLimitPreHandler] },
    getWebhookHandler
  );

  // POST /v1/drive/webhooks/list
  fastify.post<{ Params: OrgIdParams; Body: IRequestListWebhooks }>(
    "/list",
    { preHandler: [driveRateLimitPreHandler] },
    listWebhooksHandler
  );

  // POST /v1/drive/webhooks/create
  fastify.post<{ Params: OrgIdParams; Body: IRequestCreateWebhook }>(
    "/create",
    { preHandler: [driveRateLimitPreHandler] },
    createWebhookHandler
  );

  // POST /v1/drive/webhooks/update
  fastify.post<{ Params: OrgIdParams; Body: IRequestUpdateWebhook }>(
    "/update",
    { preHandler: [driveRateLimitPreHandler] },
    updateWebhookHandler
  );

  // POST /v1/drive/webhooks/delete
  fastify.post<{ Params: OrgIdParams; Body: IRequestDeleteWebhook }>(
    "/delete",
    { preHandler: [driveRateLimitPreHandler] },
    deleteWebhookHandler
  );
};

export default webhookRoutes;
