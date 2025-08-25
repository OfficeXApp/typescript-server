// src/routes/v1/factory/api_keys/index.ts
import { FastifyPluginAsync } from "fastify";
import {
  getApiKeyHandler,
  listApiKeysHandler,
  createApiKeyHandler,
  updateApiKeyHandler,
  deleteApiKeyHandler,
} from "./handlers";
import { driveRateLimitPreHandler } from "../../../../services/rate-limit";
import {
  ApiKeyID,
  IRequestCreateApiKey,
  IRequestDeleteApiKey,
  IRequestListApiKeys,
  IRequestListContacts,
  IRequestUpdateApiKey,
  IResponseCreateApiKey,
  IResponseDeleteApiKey,
  IResponseGetApiKey,
  IResponseListApiKeys,
  IResponseUpdateApiKey,
  UserID,
} from "@officexapp/types";
import { OrgIdParams } from "../../types";

interface GetApiKeyParams {
  org_id: string; // Comes from the parent plugin's prefix
  api_key_id: ApiKeyID; // Specific parameter for this route
}

interface ListApiKeysParams {
  org_id: string; // Comes from the parent plugin's prefix
  user_id: UserID; // Specific parameter for this route, assuming it's optional as per your API structure.
}

const apiKeyRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/drive/api_keys/get/:api_key_id
  fastify.get<{ Params: GetApiKeyParams; Reply: IResponseGetApiKey }>(
    "/get/:api_key_id",
    { preHandler: [driveRateLimitPreHandler] },
    getApiKeyHandler
  );

  // POST /v1/drive/api_keys/list/:user_id
  fastify.post<{
    Params: ListApiKeysParams;
    Body: IRequestListApiKeys;
    Reply: IResponseListApiKeys;
  }>(
    "/list/:user_id",
    { preHandler: [driveRateLimitPreHandler] },
    listApiKeysHandler
  );

  // POST /v1/drive/api_keys/create
  fastify.post<{
    Body: IRequestCreateApiKey;
    Params: OrgIdParams;
    Reply: IResponseCreateApiKey;
  }>(
    "/create",
    { preHandler: [driveRateLimitPreHandler] },
    createApiKeyHandler
  );

  // POST /v1/drive/api_keys/update
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestUpdateApiKey;
    Reply: IResponseUpdateApiKey;
  }>(
    "/update",
    { preHandler: [driveRateLimitPreHandler] },
    updateApiKeyHandler
  );

  // POST /v1/drive/api_keys/delete
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestDeleteApiKey;
    Reply: IResponseDeleteApiKey;
  }>(
    "/delete",
    { preHandler: [driveRateLimitPreHandler] },
    deleteApiKeyHandler
  );
};

export default apiKeyRoutes;
