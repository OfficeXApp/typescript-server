// src/routes/v1/drive/labels/index.ts

import { FastifyPluginAsync } from "fastify";
import {
  getLabelHandler,
  listLabelsHandler,
  createLabelHandler,
  updateLabelHandler,
  deleteLabelHandler,
  labelResourceHandler,
} from "./handlers";
import { driveRateLimitPreHandler } from "../../../../services/rate-limit"; // Import the preHandler
import { OrgIdParams } from "../../types";
import {
  LabelID, // Assuming you have a LabelID type
  IRequestCreateLabel,
  IRequestDeleteLabel,
  IRequestListLabels,
  IRequestUpdateLabel,
  IRequestLabelResource,
} from "@officexapp/types"; // Adjust this path if your types are elsewhere

interface GetLabelParams extends OrgIdParams {
  label_id: LabelID;
}

const labelRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/drive/labels/get/:label_id
  fastify.get<{ Params: GetLabelParams }>(
    "/get/:label_id",
    { preHandler: [driveRateLimitPreHandler] },
    getLabelHandler
  );

  // POST /v1/drive/labels/list
  fastify.post<{ Params: OrgIdParams; Body: IRequestListLabels }>(
    "/list",
    { preHandler: [driveRateLimitPreHandler] },
    listLabelsHandler
  );

  // POST /v1/drive/labels/create
  fastify.post<{ Params: OrgIdParams; Body: IRequestCreateLabel }>(
    "/create",
    { preHandler: [driveRateLimitPreHandler] },
    createLabelHandler
  );

  // POST /v1/drive/labels/update
  fastify.post<{ Params: OrgIdParams; Body: IRequestUpdateLabel }>(
    "/update",
    { preHandler: [driveRateLimitPreHandler] },
    updateLabelHandler
  );

  // POST /v1/drive/labels/delete
  fastify.post<{ Params: OrgIdParams; Body: IRequestDeleteLabel }>(
    "/delete",
    { preHandler: [driveRateLimitPreHandler] },
    deleteLabelHandler
  );

  // POST /v1/drive/labels/pin
  fastify.post<{ Params: OrgIdParams; Body: IRequestLabelResource }>( // Assuming 'pin' corresponds to labelResourceHandler and takes IRequestLabelResource
    "/pin",
    { preHandler: [driveRateLimitPreHandler] },
    labelResourceHandler
  );
};

export default labelRoutes;
