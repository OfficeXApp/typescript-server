import { FastifyPluginAsync } from "fastify";
import {
  getLabelHandler,
  listLabelsHandler,
  createLabelHandler,
  updateLabelHandler,
  deleteLabelHandler,
  labelResourceHandler,
} from "./handlers";

const labelRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/drive/labels/get/:label_id
  fastify.get("/get/:label_id", getLabelHandler);

  // POST /v1/drive/labels/list
  fastify.post("/list", listLabelsHandler);

  // POST /v1/drive/labels/create
  fastify.post("/create", createLabelHandler);

  // POST /v1/drive/labels/update
  fastify.post("/update", updateLabelHandler);

  // POST /v1/drive/labels/delete
  fastify.post("/delete", deleteLabelHandler);

  // POST /v1/drive/labels/pin
  fastify.post("/pin", labelResourceHandler);
};

export default labelRoutes;
