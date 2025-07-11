import { FastifyPluginAsync } from "fastify";
import {
  getDriveHandler,
  listDrivesHandler,
  createDriveHandler,
  updateDriveHandler,
  deleteDriveHandler,
} from "./handlers";

const driveRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/drive/drives/get/:drive_id
  fastify.get("/get/:drive_id", getDriveHandler);

  // POST /v1/drive/drives/list
  fastify.post("/list", listDrivesHandler);

  // POST /v1/drive/drives/create
  fastify.post("/create", createDriveHandler);

  // POST /v1/drive/drives/update
  fastify.post("/update", updateDriveHandler);

  // POST /v1/drive/drives/delete
  fastify.post("/delete", deleteDriveHandler);
};

export default driveRoutes;
