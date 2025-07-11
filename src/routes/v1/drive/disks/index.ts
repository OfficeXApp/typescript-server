import { FastifyPluginAsync } from "fastify";
import {
  getDiskHandler,
  listDisksHandler,
  createDiskHandler,
  updateDiskHandler,
  deleteDiskHandler,
} from "./handlers";

const diskRoutes: FastifyPluginAsync = async (fastify, opts): Promise<void> => {
  // GET /v1/drive/disks/get/:disk_id
  fastify.get("/get/:disk_id", getDiskHandler);

  // POST /v1/drive/disks/list
  fastify.post("/list", listDisksHandler);

  // POST /v1/drive/disks/create
  fastify.post("/create", createDiskHandler);

  // POST /v1/drive/disks/update
  fastify.post("/update", updateDiskHandler);

  // POST /v1/drive/disks/delete
  fastify.post("/delete", deleteDiskHandler);
};

export default diskRoutes;
