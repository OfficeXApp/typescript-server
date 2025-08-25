// src/routes/v1/drive/disks/index.ts

import { FastifyPluginAsync } from "fastify";
import {
  getDiskHandler,
  listDisksHandler,
  createDiskHandler,
  updateDiskHandler,
  deleteDiskHandler,
} from "./handlers";
import { driveRateLimitPreHandler } from "../../../../services/rate-limit"; // Import the preHandler
import {
  OrgIdParams, // Assuming org_id is part of the parent plugin's prefix
} from "../../types"; // Adjust path if needed for your types
import {
  DiskID, // Assuming you have a DiskID type
  IRequestCreateDisk, // Assuming these types exist for your handlers
  IRequestDeleteDisk,
  IRequestListDisks,
  IRequestUpdateDisk,
  IResponseCreateDisk,
  IResponseDeleteDisk,
  IResponseGetDisk,
  IResponseListDisks,
  IResponseUpdateDisk,
} from "@officexapp/types"; // Adjust this path if your types are elsewhere

// Define interfaces for params and body
interface GetDiskParams extends OrgIdParams {
  disk_id: DiskID;
}

const diskRoutes: FastifyPluginAsync = async (fastify, opts): Promise<void> => {
  // GET /v1/drive/disks/get/:disk_id
  fastify.get<{ Params: GetDiskParams; Reply: IResponseGetDisk }>(
    "/get/:disk_id",
    { preHandler: [driveRateLimitPreHandler] }, // Add the preHandler here
    getDiskHandler
  );

  // POST /v1/drive/disks/list
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestListDisks;
    Reply: IResponseListDisks;
  }>( // Assuming IRequestListDisks exists
    "/list",
    { preHandler: [driveRateLimitPreHandler] }, // Add the preHandler here
    listDisksHandler
  );

  // POST /v1/drive/disks/create
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestCreateDisk;
    Reply: IResponseCreateDisk;
  }>( // Assuming IRequestCreateDisk exists
    "/create",
    { preHandler: [driveRateLimitPreHandler] }, // Add the preHandler here
    createDiskHandler
  );

  // POST /v1/drive/disks/update
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestUpdateDisk;
    Reply: IResponseUpdateDisk;
  }>( // Assuming IRequestUpdateDisk exists
    "/update",
    { preHandler: [driveRateLimitPreHandler] }, // Add the preHandler here
    updateDiskHandler
  );

  // POST /v1/drive/disks/delete
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestDeleteDisk;
    Reply: IResponseDeleteDisk;
  }>( // Assuming IRequestDeleteDisk exists
    "/delete",
    { preHandler: [driveRateLimitPreHandler] }, // Add the preHandler here
    deleteDiskHandler
  );
};

export default diskRoutes;
