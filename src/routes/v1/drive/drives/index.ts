// src/routes/v1/drive/drives/index.ts

import { FastifyPluginAsync } from "fastify";
import {
  getDriveHandler,
  listDrivesHandler,
  createDriveHandler,
  updateDriveHandler,
  deleteDriveHandler,
} from "./handlers";
import { driveRateLimitPreHandler } from "../../../../services/rate-limit"; // Import the preHandler
import { OrgIdParams } from "../../types";
import {
  DriveID, // Assuming you have a DriveID type
  IRequestCreateDrive,
  IRequestDeleteDrive,
  IRequestListDrives,
  IRequestUpdateDrive,
  IResponseCreateDrive,
  IResponseDeleteDrive,
  IResponseGetDrive,
  IResponseListDrives,
  IResponseUpdateDrive,
} from "@officexapp/types"; // Adjust this path if your types are elsewhere

interface GetDriveParams extends OrgIdParams {
  drive_id: DriveID;
}

const driveRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/drive/drives/get/:drive_id
  fastify.get<{ Params: GetDriveParams; Reply: IResponseGetDrive }>(
    "/get/:drive_id",
    { preHandler: [driveRateLimitPreHandler] },
    getDriveHandler
  );

  // POST /v1/drive/drives/list
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestListDrives;
    Reply: IResponseListDrives;
  }>("/list", { preHandler: [driveRateLimitPreHandler] }, listDrivesHandler);

  // POST /v1/drive/drives/create
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestCreateDrive;
    Reply: IResponseCreateDrive;
  }>("/create", { preHandler: [driveRateLimitPreHandler] }, createDriveHandler);

  // POST /v1/drive/drives/update
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestUpdateDrive;
    Reply: IResponseUpdateDrive;
  }>("/update", { preHandler: [driveRateLimitPreHandler] }, updateDriveHandler);

  // POST /v1/drive/drives/delete
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestDeleteDrive;
    Reply: IResponseDeleteDrive;
  }>("/delete", { preHandler: [driveRateLimitPreHandler] }, deleteDriveHandler);
};

export default driveRoutes;
