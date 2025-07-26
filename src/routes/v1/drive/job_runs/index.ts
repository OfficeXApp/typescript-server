// src/routes/v1/drive/job-runs/index.ts

import { FastifyPluginAsync } from "fastify";
import {
  getJobRunHandler,
  listJobRunsHandler,
  createJobRunHandler,
  updateJobRunHandler,
  deleteJobRunHandler,
} from "./handlers";
import { driveRateLimitPreHandler } from "../../../../services/rate-limit";
import { OrgIdParams } from "../../types";
import {
  JobRunID,
  IRequestCreateJobRun,
  IRequestDeleteJobRun,
  IRequestGetJobRun,
  IRequestListJobRuns,
  IRequestUpdateJobRun,
} from "@officexapp/types";

// Define interfaces for params and body
export interface GetJobRunParams extends OrgIdParams {
  job_run_id: JobRunID;
}

const jobRunsRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/drive/job-runs/get/:job_run_id
  fastify.get<{ Params: GetJobRunParams }>(
    "/get/:job_run_id",
    { preHandler: [driveRateLimitPreHandler] },
    getJobRunHandler
  );

  // POST /v1/drive/job-runs/list
  fastify.post<{ Params: OrgIdParams; Body: IRequestListJobRuns }>(
    "/list",
    { preHandler: [driveRateLimitPreHandler] },
    listJobRunsHandler
  );

  // POST /v1/drive/job-runs/create
  fastify.post<{ Params: OrgIdParams; Body: IRequestCreateJobRun }>(
    "/create",
    { preHandler: [driveRateLimitPreHandler] },
    createJobRunHandler
  );

  // POST /v1/drive/job-runs/update
  fastify.post<{ Params: OrgIdParams; Body: IRequestUpdateJobRun }>(
    "/update",
    { preHandler: [driveRateLimitPreHandler] },
    updateJobRunHandler
  );

  // POST /v1/drive/job-runs/delete
  fastify.post<{ Params: OrgIdParams; Body: IRequestDeleteJobRun }>(
    "/delete",
    { preHandler: [driveRateLimitPreHandler] },
    deleteJobRunHandler
  );
};

export default jobRunsRoutes;
