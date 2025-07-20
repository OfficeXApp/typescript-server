// src/routes/v1/drive/directory/index.ts

import { FastifyPluginAsync } from "fastify";
import {
  listDirectoryHandler,
  directoryActionHandler,
  handleUploadChunk,
  handleCompleteUpload,
  downloadFileMetadataHandler,
  downloadFileChunkHandler,
  getRawUrlProxyHandler,
} from "./handlers";
import { driveRateLimitPreHandler } from "../../../../services/rate-limit"; // Import the preHandler
import { OrgIdParams } from "../../types"; // Adjust path if needed for your types
import {
  IRequestListDirectory, // Assuming these types exist for your handlers
  IRequestDirectoryAction,
} from "@officexapp/types"; // Adjust this path if your types are elsewhere

// Define interfaces for params and body if they are not already defined in @officexapp/types
// (These are examples, replace with your actual types if different)

// Assuming listDirectoryHandler, directoryActionHandler, etc., will receive OrgIdParams
// in addition to their specific body types.

// For getRawUrlProxyHandler
interface GetRawUrlProxyParams extends OrgIdParams {
  file_id_with_extension: string; // As per your route path
}

const DIRECTORY_LIST_PATH = "/list";
const DIRECTORY_ACTION_PATH = "/action";
const UPLOAD_CHUNK_PATH = "/raw_upload/chunk";
const COMPLETE_UPLOAD_PATH = "/raw_upload/complete";
const RAW_DOWNLOAD_META_PATH = "/raw_download/meta";
const RAW_DOWNLOAD_CHUNK_PATH = "/raw_download/chunk";
const RAW_URL_PROXY_PATH = "/asset/:file_id_with_extension";

const directoryRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // POST /v1/drive/:org_id/directory/list
  fastify.post<{ Params: OrgIdParams; Body: IRequestListDirectory }>(
    DIRECTORY_LIST_PATH,
    { preHandler: [driveRateLimitPreHandler] },
    listDirectoryHandler
  );

  // POST /v1/drive/:org_id/directory/action
  fastify.post<{ Params: OrgIdParams; Body: IRequestDirectoryAction }>(
    DIRECTORY_ACTION_PATH,
    { preHandler: [driveRateLimitPreHandler] },
    directoryActionHandler
  );

  // POST /v1/drive/:org_id/directory/raw_upload/chunk
  fastify.post(
    UPLOAD_CHUNK_PATH,

    handleUploadChunk
  );

  // POST /v1/drive/:org_id/directory/raw_upload/complete
  fastify.post(COMPLETE_UPLOAD_PATH, handleCompleteUpload);

  // GET /v1/drive/:org_id/directory/raw_download/meta
  fastify.get(RAW_DOWNLOAD_META_PATH, downloadFileMetadataHandler);

  // GET /v1/drive/:org_id/directory/raw_download/chunk
  fastify.get(RAW_DOWNLOAD_CHUNK_PATH, downloadFileChunkHandler);

  // GET /v1/drive/:org_id/directory/asset/:file_id_with_extension
  fastify.get(RAW_URL_PROXY_PATH, getRawUrlProxyHandler);
};

export default directoryRoutes;
