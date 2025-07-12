// /src/routes/v1/drive/permissions/index.ts
import { FastifyPluginAsync } from "fastify";
import {
  getDirectoryPermissionById,
  listDirectoryPermissionsForResource,
  createDirectoryPermissionsHandler,
  updateDirectoryPermissionsHandler,
  deleteDirectoryPermissionsHandler,
  checkDirectoryPermissionsHandler,
  redeemDirectoryPermissionsHandler,
  getSystemPermissionsHandler,
  listSystemPermissionsHandler,
  createSystemPermissionsHandler,
  updateSystemPermissionsHandler,
  deleteSystemPermissionsHandler,
  checkSystemPermissionsHandler,
  redeemSystemPermissionsHandler,
  listDirectoryPermissionsHandler,
  getDirectoryPermissionsHandler,
} from "./handlers";

const permissionRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // Directory Permissions
  // GET /v1/drive/permissions/directory/get/:org_id/:directory_permission_id
  fastify.get(
    "/directory/get/:org_id/:directory_permission_id",
    getDirectoryPermissionsHandler
  );

  // POST /v1/drive/permissions/directory/list/:org_id
  fastify.post("/directory/list/:org_id", listDirectoryPermissionsHandler);

  // POST /v1/drive/permissions/directory/create/:org_id
  fastify.post("/directory/create/:org_id", createDirectoryPermissionsHandler);

  // POST /v1/drive/permissions/directory/update/:org_id
  fastify.post("/directory/update/:org_id", updateDirectoryPermissionsHandler);

  // POST /v1/drive/permissions/directory/delete/:org_id
  fastify.post("/directory/delete/:org_id", deleteDirectoryPermissionsHandler);

  // POST /v1/drive/permissions/directory/check/:org_id
  fastify.post("/directory/check/:org_id", checkDirectoryPermissionsHandler);

  // POST /v1/drive/permissions/directory/redeem/:org_id
  fastify.post("/directory/redeem/:org_id", redeemDirectoryPermissionsHandler);

  // System Permissions
  // GET /v1/drive/permissions/system/get/:org_id/:system_permission_id
  fastify.get(
    "/system/get/:org_id/:system_permission_id",
    getSystemPermissionsHandler
  );

  // POST /v1/drive/permissions/system/list/:org_id
  fastify.post("/system/list/:org_id", listSystemPermissionsHandler);

  // POST /v1/drive/permissions/system/create/:org_id
  fastify.post("/system/create/:org_id", createSystemPermissionsHandler);

  // POST /v1/drive/permissions/system/update/:org_id
  fastify.post("/system/update/:org_id", updateSystemPermissionsHandler);

  // POST /v1/drive/permissions/system/delete/:org_id
  fastify.post("/system/delete/:org_id", deleteSystemPermissionsHandler);

  // POST /v1/drive/permissions/system/check/:org_id
  fastify.post("/system/check/:org_id", checkSystemPermissionsHandler);

  // POST /v1/drive/permissions/system/redeem/:org_id
  fastify.post("/system/redeem/:org_id", redeemSystemPermissionsHandler);
};

export default permissionRoutes;
