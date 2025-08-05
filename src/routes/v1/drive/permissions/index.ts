// /src/routes/v1/drive/permissions/index.ts
import { FastifyPluginAsync } from "fastify";
import {
  // getDirectoryPermissionById, // Unused as per provided routes
  // listDirectoryPermissionsForResource, // Unused as per provided routes
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
import { driveRateLimitPreHandler } from "../../../../services/rate-limit";
import {
  DirectoryPermissionID,
  SystemPermissionID,
  IRequestGetDirectoryPermission, // Used for 'get' route's params
  IRequestListDirectoryPermissions,
  IRequestCreateDirectoryPermission,
  IRequestUpdateDirectoryPermission,
  IRequestDeleteDirectoryPermission,
  IRequestCheckDirectoryPermissions,
  IRequestRedeemDirectoryPermission,
  IRequestGetSystemPermission, // Used for 'get' route's params
  IRequestListSystemPermissions,
  IRequestCreateSystemPermission,
  IRequestUpdateSystemPermission,
  IRequestDeleteSystemPermission,
  IRequestCheckSystemPermissions,
  IRequestRedeemSystemPermission,
} from "@officexapp/types"; // Adjust this path if your types are elsewhere
import { OrgIdParams } from "../../types";

// Interfaces for specific route parameters that include org_id explicitly in the path
interface GetDirectoryPermissionRouteParams {
  org_id: string; // From route path, not parent prefix
  directory_permission_id: DirectoryPermissionID;
}

interface GetSystemPermissionRouteParams {
  org_id: string; // From route path, not parent prefix
  system_permission_id: SystemPermissionID;
}

const permissionRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // Directory Permissions
  // GET /v1/drive/permissions/directory/get/:directory_permission_id
  fastify.get<{ Params: GetDirectoryPermissionRouteParams }>(
    "/directory/get/:directory_permission_id",
    { preHandler: [driveRateLimitPreHandler] },
    getDirectoryPermissionsHandler
  );

  // POST /v1/drive/permissions/directory/list
  // Note: Your route definition is "/directory/list", but comment shows /:org_id.
  // Assuming org_id comes from a parent prefix if not explicitly in route string.
  // If :org_id is *meant* to be a path param here, the route string needs adjustment.
  fastify.post<{ Params: OrgIdParams; Body: IRequestListDirectoryPermissions }>(
    "/directory/list",
    { preHandler: [driveRateLimitPreHandler] },
    listDirectoryPermissionsHandler
  );

  // POST /v1/drive/permissions/directory/create
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestCreateDirectoryPermission;
  }>(
    "/directory/create",
    { preHandler: [driveRateLimitPreHandler] },
    createDirectoryPermissionsHandler
  );

  // POST /v1/drive/permissions/directory/update
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestUpdateDirectoryPermission;
  }>(
    "/directory/update",
    { preHandler: [driveRateLimitPreHandler] },
    updateDirectoryPermissionsHandler
  );

  // POST /v1/drive/permissions/directory/delete
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestDeleteDirectoryPermission;
  }>(
    "/directory/delete",
    { preHandler: [driveRateLimitPreHandler] },
    deleteDirectoryPermissionsHandler
  );

  // POST /v1/drive/permissions/directory/check
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestCheckDirectoryPermissions;
  }>(
    "/directory/check",
    { preHandler: [driveRateLimitPreHandler] },
    checkDirectoryPermissionsHandler
  );

  // POST /v1/drive/permissions/directory/redeem
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestRedeemDirectoryPermission;
  }>(
    "/directory/redeem",
    { preHandler: [driveRateLimitPreHandler] },
    redeemDirectoryPermissionsHandler
  );

  // System Permissions
  // GET /v1/drive/permissions/system/get/:system_permission_id
  fastify.get<{ Params: GetSystemPermissionRouteParams }>(
    "/system/get/:system_permission_id",
    { preHandler: [driveRateLimitPreHandler] },
    getSystemPermissionsHandler
  );

  // POST /v1/drive/permissions/system/list
  fastify.post<{ Params: OrgIdParams; Body: IRequestListSystemPermissions }>(
    "/system/list",
    { preHandler: [driveRateLimitPreHandler] },
    listSystemPermissionsHandler
  );

  // POST /v1/drive/permissions/system/create
  fastify.post<{ Params: OrgIdParams; Body: IRequestCreateSystemPermission }>(
    "/system/create",
    { preHandler: [driveRateLimitPreHandler] },
    createSystemPermissionsHandler
  );

  // POST /v1/drive/permissions/system/update
  fastify.post<{ Params: OrgIdParams; Body: IRequestUpdateSystemPermission }>(
    "/system/update",
    { preHandler: [driveRateLimitPreHandler] },
    updateSystemPermissionsHandler
  );

  // POST /v1/drive/permissions/system/delete
  fastify.post<{ Params: OrgIdParams; Body: IRequestDeleteSystemPermission }>(
    "/system/delete",
    { preHandler: [driveRateLimitPreHandler] },
    deleteSystemPermissionsHandler
  );

  // POST /v1/drive/permissions/system/check
  fastify.post<{ Params: OrgIdParams; Body: IRequestCheckSystemPermissions }>(
    "/system/check",
    { preHandler: [driveRateLimitPreHandler] },
    checkSystemPermissionsHandler
  );

  // POST /v1/drive/permissions/system/redeem
  fastify.post<{ Params: OrgIdParams; Body: IRequestRedeemSystemPermission }>(
    "/system/redeem",
    { preHandler: [driveRateLimitPreHandler] },
    redeemSystemPermissionsHandler
  );
};

export default permissionRoutes;
