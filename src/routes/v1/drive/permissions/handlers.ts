import { FastifyRequest, FastifyReply } from "fastify";
import {
  DirectoryPermissionFE,
  IRequestListDirectoryPermissions,
  IPaginatedResponse,
  SortDirection,
  IRequestRedeemDirectoryPermission,
  IRequestRedeemSystemPermission,
  SystemPermissionFE,
  IRequestCreateDirectoryPermission,
  IDPrefixEnum,
  IRequestUpdateDirectoryPermission,
  DirectoryPermission,
  IRequestDeleteDirectoryPermission,
  IResponseDeleteDirectoryPermission,
  IRequestCheckDirectoryPermissions,
  IResponseCheckDirectoryPermissions,
  IResponseRedeemDirectoryPermission,
  IRequestListSystemPermissions,
  IRequestUpdateSystemPermission,
  IResponseDeleteSystemPermission,
  IRequestCheckSystemPermissions,
  IResponseRedeemSystemPermission,
  IResponseCheckSystemPermissions,
  DirectoryPermissionType,
  IRequestDeleteSystemPermission,
  IRequestCreateSystemPermission,
} from "@officexapp/types";
import { authenticateRequest } from "../../../../services/auth";

import {
  getDirectoryPermissionById,
  listDirectoryPermissionsForResource,
} from "../../../../services/permissions/directory";
import { createApiResponse, getDriveOwnerId, OrgIdParams } from "../../types";
import {
  checkPermissionsTableAccess,
  checkSystemPermissions,
} from "../../../../services/permissions/system";

// Define route-specific param types
interface GetDirectoryPermissionParams extends OrgIdParams {
  directory_permission_id: string;
}

/**
 * Handles the request to get a single directory permission by its ID.
 * Corresponds to Rust's `get_directory_permissions_handler`.
 */
export async function getDirectoryPermissionsHandler(
  request: FastifyRequest<{ Params: GetDirectoryPermissionParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id, directory_permission_id } = request.params;

    // 1. Authenticate the request and get the requester's identity
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    // 2. Fetch the permission and check access rights.
    // The 'getDirectoryPermissionById' service function is expected to handle
    // fetching from the database and checking if the requester has permission to view the record.
    const permission = await getDirectoryPermissionById(
      org_id,
      directory_permission_id,
      requesterApiKey.user_id
    );

    // 3. Handle response
    if (!permission) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Permission not found or access denied",
        })
      );
    }

    return reply.status(200).send(createApiResponse(permission));
  } catch (error) {
    request.log.error("Error in getDirectoryPermissionsHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal Server Error",
      })
    );
  }
}

/**
 * Handles the request to list directory permissions for a specific resource.
 * Corresponds to Rust's `list_directory_permissions_handler`.
 */
export async function listDirectoryPermissionsHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestListDirectoryPermissions;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const {
      filters,
      page_size = 50,
      direction = SortDirection.DESC,
      cursor,
    } = request.body;

    // 1. Authenticate the request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    // 2. Validate required filters
    if (!filters?.resource_id) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Resource ID filter is required.",
        })
      );
    }

    // 3. Call the service to list permissions.
    // This service function is responsible for checking if the requester has 'VIEW' rights on the resource,
    // and then fetching the paginated list of permissions accordingly.
    const result = await listDirectoryPermissionsForResource({
      orgId: org_id,
      resourceId: filters.resource_id,
      requesterId: requesterApiKey.user_id,
      pageSize: page_size,
      direction,
      cursor,
    });

    // If the service indicates the user is not authorized to view permissions for this resource
    if (!result.authorized) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message:
            "Forbidden: You don't have permission to view this resource's permissions.",
        })
      );
    }

    // 4. Format and send the successful response
    const responseData: IPaginatedResponse<DirectoryPermissionFE> = {
      items: result.items,
      page_size: result.items.length,
      total: result.total,
      cursor: result.newCursor,
    };

    return reply.status(200).send(createApiResponse(responseData));
  } catch (error) {
    request.log.error("Error in listDirectoryPermissionsHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal Server Error",
      })
    );
  }
}

/**
 * Handles creating a new directory permission.
 * Corresponds to Rust's `create_directory_permissions_handler`.
 */
export async function createDirectoryPermissionsHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestCreateDirectoryPermission;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const body = request.body;

    // 1. Authenticate
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }
    const requesterId = requesterApiKey.user_id;

    // 2. Validate resource existence
    const resourceExists = body.resource_id.startsWith(IDPrefixEnum.File)
      ? !!(await getFileMetadata(org_id, body.resource_id))
      : !!(await getFolderMetadata(org_id, body.resource_id));
    if (!resourceExists) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Resource not found",
        })
      );
    }

    // 3. Authorize the action
    const ownerId = await getDriveOwnerId(org_id);
    const isOwner = requesterId === ownerId;
    const requesterPermissions = await checkDirectoryPermissions(
      org_id,
      body.resource_id,
      requesterId
    );

    const canManage = requesterPermissions.includes(
      DirectoryPermissionType.MANAGE
    );
    const canInvite = requesterPermissions.includes(
      DirectoryPermissionType.INVITE
    );

    if (!isOwner && !canManage && !canInvite) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Not authorized to create permissions for this resource",
        })
      );
    }

    // Ensure requester doesn't grant permissions they don't have
    if (!isOwner && !canManage) {
      const hasAllRequestedPerms = body.permission_types.every((p) =>
        requesterPermissions.includes(p)
      );
      if (!hasAllRequestedPerms) {
        return reply.status(403).send(
          createApiResponse(undefined, {
            code: 403,
            message: "Cannot grant permissions you do not possess",
          })
        );
      }
    }

    // 4. Create the permission
    const newPermission = await createDirectoryPermission(
      org_id,
      body,
      requesterId
    );

    // 5. Respond
    return reply.status(201).send(createApiResponse(newPermission));
  } catch (error) {
    request.log.error("Error in createDirectoryPermissionsHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal Server Error",
      })
    );
  }
}

/**
 * Handles updating an existing directory permission.
 * Corresponds to Rust's `update_directory_permissions_handler`.
 */
export async function updateDirectoryPermissionsHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestUpdateDirectoryPermission;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const body = request.body;

    // 1. Authenticate
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }
    const requesterId = requesterApiKey.user_id;

    // 2. Fetch existing permission
    const existingPermission = await getDirectoryPermissionById(
      org_id,
      body.id,
      requesterId,
      true
    ); // Bypass normal access check to get the raw record
    if (!existingPermission) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Permission not found",
        })
      );
    }

    // 3. Authorize the update action
    const ownerId = await getDriveOwnerId(org_id);
    const isOwner = requesterId === ownerId;
    const isGranter =
      (existingPermission as unknown as DirectoryPermission).granted_by ===
      requesterId; // Cast to access internal property
    const canManage = await hasDirectoryManagePermission(
      org_id,
      existingPermission.resource_id,
      requesterId
    );

    if (!isOwner && !isGranter && !canManage) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Not authorized to update this permission",
        })
      );
    }

    // 4. Update the permission
    const updatedPermission = await updateDirectoryPermission(
      org_id,
      body,
      requesterId
    );

    // 5. Respond
    if (!updatedPermission) {
      // This might happen if the permission was deleted between the fetch and update, or another validation failed.
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Permission not found or update failed",
        })
      );
    }

    return reply.status(200).send(createApiResponse(updatedPermission));
  } catch (error) {
    request.log.error("Error in updateDirectoryPermissionsHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal Server Error",
      })
    );
  }
}

/**
 * Handles deleting a directory permission.
 * Corresponds to Rust's `delete_directory_permissions_handler`.
 */
export async function deleteDirectoryPermissionsHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestDeleteDirectoryPermission;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const { permission_id } = request.body;

    // 1. Authenticate
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }
    const requesterId = requesterApiKey.user_id;

    // 2. Fetch the permission to be deleted
    const permission = await getDirectoryPermissionById(
      org_id,
      permission_id,
      requesterId,
      true
    );
    if (!permission) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Permission not found",
        })
      );
    }

    // 3. Authorize deletion
    const ownerId = await getDriveOwnerId(org_id);
    const isOwner = requesterId === ownerId;
    const isGranter =
      (permission as unknown as DirectoryPermission).granted_by === requesterId;
    const canManage = await hasDirectoryManagePermission(
      org_id,
      permission.resource_id,
      requesterId
    );

    if (!isOwner && !isGranter && !canManage) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Not authorized to delete this permission",
        })
      );
    }

    // 4. Perform deletion
    const deletedId = await deleteDirectoryPermission(org_id, permission_id);

    // 5. Respond
    const response: IResponseDeleteDirectoryPermission = {
      ok: {
        data: {
          deleted_id: deletedId,
        },
      },
    };
    return reply.status(200).send(response);
  } catch (error) {
    request.log.error("Error in deleteDirectoryPermissionsHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal Server Error",
      })
    );
  }
}

/**
 * Handles checking permissions for a given resource and grantee.
 * Corresponds to Rust's `check_directory_permissions_handler`.
 */
export async function checkDirectoryPermissionsHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestCheckDirectoryPermissions;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const { resource_id, grantee_id } = request.body;

    // 1. Authenticate
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }
    const requesterId = requesterApiKey.user_id;

    // 2. Authorize the check
    const ownerId = await getDriveOwnerId(org_id);
    const isOwner = requesterId === ownerId;
    const isCheckingOwnPermissions = grantee_id === requesterId;
    const canManageResource = await hasDirectoryManagePermission(
      org_id,
      resource_id,
      requesterId
    );

    let isGroupAdminOfGrantee = false;
    if (grantee_id.startsWith(IDPrefixEnum.Group)) {
      isGroupAdminOfGrantee = await isGroupAdmin(
        org_id,
        requesterId,
        grantee_id
      );
    }

    if (
      !isOwner &&
      !isCheckingOwnPermissions &&
      !canManageResource &&
      !isGroupAdminOfGrantee
    ) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Not authorized to check permissions for this grantee",
        })
      );
    }

    // 3. Perform the permission check
    const permissions = await checkDirectoryPermissions(
      org_id,
      resource_id,
      grantee_id
    );

    // 4. Respond
    const response: IResponseCheckDirectoryPermissions = {
      ok: {
        data: {
          resource_id: resource_id,
          grantee_id: grantee_id,
          permissions: permissions,
        },
      },
    };
    return reply.status(200).send(response);
  } catch (error) {
    request.log.error("Error in checkDirectoryPermissionsHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal Server Error",
      })
    );
  }
}

/**
 * Handles redeeming a placeholder (magic link) permission.
 * Corresponds to Rust's `redeem_directory_permissions_handler`.
 */
export async function redeemDirectoryPermissionsHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestRedeemDirectoryPermission;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const { permission_id, user_id, redeem_code, note } = request.body;

    // 1. Authenticate (the user trying to redeem)
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey || requesterApiKey.user_id !== user_id) {
      return reply.status(401).send(
        createApiResponse(undefined, {
          code: 401,
          message: "Unauthorized or user mismatch",
        })
      );
    }

    // 2. Call the service to redeem the permission
    const redeemedPermission = await redeemDirectoryPermission(org_id, {
      permission_id,
      user_id,
      redeem_code,
      note,
      requesterId: requesterApiKey.user_id,
    });

    // 3. Respond based on the service result
    if (redeemedPermission.error) {
      let statusCode = 400;
      if (redeemedPermission.error === "Permission not found") statusCode = 404;
      if (redeemedPermission.error === "Invalid redeem code") statusCode = 403;
      return reply.status(statusCode).send(
        createApiResponse(undefined, {
          code: statusCode,
          message: redeemedPermission.error,
        })
      );
    }

    const response: IResponseRedeemDirectoryPermission = {
      ok: {
        data: {
          permission: redeemedPermission.permission!,
        },
      },
    };

    return reply.status(200).send(response);
  } catch (error) {
    request.log.error("Error in redeemDirectoryPermissionsHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal Server Error",
      })
    );
  }
}

interface GetSystemPermissionParams extends OrgIdParams {
  system_permission_id: string;
}

/**
 * Handles the request to get a single system permission by its ID.
 * Corresponds to Rust's `get_system_permissions_handler`.
 */
export async function getSystemPermissionsHandler(
  request: FastifyRequest<{ Params: GetSystemPermissionParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id, system_permission_id } = request.params;

    // 1. Authenticate
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    // 2. Fetch permission and check access rights
    const permission = await getSystemPermissionById(
      org_id,
      system_permission_id,
      requesterApiKey.user_id
    );

    // 3. Respond
    if (!permission) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Permission not found or access denied",
        })
      );
    }

    return reply.status(200).send(createApiResponse(permission));
  } catch (error) {
    request.log.error("Error in getSystemPermissionsHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal Server Error",
      })
    );
  }
}

/**
 * Handles the request to list system permissions.
 * Corresponds to Rust's `list_system_permissions_handler`.
 */
export async function listSystemPermissionsHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestListSystemPermissions;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const body = request.body;

    // 1. Authenticate
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    // 2. Call the service to list permissions
    const result = await listSystemPermissions(
      org_id,
      body,
      requesterApiKey.user_id
    );

    // Check for authorization error from the service
    if (result.error) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: result.error })
        );
    }

    // 3. Format and send the successful response
    const responseData: IPaginatedResponse<SystemPermissionFE> = {
      items: result.items!,
      page_size: result.items!.length,
      total: result.total!,
      cursor: result.newCursor,
    };

    return reply.status(200).send(createApiResponse(responseData));
  } catch (error) {
    request.log.error("Error in listSystemPermissionsHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal Server Error",
      })
    );
  }
}

/**
 * Handles creating a new system permission.
 * Corresponds to Rust's `create_system_permissions_handler`.
 */
export async function createSystemPermissionsHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestCreateSystemPermission;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const body = request.body;

    // 1. Authenticate
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }
    const requesterId = requesterApiKey.user_id;

    // 2. Parse and validate resource ID
    const resourceId = parseSystemResourceID(body.resource_id);
    if (!resourceId) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Invalid resource ID format",
        })
      );
    }

    // 3. Authorize the action
    const isOwner = (await getDriveOwnerId(org_id)) === requesterId;
    const hasTablePerms = await checkPermissionsTableAccess(
      org_id,
      requesterId,
      "CREATE",
      isOwner
    );
    const hasManagePerms = await hasSystemManagePermission(
      org_id,
      resourceId,
      requesterId
    );

    if (!isOwner && !hasTablePerms && !hasManagePerms) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Not authorized to create system permissions",
        })
      );
    }

    // 4. Create the permission
    const newPermission = await createSystemPermission(
      org_id,
      body,
      requesterId
    );

    if (newPermission.error) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: newPermission.error,
        })
      );
    }

    // 5. Respond
    return reply.status(201).send(createApiResponse(newPermission.permission));
  } catch (error) {
    request.log.error("Error in createSystemPermissionsHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal Server Error",
      })
    );
  }
}

/**
 * Handles updating an existing system permission.
 * Corresponds to Rust's `update_system_permissions_handler`.
 */
export async function updateSystemPermissionsHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestUpdateSystemPermission;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const body = request.body;

    // 1. Authenticate
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }
    const requesterId = requesterApiKey.user_id;

    // 2. Fetch existing permission to get its resource_id for auth checks
    const existingPermission = await getSystemPermissionById(
      org_id,
      body.id,
      requesterId,
      true
    );
    if (!existingPermission) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Permission not found",
        })
      );
    }

    // 3. Authorize the update
    const isOwner = (await getDriveOwnerId(org_id)) === requesterId;
    const hasTablePerms = await checkPermissionsTableAccess(
      org_id,
      requesterId,
      "EDIT",
      isOwner
    );
    const hasManagePerms = await hasSystemManagePermission(
      org_id,
      existingPermission.resource_id,
      requesterId
    );

    if (!isOwner && !hasTablePerms && !hasManagePerms) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Not authorized to update this system permission",
        })
      );
    }

    // 4. Update the permission
    const updatedPermission = await updateSystemPermission(
      org_id,
      body,
      requesterId
    );

    // 5. Respond
    if (!updatedPermission) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Permission not found or update failed",
        })
      );
    }

    return reply.status(200).send(createApiResponse(updatedPermission));
  } catch (error) {
    request.log.error("Error in updateSystemPermissionsHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal Server Error",
      })
    );
  }
}

/**
 * Handles deleting a system permission.
 * Corresponds to Rust's `delete_system_permissions_handler`.
 */
export async function deleteSystemPermissionsHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestDeleteSystemPermission;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const { permission_id } = request.body;

    // 1. Authenticate
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }
    const requesterId = requesterApiKey.user_id;

    // 2. Fetch the permission to authorize against it
    const permission = await getSystemPermissionById(
      org_id,
      permission_id,
      requesterId,
      true
    );
    if (!permission) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Permission not found",
        })
      );
    }

    // 3. Authorize deletion
    const isOwner = (await getDriveOwnerId(org_id)) === requesterId;
    const isGranter = permission.granted_by === requesterId;
    const hasTablePerms = await checkPermissionsTableAccess(
      org_id,
      requesterId,
      "DELETE",
      isOwner
    );

    if (!isOwner && !isGranter && !hasTablePerms) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Not authorized to delete this permission",
        })
      );
    }

    // 4. Perform deletion
    const deletedId = await deleteSystemPermission(
      org_id,
      permission_id,
      requesterId
    );
    if (!deletedId) {
      // This could happen if the permission was already deleted, or another error occurred.
      return reply.status(500).send(
        createApiResponse(undefined, {
          code: 500,
          message: "Failed to delete permission",
        })
      );
    }

    // 5. Respond
    const response: IResponseDeleteSystemPermission = {
      ok: { data: { deleted_id: deletedId } },
    };
    return reply.status(200).send(response);
  } catch (error) {
    request.log.error("Error in deleteSystemPermissionsHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal Server Error",
      })
    );
  }
}

/**
 * Handles checking system permissions for a given resource and grantee.
 * Corresponds to Rust's `check_system_permissions_handler`.
 */
export async function checkSystemPermissionsHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestCheckSystemPermissions;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const { resource_id, grantee_id } = request.body;

    // 1. Authenticate
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }
    const requesterId = requesterApiKey.user_id;

    // 2. Authorize the check action itself
    const isOwner = (await getDriveOwnerId(org_id)) === requesterId;
    const isCheckingOwn = grantee_id === requesterId;
    const hasTableViewPerms = await checkPermissionsTableAccess(
      org_id,
      requesterId,
      "VIEW",
      isOwner
    );

    if (!isOwner && !isCheckingOwn && !hasTableViewPerms) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Not authorized to check permissions",
        })
      );
    }

    // 3. Parse resource ID and perform the check
    const resourceIdObj = parseSystemResourceID(resource_id);
    if (!resourceIdObj) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Invalid resource ID format",
        })
      );
    }

    const permissions = await checkSystemPermissions(
      org_id,
      resourceIdObj,
      grantee_id
    );

    // 4. Respond
    const response: IResponseCheckSystemPermissions = {
      ok: {
        data: {
          resource_id: resource_id,
          grantee_id: grantee_id,
          permissions: permissions,
        },
      },
    };
    return reply.status(200).send(response);
  } catch (error) {
    request.log.error("Error in checkSystemPermissionsHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal Server Error",
      })
    );
  }
}

/**
 * Handles redeeming a placeholder (magic link) system permission.
 * Corresponds to Rust's `redeem_system_permissions_handler`.
 */
export async function redeemSystemPermissionsHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestRedeemSystemPermission;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const { permission_id, user_id, redeem_code } = request.body;

    // 1. Authenticate the user trying to redeem
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey || requesterApiKey.user_id !== user_id) {
      return reply.status(401).send(
        createApiResponse(undefined, {
          code: 401,
          message: "Unauthorized or user mismatch",
        })
      );
    }

    // 2. Call the service to perform the redemption
    const result = await redeemSystemPermission(org_id, {
      permission_id,
      user_id,
      redeem_code,
      requesterId: requesterApiKey.user_id,
    });

    // 3. Respond based on the service's result
    if (result.error) {
      let statusCode = 400;
      if (result.error === "Permission not found") statusCode = 404;
      if (result.error === "Invalid redeem code") statusCode = 403;
      return reply.status(statusCode).send(
        createApiResponse(undefined, {
          code: statusCode,
          message: result.error,
        })
      );
    }

    const response: IResponseRedeemSystemPermission = {
      ok: {
        data: {
          permission: result.permission!,
        },
      },
    };

    return reply.status(200).send(response);
  } catch (error) {
    request.log.error("Error in redeemSystemPermissionsHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal Server Error",
      })
    );
  }
}
