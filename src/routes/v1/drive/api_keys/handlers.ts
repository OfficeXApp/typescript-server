import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import crypto from "crypto";
import {
  ApiKey,
  FactoryApiResponse,
  IRequestCreateApiKey,
  IRequestUpdateApiKey,
  IRequestDeleteApiKey,
  IResponseDeleteApiKey,
  IDPrefixEnum,
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import {
  authenticateRequest,
  generateApiKey,
  getOwnerId,
} from "../../../../services/auth";

interface OrgIdParams {
  org_id: string;
}

// Type definitions for route params
interface GetApiKeyParams extends OrgIdParams {
  api_key_id: string;
}

interface ListApiKeysParams extends OrgIdParams {
  user_id: string;
}

// Helper function to validate request body
function validateCreateRequest(body: IRequestCreateApiKey): {
  valid: boolean;
  error?: string;
} {
  if (!body.name || body.name.length > 256) {
    return {
      valid: false,
      error: "Name is required and must be less than 256 characters",
    };
  }

  if (body.user_id && !body.user_id.startsWith("UserID_")) {
    return { valid: false, error: "User ID must start with UserID_" };
  }

  if (body.expires_at && body.expires_at !== -1) {
    const now = Date.now();
    if (body.expires_at <= now) {
      return {
        valid: false,
        error: "Expiration time must be in the future or -1 for never expires",
      };
    }
  }

  return { valid: true };
}

function validateUpdateRequest(body: IRequestUpdateApiKey): {
  valid: boolean;
  error?: string;
} {
  if (!body.id || !body.id.startsWith("ApiKeyID_")) {
    return { valid: false, error: "API Key ID must start with ApiKeyID_" };
  }

  if (body.name !== undefined && body.name.length > 256) {
    return { valid: false, error: "Name must be less than 256 characters" };
  }

  if (body.expires_at !== undefined && body.expires_at !== -1) {
    const now = Date.now();
    if (body.expires_at <= now) {
      return {
        valid: false,
        error: "Expiration time must be in the future or -1 for never expires",
      };
    }
  }

  return { valid: true };
}

function validateDeleteRequest(body: IRequestDeleteApiKey): {
  valid: boolean;
  error?: string;
} {
  if (!body.id || !body.id.startsWith("ApiKeyID_")) {
    return { valid: false, error: "API Key ID must start with ApiKeyID_" };
  }

  return { valid: true };
}

function createApiResponse<T>(
  data?: T,
  error?: { code: number; message: string }
): FactoryApiResponse<T> {
  return {
    status: error ? "error" : "success",
    data,
    error,
    timestamp: Date.now(),
  };
}

export async function getApiKeyHandler(
  request: FastifyRequest<{ Params: GetApiKeyParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive");
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const requestedId = request.params.api_key_id;

    // Get the requested API key
    const apiKeys = await db.queryDrive(
      request.params.org_id,
      "SELECT * FROM api_keys WHERE id = ?",
      [requestedId]
    );

    if (!apiKeys || apiKeys.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "API key not found",
        })
      );
    }

    const apiKey = apiKeys[0] as ApiKey;
    const ownerId = await getOwnerId();
    const isOwner = requesterApiKey.user_id === ownerId;
    const isOwnKey = requesterApiKey.user_id === apiKey.user_id;

    // Check permissions
    if (!isOwner && !isOwnKey) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    return reply.status(200).send(createApiResponse(apiKey));
  } catch (error) {
    request.log.error("Error in getApiKeyHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function listApiKeysHandler(
  request: FastifyRequest<{ Params: ListApiKeysParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    // const requesterApiKey = await authenticateRequest(request);
    // if (!requesterApiKey) {
    //   return reply
    //     .status(401)
    //     .send(
    //       createApiResponse(undefined, { code: 401, message: "Unauthorized" })
    //     );
    // }

    // const requestedUserId = request.params.user_id;
    // const ownerId = await getOwnerId();
    // const isOwner = requesterApiKey.user_id === ownerId;
    // const isOwnKeys = requesterApiKey.user_id === requestedUserId;

    // if (!isOwner && !isOwnKeys) {
    //   return reply
    //     .status(403)
    //     .send(
    //       createApiResponse(undefined, { code: 403, message: "Forbidden" })
    //     );
    // }

    // Get all API keys (matching Rust implementation)
    const apiKeys = await db.queryDrive(
      request.params.org_id,
      "SELECT * FROM api_keys ORDER BY created_at DESC"
    );

    if (!apiKeys || apiKeys.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "No API keys found",
        })
      );
    }

    return reply.status(200).send(createApiResponse(apiKeys as ApiKey[]));
  } catch (error) {
    request.log.error("Error in listApiKeysHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

// export async function upsertApiKeyHandler(
//   request: FastifyRequest<{
//     Body: IRequestCreateApiKey | IRequestUpdateApiKey;
//   }>,
//   reply: FastifyReply
// ): Promise<void> {
//   try {
//     // // Authenticate request
//     // const requesterApiKey = await authenticateRequest(request);
//     // if (!requesterApiKey) {
//     //   return reply
//     //     .status(401)
//     //     .send(
//     //       createApiResponse(undefined, { code: 401, message: "Unauthorized" })
//     //     );
//     // }

//     const body = request.body as UpsertApiKeyRequestBody;

//     if (body.action === "CREATE") {
//       const createBody = body as IRequestCreateApiKey;

//       // Validate request
//       const validation = validateCreateRequest(createBody);
//       if (!validation.valid) {
//         return reply.status(400).send(
//           createApiResponse(undefined, {
//             code: 400,
//             message: validation.error!,
//           })
//         );
//       }

//       //   const ownerId = await getOwnerId();
//       //   const isOwner = requesterApiKey.user_id === ownerId;

//       //   // Check permissions
//       //   if (!isOwner) {
//       //     return reply
//       //       .status(403)
//       //       .send(
//       //         createApiResponse(undefined, { code: 403, message: "Forbidden" })
//       //       );
//       //   }

//       //   // Determine user_id for new key
//       //   const keyUserId =
//       //     isOwner && createBody.user_id
//       //       ? createBody.user_id
//       //       : requesterApiKey.user_id;
//       const keyUserId = "UserID_default_owner";

//       // Create new API key
//       const newApiKey: ApiKey = {
//         id: generateUuidv4("ApiKeyID") as any,
//         value: generateApiKey() as any,
//         user_id: keyUserId as any,
//         name: createBody.name,
//         created_at: Date.now(),
//         expires_at: createBody.expires_at || -1,
//         is_revoked: false,
//       };

//       // Insert into database using transaction
//       await dbHelpers.transaction(null, (database) => {
//         const stmt = database.prepare(
//           `INSERT INTO api_keys (id, value, user_id, name, created_at, expires_at, is_revoked)
//            VALUES (?, ?, ?, ?, ?, ?, ?)`
//         );
//         stmt.run(
//           newApiKey.id,
//           newApiKey.value,
//           newApiKey.user_id,
//           newApiKey.name,
//           newApiKey.created_at,
//           newApiKey.expires_at,
//           newApiKey.is_revoked ? 1 : 0
//         );
//       });

//       return reply.status(200).send(createApiResponse(newApiKey));
//     } else if (body.action === "UPDATE") {
//       const updateBody = body as UpdateApiKeyRequestBody;

//       // Validate request
//       const validation = validateUpdateRequest(updateBody);
//       if (!validation.valid) {
//         return reply.status(400).send(
//           createApiResponse(undefined, {
//             code: 400,
//             message: validation.error!,
//           })
//         );
//       }

//       // Get the existing API key
//       const apiKeys = await db.query(
//         "SELECT * FROM api_keys WHERE id = ?",
//         [updateBody.id]
//       );

//       if (!apiKeys || apiKeys.length === 0) {
//         return reply.status(404).send(
//           createApiResponse(undefined, {
//             code: 404,
//             message: "API key not found",
//           })
//         );
//       }

//       const apiKey = apiKeys[0] as ApiKey;
//       const ownerId = await getOwnerId();
//       //   const isOwner = requesterApiKey.user_id === ownerId;
//       //   const isOwnKey = requesterApiKey.user_id === apiKey.user_id;

//       //   // Check permissions
//       //   if (!isOwner && !isOwnKey) {
//       //     return reply
//       //       .status(403)
//       //       .send(
//       //         createApiResponse(undefined, { code: 403, message: "Forbidden" })
//       //       );
//       //   }

//       // Build update query dynamically
//       const updates: string[] = [];
//       const values: any[] = [];

//       if (updateBody.name !== undefined) {
//         updates.push("name = ?");
//         values.push(updateBody.name);
//       }
//       if (updateBody.expires_at !== undefined) {
//         updates.push("expires_at = ?");
//         values.push(updateBody.expires_at);
//       }
//       if (updateBody.is_revoked !== undefined) {
//         updates.push("is_revoked = ?");
//         values.push(updateBody.is_revoked ? 1 : 0);
//       }

//       if (updates.length === 0) {
//         return reply.status(400).send(
//           createApiResponse(undefined, {
//             code: 400,
//             message: "No fields to update",
//           })
//         );
//       }

//       values.push(updateBody.id);

//       // Update in transaction
//       await dbHelpers.transaction(null, (database) => {
//         const stmt = database.prepare(
//           `UPDATE api_keys SET ${updates.join(", ")} WHERE id = ?`
//         );
//         stmt.run(...values);
//       });

//       // Get updated API key
//       const updatedKeys = await db.query(
//         "SELECT * FROM api_keys WHERE id = ?",
//         [updateBody.id]
//       );

//       return reply
//         .status(200)
//         .send(createApiResponse(updatedKeys[0] as ApiKey));
//     } else {
//       return reply
//         .status(400)
//         .send(
//           createApiResponse(undefined, { code: 400, message: "Invalid action" })
//         );
//     }
//   } catch (error) {
//     request.log.error("Error in upsertApiKeyHandler:", error);
//     return reply.status(500).send(
//       createApiResponse(undefined, {
//         code: 500,
//         message: "Internal server error",
//       })
//     );
//   }
// }

export async function createApiKeyHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestCreateApiKey }>,
  reply: FastifyReply
): Promise<void> {
  try {
    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive");
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const body = request.body as IRequestCreateApiKey;
    const createBody = body as IRequestCreateApiKey;

    // Validate request
    const validation = validateCreateRequest(createBody);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    // Build API key
    const apiKey: ApiKey = {
      id: `${IDPrefixEnum.ApiKey}${uuidv4()}` as any,
      value: await generateApiKey(),
      user_id: requesterApiKey.user_id,
      name: createBody.name,
      created_at: Date.now(),
      is_revoked: false,
      begins_at: createBody.begins_at || Date.now(),
      expires_at: createBody.expires_at || -1,
      labels: [],
    };

    // Insert into database using transaction
    await dbHelpers.transaction("drive", request.params.org_id, (database) => {
      const stmt = database.prepare(
        `INSERT INTO api_keys (id, value, user_id, name, created_at, expires_at, is_revoked, begins_at, labels)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );
      stmt.run(
        apiKey.id,
        apiKey.value,
        apiKey.user_id,
        apiKey.name,
        apiKey.created_at,
        apiKey.expires_at,
        apiKey.is_revoked ? 1 : 0,
        apiKey.begins_at,
        apiKey.labels
      );
    });

    return reply.status(200).send(createApiResponse(apiKey));
  } catch (error) {
    request.log.error("Error in createApiKeyHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function updateApiKeyHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestUpdateApiKey }>,
  reply: FastifyReply
): Promise<void> {
  try {
    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive");
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const body = request.body as IRequestUpdateApiKey;

    // Validate request
    const validation = validateUpdateRequest(body);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    // Get the existing API key
    const apiKeys = await db.queryDrive(
      request.params.org_id,
      "SELECT * FROM api_keys WHERE id = ?",
      [body.id]
    );

    if (!apiKeys || apiKeys.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "API key not found",
        })
      );
    }

    const apiKey = apiKeys[0] as ApiKey;
    const ownerId = await getOwnerId();
    const isOwner = requesterApiKey.user_id === ownerId;
    const isOwnKey = requesterApiKey.user_id === apiKey.user_id;

    // Check permissions
    if (!isOwner && !isOwnKey) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    // Build update query dynamically
    const updates: string[] = [];
    const values: any[] = [];

    if (body.name !== undefined) {
      updates.push("name = ?");
      values.push(body.name);
    }
    if (body.expires_at !== undefined) {
      updates.push("expires_at = ?");
      values.push(body.expires_at);
    }
    if (body.is_revoked !== undefined) {
      updates.push("is_revoked = ?");
      values.push(body.is_revoked ? 1 : 0);
    }

    if (updates.length === 0) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "No fields to update",
        })
      );
    }

    values.push(body.id);

    // Update in transaction
    await dbHelpers.transaction("drive", request.params.org_id, (database) => {
      const stmt = database.prepare(
        `UPDATE api_keys SET ${updates.join(", ")} WHERE id = ?`
      );
      stmt.run(...values);
    });

    // Get updated API key
    const updatedKeys = await db.queryDrive(
      request.params.org_id,
      "SELECT * FROM api_keys WHERE id = ?",
      [body.id]
    );

    return reply.status(200).send(createApiResponse(updatedKeys[0] as ApiKey));
  } catch (error) {
    request.log.error("Error in updateApiKeyHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function deleteApiKeyHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestDeleteApiKey }>,
  reply: FastifyReply
): Promise<void> {
  try {
    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive");
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const body = request.body as IRequestDeleteApiKey;

    // Validate request
    const validation = validateDeleteRequest(body);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    // Get the API key to be deleted
    const apiKeys = await db.queryDrive(
      request.params.org_id,
      "SELECT * FROM api_keys WHERE id = ?",
      [body.id]
    );

    if (!apiKeys || apiKeys.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "API key not found",
        })
      );
    }

    const apiKey = apiKeys[0] as ApiKey;
    const ownerId = await getOwnerId();
    const isOwner = requesterApiKey.user_id === ownerId;
    const isOwnKey = requesterApiKey.user_id === apiKey.user_id;

    // Check permissions
    if (!isOwner && !isOwnKey) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    // Delete the API key in transaction
    await dbHelpers.transaction("drive", request.params.org_id, (database) => {
      const stmt = database.prepare("DELETE FROM api_keys WHERE id = ?");
      stmt.run(body.id);
    });

    const deletedData: IResponseDeleteApiKey = {
      ok: {
        data: {
          id: body.id,
          deleted: true,
        },
      },
    };

    return reply.status(200).send(createApiResponse(deletedData));
  } catch (error) {
    request.log.error("Error in deleteApiKeyHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}
