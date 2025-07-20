import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import crypto from "crypto";
import {
  FactoryApiKey,
  ApiResponse,
  FactoryCreateApiKeyRequestBody,
  FactoryUpdateApiKeyRequestBody,
  FactoryUpsertApiKeyRequestBody,
  FactoryDeleteApiKeyRequestBody,
  FactoryDeletedApiKeyData,
  FactorySnapshotResponse,
  IDPrefixEnum,
  DriveID,
  UserID,
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import { authenticateRequest, generateApiKey } from "../../../../services/auth";
import { OrgIdParams } from "../../types";
import {
  getFactorySnapshot,
  FactoryStateSnapshot,
} from "../../../../services/snapshot/factory";
import { LOCAL_DEV_MODE } from "../../../../constants";
import { getAppropriateUrlEndpoint } from "../spawnorg/handlers";

// Import the database service

// Type definitions for route params
export interface GetApiKeyParams {
  api_key_id: string;
}

export interface ListApiKeysParams {
  user_id: string;
}

// Helper function to validate request body
function validateCreateRequest(body: FactoryCreateApiKeyRequestBody): {
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

function validateUpdateRequest(body: FactoryUpdateApiKeyRequestBody): {
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

function validateDeleteRequest(body: FactoryDeleteApiKeyRequestBody): {
  valid: boolean;
  error?: string;
} {
  if (!body.id || !body.id.startsWith("ApiKeyID_")) {
    return { valid: false, error: "API Key ID must start with ApiKeyID_" };
  }

  return { valid: true };
}

function createApiResponse<T>(
  data: T,
  error?: { code: number; message: string }
): ApiResponse<T> {
  return {
    ok: {
      data,
    },
  };
}

export async function getApiKeyHandler(
  request: FastifyRequest<{ Params: GetApiKeyParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "factory");
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const requestedId = request.params.api_key_id;

    // Get the requested API key
    const apiKeys = await db.queryFactory(
      "SELECT * FROM factory_api_keys WHERE id = ?",
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

    const apiKey = apiKeys[0] as FactoryApiKey;
    const ownerId = request.server.factory_owner;
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
        message: `Internal server error - ${error}`,
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
    const apiKeys = await db.queryFactory(
      "SELECT * FROM factory_api_keys ORDER BY created_at DESC"
    );

    if (!apiKeys || apiKeys.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "No API keys found",
        })
      );
    }

    return reply
      .status(200)
      .send(createApiResponse(apiKeys as FactoryApiKey[]));
  } catch (error) {
    request.log.error("Error in listApiKeysHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function upsertApiKeyHandler(
  request: FastifyRequest<{ Body: FactoryUpsertApiKeyRequestBody }>,
  reply: FastifyReply
): Promise<void> {
  try {
    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "factory");
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const body = request.body as FactoryUpsertApiKeyRequestBody;

    if (body.action === "CREATE") {
      const createBody = body as FactoryCreateApiKeyRequestBody;

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

      const ownerId = request.server.factory_owner;
      const isOwner = requesterApiKey.user_id === ownerId;

      // Check permissions
      if (!isOwner) {
        return reply
          .status(403)
          .send(
            createApiResponse(undefined, { code: 403, message: "Forbidden" })
          );
      }
      const ownerID = request.server.factory_owner;

      // Create new API key
      const newApiKey: FactoryApiKey = {
        id: `${IDPrefixEnum.ApiKey}${uuidv4()}` as any,
        value: await generateApiKey(),
        user_id: ownerID,
        name: createBody.name,
        created_at: Date.now(),
        expires_at: createBody.expires_at || -1,
        is_revoked: false,
      };

      // Insert into database using transaction
      await dbHelpers.transaction("factory", null, (database) => {
        const stmt = database.prepare(
          `INSERT INTO factory_api_keys (id, value, user_id, name, created_at, expires_at, is_revoked) 
           VALUES (?, ?, ?, ?, ?, ?, ?)`
        );
        stmt.run(
          newApiKey.id,
          newApiKey.value,
          newApiKey.user_id,
          newApiKey.name,
          newApiKey.created_at,
          newApiKey.expires_at,
          newApiKey.is_revoked ? 1 : 0
        );
      });

      return reply.status(200).send(createApiResponse(newApiKey));
    } else if (body.action === "UPDATE") {
      const updateBody = body as FactoryUpdateApiKeyRequestBody;

      // Validate request
      const validation = validateUpdateRequest(updateBody);
      if (!validation.valid) {
        return reply.status(400).send(
          createApiResponse(undefined, {
            code: 400,
            message: validation.error!,
          })
        );
      }

      // Get the existing API key
      const apiKeys = await db.queryFactory(
        "SELECT * FROM factory_api_keys WHERE id = ?",
        [updateBody.id]
      );

      if (!apiKeys || apiKeys.length === 0) {
        return reply.status(404).send(
          createApiResponse(undefined, {
            code: 404,
            message: "API key not found",
          })
        );
      }

      const apiKey = apiKeys[0] as FactoryApiKey;
      const ownerId = request.server.factory_owner;
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

      if (updateBody.name !== undefined) {
        updates.push("name = ?");
        values.push(updateBody.name);
      }
      if (updateBody.expires_at !== undefined) {
        updates.push("expires_at = ?");
        values.push(updateBody.expires_at);
      }
      if (updateBody.is_revoked !== undefined) {
        updates.push("is_revoked = ?");
        values.push(updateBody.is_revoked ? 1 : 0);
      }

      if (updates.length === 0) {
        return reply.status(400).send(
          createApiResponse(undefined, {
            code: 400,
            message: "No fields to update",
          })
        );
      }

      values.push(updateBody.id);

      // Update in transaction
      await dbHelpers.transaction("factory", null, (database) => {
        const stmt = database.prepare(
          `UPDATE factory_api_keys SET ${updates.join(", ")} WHERE id = ?`
        );
        stmt.run(...values);
      });

      // Get updated API key
      const updatedKeys = await db.queryFactory(
        "SELECT * FROM factory_api_keys WHERE id = ?",
        [updateBody.id]
      );

      return reply
        .status(200)
        .send(createApiResponse(updatedKeys[0] as FactoryApiKey));
    } else {
      return reply
        .status(400)
        .send(
          createApiResponse(undefined, { code: 400, message: "Invalid action" })
        );
    }
  } catch (error) {
    request.log.error("Error in upsertApiKeyHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function deleteApiKeyHandler(
  request: FastifyRequest<{ Body: FactoryDeleteApiKeyRequestBody }>,
  reply: FastifyReply
): Promise<void> {
  try {
    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "factory");
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const body = request.body as FactoryDeleteApiKeyRequestBody;

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
    const apiKeys = await db.queryFactory(
      "SELECT * FROM factory_api_keys WHERE id = ?",
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

    const apiKey = apiKeys[0] as FactoryApiKey;
    const ownerId = request.server.factory_owner;
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
    await dbHelpers.transaction("factory", null, (database) => {
      const stmt = database.prepare(
        "DELETE FROM factory_api_keys WHERE id = ?"
      );
      stmt.run(body.id);
    });

    const deletedData: FactoryDeletedApiKeyData = {
      id: body.id,
      deleted: true,
    };

    return reply.status(200).send(createApiResponse(deletedData));
  } catch (error) {
    request.log.error("Error in deleteApiKeyHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function snapshotFactoryHandler(
  request: FastifyRequest<{}>,
  reply: FastifyReply
): Promise<void> {
  try {
    if (!LOCAL_DEV_MODE) {
      // Authenticate request and check if owner
      const requesterApiKey = await authenticateRequest(request, "factory");
      if (!requesterApiKey) {
        return reply.status(401).send(
          createApiResponse<undefined>(undefined, {
            code: 401,
            message: "Unauthorized",
          })
        );
      }
    }

    // Call the refactored function to get the full snapshot
    const endpoint = getAppropriateUrlEndpoint(request);
    const stateSnapshot: FactoryStateSnapshot =
      await getFactorySnapshot(endpoint);

    reply
      .status(200)
      .send(createApiResponse<FactoryStateSnapshot>(stateSnapshot));
  } catch (error: any) {
    request.log.error("Error in snapshotFactoryHandler:", error);
    // Differentiate between authorization errors and other internal errors
    if (error.message.includes("Forbidden")) {
      reply.status(403).send(
        createApiResponse<undefined>(undefined, {
          code: 403,
          message: error.message,
        })
      );
    } else {
      reply.status(500).send(
        createApiResponse<undefined>(undefined, {
          code: 500,
          message: `Internal server error - ${error}`,
        })
      );
    }
  }
}
