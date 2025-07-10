import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import crypto from "crypto";
import {
  FactoryApiKey,
  FactoryApiResponse,
  FactoryCreateApiKeyRequestBody,
  FactoryUpdateApiKeyRequestBody,
  FactoryUpsertApiKeyRequestBody,
  FactoryDeleteApiKeyRequestBody,
  FactoryDeletedApiKeyData,
  FactoryStateSnapshot,
  FactorySnapshotResponse,
  IDPrefixEnum,
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import { authenticateRequest, getOwnerId } from "../../../../services/auth";

// Import the database service

// Type definitions for route params
interface GetApiKeyParams {
  api_key_id: string;
}

interface ListApiKeysParams {
  user_id: string;
}

// Helper function to generate API key value
function generateApiKey(): string {
  return crypto.randomBytes(32).toString("base64url");
}

// Helper function to generate UUID with prefix
function generateUuidv4(prefix: string): string {
  return `${prefix}_${uuidv4()}`;
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
        message: "Internal server error",
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

      const ownerId = await getOwnerId();
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
        message: "Internal server error",
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
        message: "Internal server error",
      })
    );
  }
}

export async function snapshotHandler(
  request: FastifyRequest,
  reply: FastifyReply
): Promise<void> {
  try {
    // Check if local environment (implement your own logic)
    const isLocalEnvironment = process.env.NODE_ENV === "development";

    if (!isLocalEnvironment) {
      // Authenticate request
      const requesterApiKey = await authenticateRequest(request, "factory");
      if (!requesterApiKey) {
        return reply
          .status(401)
          .send(
            createApiResponse(undefined, { code: 401, message: "Unauthorized" })
          );
      }

      // Check if requester is owner
      const ownerId = await getOwnerId();
      const isOwner = requesterApiKey.user_id === ownerId;
      if (!isOwner) {
        return reply
          .status(403)
          .send(
            createApiResponse(undefined, { code: 403, message: "Forbidden" })
          );
      }
    }

    // Gather all state data using the database helpers
    const stateSnapshot = await dbHelpers.withFactory((database) => {
      // Get all API keys
      const apiKeysStmt = database.prepare("SELECT * FROM factory_api_keys");
      const apiKeys = apiKeysStmt.all() as FactoryApiKey[];

      const apiKeysByIdMap: Record<string, FactoryApiKey> = {};
      const apiKeysByValueMap: Record<string, string> = {};
      const apiKeysHistory: string[] = [];

      for (const key of apiKeys) {
        apiKeysByIdMap[key.id] = key;
        apiKeysByValueMap[key.value] = key.id;
        apiKeysHistory.push(key.id);
      }

      // Get users' API keys mapping
      const usersApiKeysStmt = database.prepare(`
        SELECT user_id, GROUP_CONCAT(id) as api_key_ids 
        FROM factory_api_keys 
        GROUP BY user_id
      `);
      const usersApiKeys = usersApiKeysStmt.all() as any[];
      const usersApiKeysMap: Record<string, string[]> = {};

      for (const row of usersApiKeys) {
        usersApiKeysMap[row.user_id] = row.api_key_ids
          ? row.api_key_ids.split(",")
          : [];
      }

      // Get GiftcardSpawnOrg data
      const giftcardsStmt = database.prepare(
        "SELECT * FROM giftcard_spawn_orgs"
      );
      const giftcards = giftcardsStmt.all() as any[];
      const giftcardByIdMap: Record<string, any> = {};
      const historicalGiftcards: string[] = [];

      for (const giftcard of giftcards) {
        giftcardByIdMap[giftcard.id] = giftcard;
        historicalGiftcards.push(giftcard.id);
      }

      // Get deployments
      const deploymentsStmt = database.prepare(
        "SELECT * FROM factory_spawn_history"
      );
      const deployments = deploymentsStmt.all() as any[];
      const deploymentsByGiftcardId: Record<string, any> = {};

      for (const deployment of deployments) {
        deploymentsByGiftcardId[deployment.giftcard_id] = deployment;
      }

      // Get drive to giftcard mapping
      const driveToGiftcardStmt = database.prepare(
        "SELECT drive_id, giftcard_id FROM factory_spawn_history"
      );
      const driveToGiftcard = driveToGiftcardStmt.all() as any[];
      const driveToGiftcardMap: Record<string, string> = {};

      for (const row of driveToGiftcard) {
        driveToGiftcardMap[row.drive_id] = row.giftcard_id;
      }

      // Get user to giftcards mapping
      const userGiftcardsStmt = database.prepare(`
        SELECT user_id, GROUP_CONCAT(giftcard_id) as giftcard_ids 
        FROM user_giftcard_spawn_orgs 
        GROUP BY user_id
      `);
      const userGiftcards = userGiftcardsStmt.all() as any[];
      const userToGiftcardsMap: Record<string, string[]> = {};

      for (const row of userGiftcards) {
        userToGiftcardsMap[row.user_id] = row.giftcard_ids
          ? row.giftcard_ids.split(",")
          : [];
      }

      // Get system configuration
      const systemConfig = {
        canister_id: process.env.CANISTER_ID || "unknown",
        version: process.env.VERSION || "1.0.0",
        owner_id: process.env.OWNER_ID || "UserID_default_owner",
        endpoint_url: process.env.ENDPOINT_URL || "http://localhost:3000",
      };

      return {
        // System info
        canister_id: systemConfig.canister_id,
        version: systemConfig.version,
        owner_id: systemConfig.owner_id as any,
        endpoint_url: systemConfig.endpoint_url,

        // API keys state
        apikeys_by_value: apiKeysByValueMap,
        apikeys_by_id: apiKeysByIdMap,
        users_apikeys: usersApiKeysMap,
        apikeys_history: apiKeysHistory,

        // GiftcardSpawnOrg state
        deployments_by_giftcard_id: deploymentsByGiftcardId,
        historical_giftcards: historicalGiftcards,
        drive_to_giftcard_hashtable: driveToGiftcardMap,
        user_to_giftcards_hashtable: userToGiftcardsMap,
        giftcard_by_id: giftcardByIdMap,

        // Timestamp
        timestamp_ns: Date.now() * 1_000_000, // Convert to nanoseconds
      } as FactoryStateSnapshot;
    });

    const response: FactorySnapshotResponse = {
      status: "success",
      data: stateSnapshot,
      timestamp: Date.now(),
    };

    return reply.status(200).send(response);
  } catch (error) {
    request.log.error("Error in snapshotHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}
