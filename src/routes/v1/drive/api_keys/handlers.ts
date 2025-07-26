import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import crypto from "crypto";
import {
  ApiKey,
  ISuccessResponse,
  IRequestCreateApiKey,
  IRequestUpdateApiKey,
  IRequestDeleteApiKey,
  IResponseDeleteApiKey,
  IDPrefixEnum,
  UserID,
  SystemPermissionType,
  ApiKeyValue,
  FactoryApiKey,
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import { authenticateRequest, generateApiKey } from "../../../../services/auth";
import { createApiResponse, getDriveOwnerId, OrgIdParams } from "../../types";
import { checkPermissionsTableAccess } from "../../../../services/permissions/system";

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

// Helper function to redact API key
async function redactApiKey(
  apiKey: ApiKey,
  requesterApiKey: ApiKey | FactoryApiKey,
  orgId: string
): Promise<ApiKey> {
  const redactedApiKey = { ...apiKey };

  const ownerId = await getDriveOwnerId(orgId);
  const isOwner = requesterApiKey.user_id === ownerId;
  const isOwnKey = requesterApiKey.id === apiKey.id;

  // Only the owner of the key or the drive owner can see the full key value
  if (!isOwner && !isOwnKey) {
    redactedApiKey.value = `sk_...${apiKey.value.slice(-4)}` as ApiKeyValue;
  }

  return redactedApiKey;
}

export async function getApiKeyHandler(
  request: FastifyRequest<{ Params: GetApiKeyParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    // Authenticate request
    const requesterApiKey = await authenticateRequest(
      request,
      "drive",
      request.params.org_id
    );
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

    // Augment the DB result to conform to the ApiKey type
    const apiKey: ApiKey = {
      ...(apiKeys[0] as any),
      labels: [],
      private_note: undefined,
    };

    const ownerId = await getDriveOwnerId(request.params.org_id);
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

    const redactedApiKey = await redactApiKey(
      apiKey,
      requesterApiKey,
      request.params.org_id
    );

    return reply.status(200).send(createApiResponse(redactedApiKey));
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
    const requesterApiKey = await authenticateRequest(
      request,
      "drive",
      request.params.org_id
    );
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

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

    const redactedApiKeys = await Promise.all(
      (apiKeys as any[]).map((key) => {
        // Augment each key to conform to the ApiKey type
        const fullKey: ApiKey = {
          ...(key as any),
          labels: [],
          private_note: undefined,
        };
        return redactApiKey(fullKey, requesterApiKey, request.params.org_id);
      })
    );

    return reply.status(200).send(createApiResponse(redactedApiKeys));
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

export async function createApiKeyHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestCreateApiKey }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const createBody = request.body;

    // ... (input validation: name, user_id format, expires_at) ...
    const validation = validateCreateRequest(createBody);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    const ownerId = await getDriveOwnerId(org_id);
    const isOwner = requesterApiKey.user_id === ownerId;

    let keyUserId: UserID;

    if (isOwner && createBody.user_id) {
      // Owner can specify a user_id to create a key for someone else
      keyUserId = createBody.user_id as UserID;
    } else {
      // Non-owners can only create API keys for themselves
      if (
        createBody.user_id &&
        createBody.user_id !== requesterApiKey.user_id
      ) {
        // If a non-owner tries to create a key for someone else, check for CREATE permission
        const hasCreatePermission = await checkPermissionsTableAccess(
          requesterApiKey.user_id,
          SystemPermissionType.CREATE,
          org_id
        );
        if (!hasCreatePermission) {
          return reply.status(403).send(
            createApiResponse(undefined, {
              code: 403,
              message:
                "Forbidden: Not authorized to create API keys for other users.",
            })
          );
        }
        keyUserId = createBody.user_id as UserID; // Allow if they have explicit permission
      } else {
        // Default: create API key for the requester themselves (no extra permission needed)
        keyUserId = requesterApiKey.user_id;
      }
    }

    // Ensure the target keyUserId actually exists as a contact
    const targetContact = await db.queryDrive(
      org_id,
      "SELECT id FROM contacts WHERE id = ?",
      [keyUserId]
    );
    if (targetContact.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: `Target user_id '${keyUserId}' does not exist as a contact.`,
        })
      );
    }

    const apiKey: ApiKey = {
      id: `${IDPrefixEnum.ApiKey}${uuidv4()}` as any,
      value: await generateApiKey(),
      user_id: keyUserId, // Use the determined user ID
      name: createBody.name,
      created_at: Date.now(),
      is_revoked: false,
      begins_at: createBody.begins_at || Date.now(),
      expires_at: createBody.expires_at || -1,
      labels: [], // Labels are handled separately or default empty
      private_note: undefined, // Ensure all fields are present
      external_id: createBody.external_id, // Pass along external_id
      external_payload: createBody.external_payload, // Pass along external_payload
    };

    await dbHelpers.transaction("drive", org_id, (database) => {
      const stmt = database.prepare(
        `INSERT INTO api_keys (id, value, user_id, name, private_note, created_at, expires_at, is_revoked, begins_at, external_id, external_payload)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );
      stmt.run(
        apiKey.id,
        apiKey.value,
        apiKey.user_id,
        apiKey.name,
        apiKey.private_note || null, // Ensure null for optional fields
        apiKey.created_at,
        apiKey.expires_at,
        apiKey.is_revoked ? 1 : 0,
        apiKey.begins_at,
        apiKey.external_id || null, // Ensure null for optional fields
        apiKey.external_payload || null // Ensure null for optional fields
      );
    });

    // A newly created key should be returned to the creator, but redacted for others.
    // Our redactApiKey function handles this logic correctly.
    const redactedResponse = await redactApiKey(
      apiKey,
      requesterApiKey, // The key of the user making the request
      org_id
    );

    return reply.status(200).send(createApiResponse(redactedResponse));
  } catch (error) {
    request.log.error("Error in createApiKeyHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
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
    const requesterApiKey = await authenticateRequest(
      request,
      "drive",
      request.params.org_id
    );
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
    const ownerId = "check_db_owner";
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
        message: `Internal server error - ${error}`,
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
    const requesterApiKey = await authenticateRequest(
      request,
      "drive",
      request.params.org_id
    );
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
    const ownerId = "check_db_owner";
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
        message: `Internal server error - ${error}`,
      })
    );
  }
}
