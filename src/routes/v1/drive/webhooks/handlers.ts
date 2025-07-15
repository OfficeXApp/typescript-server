// typescript-server/src/routes/v1/drive/webhooks/handlers.ts
import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import {
  Webhook,
  WebhookID,
  WebhookEventLabel,
  IDPrefixEnum,
  IRequestCreateWebhook,
  IRequestUpdateWebhook,
  IRequestDeleteWebhook,
  IResponseDeleteWebhook,
  IResponseGetWebhook,
  IResponseListWebhooks,
  IResponseCreateWebhook,
  IResponseUpdateWebhook,
  SortDirection,
  SystemPermissionType, // Import SystemPermissionType
  SystemResourceID, // Import SystemResourceID
  SystemTableValueEnum, // Import SystemTableValueEnum
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import { authenticateRequest } from "../../../../services/auth";
import { OrgIdParams } from "../../types";
import {
  canUserAccessSystemPermission, // Import the system permission checker
  hasSystemManagePermission, // Import for manage permission check
  checkSystemPermissions,
} from "../../../../services/permissions/system"; // Import relevant permission services

// Helper function to validate webhook event
function isValidWebhookEvent(event: string): boolean {
  return Object.values(WebhookEventLabel).includes(event as WebhookEventLabel);
}

// Helper function to create API response
function createApiResponse<T>(
  data?: T,
  error?: { code: number; message: string }
) {
  return {
    status: error ? "error" : "success",
    data,
    error,
    timestamp: Date.now(),
  };
}

// Helper function to validate create webhook request
function validateCreateRequest(body: IRequestCreateWebhook): {
  valid: boolean;
  error?: string;
} {
  if (!body.alt_index || body.alt_index.length > 256) {
    return {
      valid: false,
      error: "Alt index is required and must be less than 256 characters",
    };
  }

  if (!body.url || !body.url.startsWith("http")) {
    return { valid: false, error: "URL is required and must be valid" };
  }

  if (!body.event || !isValidWebhookEvent(body.event)) {
    return { valid: false, error: "Invalid webhook event" };
  }

  if (body.filters && body.filters.length > 256) {
    return {
      valid: false,
      error: "Filters must be 256 characters or less",
    };
  }

  return { valid: true };
}

// Helper function to validate update request
function validateUpdateRequest(body: IRequestUpdateWebhook): {
  valid: boolean;
  error?: string;
} {
  if (!body.id || !body.id.startsWith("WebhookID_")) {
    return { valid: false, error: "Webhook ID must start with WebhookID_" };
  }

  if (body.url && !body.url.startsWith("http")) {
    return { valid: false, error: "URL must be valid" };
  }

  if (body.filters && body.filters.length > 256) {
    return {
      valid: false,
      error: "Filters must be 256 characters or less",
    };
  }

  return { valid: true };
}

// Helper function to validate delete request
function validateDeleteRequest(body: IRequestDeleteWebhook): {
  valid: boolean;
  error?: string;
} {
  if (!body.id || !body.id.startsWith("WebhookID_")) {
    return { valid: false, error: "Webhook ID must start with WebhookID_" };
  }

  return { valid: true };
}

export async function getWebhookHandler(
  request: FastifyRequest<{ Params: { webhook_id: string } & OrgIdParams }>,
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

    const webhookId = request.params.webhook_id;

    // Retrieve the webhook from the database
    const webhooks = await db.queryDrive(
      request.params.org_id,
      "SELECT * FROM webhooks WHERE id = ?",
      [webhookId]
    );

    if (!webhooks || webhooks.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Webhook not found",
        })
      );
    }

    const webhook = webhooks[0] as Webhook;

    const hasPermission = await canUserAccessSystemPermission(
      webhook.id as SystemResourceID, // The webhook ID is a SystemResourceID::Record
      requesterApiKey.user_id,
      request.params.org_id
    );

    if (!hasPermission) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Forbidden: Not authorized to view this webhook.",
        })
      );
    }

    return reply.status(200).send(createApiResponse(webhook));
  } catch (error) {
    request.log.error("Error in getWebhookHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function listWebhooksHandler(
  request: FastifyRequest<{
    Body: {
      filters?: string;
      page_size?: number;
      direction?: SortDirection;
      cursor?: string;
    };
    Params: OrgIdParams;
  }>,
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

    // PERMIT: Check if the user has VIEW permission on the Webhooks table
    const hasPermission = await checkSystemPermissions(
      `TABLE_${SystemTableValueEnum.WEBHOOKS}` as SystemResourceID,
      requesterApiKey.user_id,
      request.params.org_id
    );

    if (!hasPermission.includes(SystemPermissionType.VIEW)) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Forbidden: Not authorized to list webhooks.",
        })
      );
    }

    const {
      filters = "",
      page_size = 50,
      direction = SortDirection.ASC,
      cursor,
    } = request.body;

    // Build SQL query
    let sql = "SELECT * FROM webhooks";
    const params: any[] = [];

    if (filters) {
      sql += " WHERE event LIKE ?";
      params.push(`%${filters}%`);
    }

    sql += ` ORDER BY created_at ${direction === SortDirection.ASC ? SortDirection.ASC : SortDirection.DESC}`;

    if (page_size) {
      sql += " LIMIT ?";
      params.push(page_size);
    }

    if (cursor) {
      // TODO: Implement cursor-based pagination
      // This is a simplified version - you'll need to adjust based on your actual pagination needs
      sql += " AND created_at > ?";
      params.push(cursor);
    }

    // Get webhooks from database
    const webhooks = await db.queryDrive(request.params.org_id, sql, params);

    const response = {
      items: webhooks as Webhook[],
      page_size: webhooks.length,
      total: webhooks.length, // TODO: Get total count from separate query for accurate pagination
      direction,
      cursor: null, // TODO: Implement cursor for next page
    };

    return reply.status(200).send(createApiResponse(response));
  } catch (error) {
    request.log.error("Error in listWebhooksHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function createWebhookHandler(
  request: FastifyRequest<{ Body: IRequestCreateWebhook; Params: OrgIdParams }>,
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

    // PERMIT: Check if the user has CREATE permission on the Webhooks table
    const hasPermission = await checkSystemPermissions(
      `TABLE_${SystemTableValueEnum.WEBHOOKS}` as SystemResourceID,
      requesterApiKey.user_id,
      request.params.org_id
    );

    if (!hasPermission.includes(SystemPermissionType.CREATE)) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Forbidden: Not authorized to create webhooks.",
        })
      );
    }

    const body = request.body;

    // Validate request
    const validation = validateCreateRequest(body);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    // Create new webhook
    const webhookId = body.id || `${IDPrefixEnum.Webhook}${uuidv4()}`;
    const now = Date.now();

    const webhook: Webhook = {
      id: webhookId,
      name: body.name || `${body.event}@${body.alt_index}`,
      url: body.url,
      alt_index: body.alt_index,
      event: body.event as WebhookEventLabel,
      description: body.description || "",
      labels: [],
      signature: body.signature || "",
      note: body.note,
      active: body.active ?? true,
      filters: body.filters || "",
      external_id: body.external_id,
      external_payload: body.external_payload,
      created_at: now,
    };

    // Insert into database using transaction
    await dbHelpers.transaction("drive", request.params.org_id, (database) => {
      const stmt = database.prepare(
        `INSERT INTO webhooks (
          id, name, url, alt_index, event, signature, note, is_active, filters, 
          external_id, external_payload, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );
      stmt.run(
        webhook.id,
        webhook.name,
        webhook.url,
        webhook.alt_index,
        webhook.event,
        webhook.signature,
        webhook.note,
        webhook.active ? 1 : 0,
        webhook.filters,
        webhook.external_id,
        webhook.external_payload,
        webhook.created_at
      );
    });

    return reply.status(200).send(createApiResponse(webhook));
  } catch (error) {
    request.log.error("Error in createWebhookHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function updateWebhookHandler(
  request: FastifyRequest<{ Body: IRequestUpdateWebhook; Params: OrgIdParams }>,
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

    const body = request.body;

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

    // Get existing webhook
    const webhooks = await db.queryDrive(
      request.params.org_id,
      "SELECT * FROM webhooks WHERE id = ?",
      [body.id]
    );

    if (!webhooks || webhooks.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Webhook not found",
        })
      );
    }

    const existingWebhook = webhooks[0] as Webhook;

    // PERMIT: Check if the user has EDIT permission on this specific webhook record
    const hasPermission = await canUserAccessSystemPermission(
      existingWebhook.id as SystemResourceID, // The webhook ID is a SystemResourceID::Record
      requesterApiKey.user_id,
      request.params.org_id
    );

    if (!hasPermission) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Forbidden: Not authorized to update this webhook.",
        })
      );
    }

    // Build update query dynamically
    const updates: string[] = [];
    const values: any[] = [];

    if (body.url !== undefined) {
      updates.push("url = ?");
      values.push(body.url);
    }
    if (body.signature !== undefined) {
      updates.push("signature = ?");
      values.push(body.signature);
    }
    if (body.name !== undefined) {
      updates.push("name = ?");
      values.push(body.name);
    }
    if (body.note !== undefined) {
      updates.push("note = ?");
      values.push(body.note);
    }
    if (body.active !== undefined) {
      updates.push("is_active = ?");
      values.push(body.active ? 1 : 0);
    }
    if (body.filters !== undefined) {
      updates.push("filters = ?");
      values.push(body.filters);
    }
    if (body.external_id !== undefined) {
      updates.push("external_id = ?");
      values.push(body.external_id);
    }
    if (body.external_payload !== undefined) {
      updates.push("external_payload = ?");
      values.push(body.external_payload);
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
        `UPDATE webhooks SET ${updates.join(", ")} WHERE id = ?`
      );
      stmt.run(...values);
    });

    // Get updated webhook
    const updatedWebhooks = await db.queryDrive(
      request.params.org_id,
      "SELECT * FROM webhooks WHERE id = ?",
      [body.id]
    );

    return reply
      .status(200)
      .send(createApiResponse(updatedWebhooks[0] as Webhook));
  } catch (error) {
    request.log.error("Error in updateWebhookHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function deleteWebhookHandler(
  request: FastifyRequest<{ Body: IRequestDeleteWebhook; Params: OrgIdParams }>,
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

    const body = request.body;

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

    // Get existing webhook to check permissions
    const webhooks = await db.queryDrive(
      request.params.org_id,
      "SELECT id FROM webhooks WHERE id = ?",
      [body.id]
    );

    if (!webhooks || webhooks.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Webhook not found",
        })
      );
    }

    const existingWebhookId = webhooks[0].id;

    // PERMIT: Check if the user has DELETE permission on this specific webhook record
    const hasPermission = await canUserAccessSystemPermission(
      existingWebhookId as SystemResourceID, // The webhook ID is a SystemResourceID::Record
      requesterApiKey.user_id,
      request.params.org_id
    );

    if (!hasPermission) {
      return reply.status(403).send(
        createApiResponse(undefined, {
          code: 403,
          message: "Forbidden: Not authorized to delete this webhook.",
        })
      );
    }

    // Delete the webhook in transaction
    await dbHelpers.transaction("drive", request.params.org_id, (database) => {
      const stmt = database.prepare("DELETE FROM webhooks WHERE id = ?");
      stmt.run(body.id);
    });

    const deletedData: IResponseDeleteWebhook = {
      ok: {
        data: {
          id: body.id,
          deleted: true,
        },
      },
    };

    return reply.status(200).send(createApiResponse(deletedData));
  } catch (error) {
    request.log.error("Error in deleteWebhookHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

// Helper functions for firing webhooks (similar to Rust implementation)
export async function getActiveWebhooks(
  orgId: string,
  altIndex: string,
  event: WebhookEventLabel
): Promise<Webhook[]> {
  const webhooks = await db.queryDrive(
    orgId,
    "SELECT * FROM webhooks WHERE alt_index = ? AND event = ? AND is_active = 1",
    [altIndex, event]
  );
  return webhooks as Webhook[];
}

export async function fireWebhook(
  orgId: string,
  event: WebhookEventLabel,
  webhooks: Webhook[],
  payload: any
): Promise<void> {
  for (const webhook of webhooks) {
    try {
      const response = await fetch(webhook.url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          signature: webhook.signature,
        },
        body: JSON.stringify({
          event: event.toString(),
          timestamp_ms: Date.now(),
          nonce: Date.now(),
          webhook_id: webhook.id,
          webhook_alt_index: webhook.alt_index,
          payload,
        }),
      });

      if (!response.ok) {
        console.error(`Webhook failed for ${webhook.url}: ${response.status}`);
      }
    } catch (error) {
      console.error(`Error firing webhook to ${webhook.url}:`, error);
    }
  }
}
