import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import {
  FactoryApiResponse,
  CreateGiftcardRefuelRequestBody,
  UpdateGiftcardRefuelRequestBody,
  UpsertGiftcardRefuelRequestBody,
  DeleteGiftcardRefuelRequestBody,
  DeletedGiftcardRefuelData,
  RedeemGiftcardRefuelData,
  GiftcardRefuel,
  RedeemGiftcardRefuelResult,
  FactoryRefuelHistoryRecord,
  ListGiftcardRefuelsRequestBody,
  ListGiftcardRefuelsResponseData,
  SortDirection,
  IDPrefixEnum,
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import { authenticateRequest } from "../../../../services/auth";
import { isValidID } from "../../../../api/helpers";
import { validateIcpPrincipal } from "../../../../services/validation";

// Type definitions for route params
interface GetGiftcardRefuelParams {
  giftcard_id: string;
}

// Helper function for API response
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

// Helper function to validate CreateGiftcardRefuelRequestBody
function validateCreateGiftcardRefuelRequest(
  body: CreateGiftcardRefuelRequestBody
): { valid: boolean; error?: string } {
  if (body.gas_cycles_included < 1_000_000_000_000) {
    return {
      valid: false,
      error: "Gas cycles included must be greater than 1T",
    };
  }
  if (body.action !== "CREATE") {
    return { valid: false, error: "Action must be 'CREATE'" };
  }
  return { valid: true };
}

// Helper function to validate UpdateGiftcardRefuelRequestBody
function validateUpdateGiftcardRefuelRequest(
  body: UpdateGiftcardRefuelRequestBody
): { valid: boolean; error?: string } {
  if (!isValidID(IDPrefixEnum.GiftcardRefuel, body.id)) {
    return { valid: false, error: "Invalid GiftcardRefuel ID" };
  }
  if (!body.id.startsWith(IDPrefixEnum.GiftcardRefuel)) {
    return {
      valid: false,
      error: `GiftcardRefuel ID must start with '${IDPrefixEnum.GiftcardRefuel}'`,
    };
  }
  if (body.action !== "UPDATE") {
    return { valid: false, error: "Action must be 'UPDATE'" };
  }
  if (
    body.gas_cycles_included !== undefined &&
    body.gas_cycles_included < 1_000_000_000_000
  ) {
    return {
      valid: false,
      error: "Gas cycles included must be greater than 1T",
    };
  }
  return { valid: true };
}

// Helper function to validate DeleteGiftcardRefuelRequestBody
function validateDeleteGiftcardRefuelRequest(
  body: DeleteGiftcardRefuelRequestBody
): { valid: boolean; error?: string } {
  if (!isValidID(IDPrefixEnum.GiftcardRefuel, body.id)) {
    return { valid: false, error: "Invalid GiftcardRefuel ID" };
  }
  if (!body.id.startsWith(IDPrefixEnum.GiftcardRefuel)) {
    return {
      valid: false,
      error: `GiftcardRefuel ID must start with '${IDPrefixEnum.GiftcardRefuel}'`,
    };
  }
  return { valid: true };
}

// Helper function to validate RedeemGiftcardRefuelData
function validateRedeemGiftcardRefuelRequest(body: RedeemGiftcardRefuelData): {
  valid: boolean;
  error?: string;
} {
  if (!body.giftcard_id.startsWith(IDPrefixEnum.GiftcardRefuel)) {
    return {
      valid: false,
      error: `GiftcardRefuel ID must start with '${IDPrefixEnum.GiftcardRefuel}'`,
    };
  }
  if (!validateIcpPrincipal(body.icp_principal)) {
    return { valid: false, error: "Invalid ICP principal" };
  }
  return { valid: true };
}

// Helper to format UserID (mimics Rust's format_user_id)
function formatUserId(principal: string): string {
  // In a real TS environment, this would involve more complex logic
  // to convert a principal string to your UserID format.
  // For this simulation, we'll keep it simple as a direct mapping.
  return `UserID_${principal.substring(0, 8)}`; // Example: UserID_xxxxx...
}

// Placeholder for `deposit_cycles` - this would be an inter-canister call in a real IC setup.
// Here, it's simulated.
async function depositCycles(
  recipientPrincipal: string,
  amount: number
): Promise<void> {
  console.log(
    `Simulating deposit of ${amount} cycles to ${recipientPrincipal}`
  );
  await new Promise((resolve) => setTimeout(resolve, 50)); // Simulate network latency
  console.log(`Successfully deposited cycles to ${recipientPrincipal}`);
}

export async function getGiftcardRefuelHandler(
  request: FastifyRequest<{ Params: GetGiftcardRefuelParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const requesterApiKey = await authenticateRequest(request, "factory");
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const requestedId = request.params.giftcard_id;

    const giftcards = await db.queryFactory(
      "SELECT * FROM giftcard_refuels WHERE id = ?",
      [requestedId]
    );

    if (!giftcards || giftcards.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "GiftcardRefuel not found",
        })
      );
    }

    const giftcard = giftcards[0] as GiftcardRefuel;
    const ownerId = request.server.factory_owner;
    const isOwner = requesterApiKey.user_id === ownerId;

    if (!isOwner) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    return reply.status(200).send(createApiResponse(giftcard));
  } catch (error) {
    request.log.error("Error in getGiftcardRefuelHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function listGiftcardRefuelsHandler(
  request: FastifyRequest<{ Body: ListGiftcardRefuelsRequestBody }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const requesterApiKey = await authenticateRequest(request, "factory");
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = request.server.factory_owner === requesterApiKey.user_id;
    if (!isOwner) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const body = request.body;
    const validation = validateListGiftcardRefuelsRequest(body);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    const pageSize = body.page_size ?? 50;
    const direction = body.direction ?? SortDirection.ASC;
    const filters = body.filters || "";

    let offset = 0;
    if (body.cursor) {
      offset = parseInt(body.cursor, 10);
      if (isNaN(offset) || offset < 0) {
        return reply.status(400).send(
          createApiResponse(undefined, {
            code: 400,
            message: "Invalid cursor format",
          })
        );
      }
    }

    const totalResult = await db.queryFactory(
      `SELECT COUNT(*) as count FROM giftcard_refuels WHERE note LIKE ?`,
      [`%${filters}%`]
    );
    const total = totalResult[0]?.count || 0;

    if (total === 0) {
      return reply.status(200).send(
        createApiResponse({
          items: [],
          page_size: 0,
          total: 0,
          direction: direction,
          cursor: null,
        })
      );
    }

    let query = `SELECT * FROM giftcard_refuels WHERE note LIKE ?`;
    query += ` ORDER BY timestamp_ms ${direction}`;
    query += ` LIMIT ? OFFSET ?`;

    const giftcards = await db.queryFactory(query, [
      `%${filters}%`,
      pageSize,
      offset,
    ]);

    const nextCursor =
      offset + giftcards.length < total
        ? (offset + giftcards.length).toString()
        : null;

    return reply.status(200).send(
      createApiResponse<ListGiftcardRefuelsResponseData>({
        items: giftcards as GiftcardRefuel[],
        page_size: giftcards.length,
        total: total,
        direction: direction,
        cursor: nextCursor,
      })
    );
  } catch (error) {
    request.log.error("Error in listGiftcardRefuelsHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

// Helper function to validate ListGiftcardRefuelsRequestBody
function validateListGiftcardRefuelsRequest(
  body: ListGiftcardRefuelsRequestBody
): { valid: boolean; error?: string } {
  if (body.filters && body.filters.length > 256) {
    return { valid: false, error: "Filters must be 256 characters or less" };
  }
  if (
    body.page_size !== undefined &&
    (body.page_size === 0 || body.page_size > 1000)
  ) {
    return { valid: false, error: "Page size must be between 1 and 1000" };
  }
  if (body.cursor && body.cursor.length > 256) {
    return { valid: false, error: "Cursor must be 256 characters or less" };
  }
  return { valid: true };
}

export async function upsertGiftcardRefuelHandler(
  request: FastifyRequest<{ Body: UpsertGiftcardRefuelRequestBody }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const requesterApiKey = await authenticateRequest(request, "factory");
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = request.server.factory_owner === requesterApiKey.user_id;
    if (!isOwner) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const body = request.body;

    if (body.action === "CREATE") {
      const createBody = body as CreateGiftcardRefuelRequestBody;
      const validation = validateCreateGiftcardRefuelRequest(createBody);
      if (!validation.valid) {
        return reply.status(400).send(
          createApiResponse(undefined, {
            code: 400,
            message: validation.error!,
          })
        );
      }

      const newGiftcard: GiftcardRefuel = {
        id: `${IDPrefixEnum.GiftcardRefuel}${uuidv4()}`,
        usd_revenue_cents: createBody.usd_revenue_cents,
        note: createBody.note,
        gas_cycles_included: createBody.gas_cycles_included,
        timestamp_ms: Date.now(),
        external_id: createBody.external_id,
        redeemed: false,
      };

      await dbHelpers.transaction("factory", null, (database) => {
        const stmt = database.prepare(
          `INSERT INTO giftcard_refuels (id, usd_revenue_cents, note, gas_cycles_included, timestamp_ms, external_id, redeemed)
           VALUES (?, ?, ?, ?, ?, ?, ?)`
        );
        stmt.run(
          newGiftcard.id,
          newGiftcard.usd_revenue_cents,
          newGiftcard.note,
          newGiftcard.gas_cycles_included,
          newGiftcard.timestamp_ms,
          newGiftcard.external_id,
          newGiftcard.redeemed ? 1 : 0
        );

        // Link to owner in user_giftcard_refuels
        const userGiftcardStmt = database.prepare(
          `INSERT INTO user_giftcard_refuels (user_id, giftcard_id) VALUES (?, ?)`
        );
        userGiftcardStmt.run(requesterApiKey.user_id, newGiftcard.id);
      });

      return reply.status(200).send(createApiResponse(newGiftcard));
    } else if (body.action === "UPDATE") {
      const updateBody = body as UpdateGiftcardRefuelRequestBody;
      const validation = validateUpdateGiftcardRefuelRequest(updateBody);
      if (!validation.valid) {
        return reply.status(400).send(
          createApiResponse(undefined, {
            code: 400,
            message: validation.error!,
          })
        );
      }

      const giftcards = await db.queryFactory(
        "SELECT * FROM giftcard_refuels WHERE id = ?",
        [updateBody.id]
      );
      if (!giftcards || giftcards.length === 0) {
        return reply.status(404).send(
          createApiResponse(undefined, {
            code: 404,
            message: "GiftcardRefuel not found",
          })
        );
      }
      let giftcardToUpdate = giftcards[0] as GiftcardRefuel;

      const updates: string[] = [];
      const values: any[] = [];

      if (updateBody.note !== undefined) {
        updates.push("note = ?");
        values.push(updateBody.note);
        giftcardToUpdate.note = updateBody.note;
      }
      if (updateBody.usd_revenue_cents !== undefined) {
        updates.push("usd_revenue_cents = ?");
        values.push(updateBody.usd_revenue_cents);
        giftcardToUpdate.usd_revenue_cents = updateBody.usd_revenue_cents;
      }
      if (updateBody.gas_cycles_included !== undefined) {
        updates.push("gas_cycles_included = ?");
        values.push(updateBody.gas_cycles_included);
        giftcardToUpdate.gas_cycles_included = updateBody.gas_cycles_included;
      }
      if (updateBody.external_id !== undefined) {
        updates.push("external_id = ?");
        values.push(updateBody.external_id);
        giftcardToUpdate.external_id = updateBody.external_id;
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

      await dbHelpers.transaction("factory", null, (database) => {
        const stmt = database.prepare(
          `UPDATE giftcard_refuels SET ${updates.join(", ")} WHERE id = ?`
        );
        stmt.run(...values);
      });

      return reply.status(200).send(createApiResponse(giftcardToUpdate));
    } else {
      return reply
        .status(400)
        .send(
          createApiResponse(undefined, { code: 400, message: "Invalid action" })
        );
    }
  } catch (error) {
    request.log.error("Error in upsertGiftcardRefuelHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function deleteGiftcardRefuelHandler(
  request: FastifyRequest<{ Body: DeleteGiftcardRefuelRequestBody }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const requesterApiKey = await authenticateRequest(request, "factory");
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = request.server.factory_owner === requesterApiKey.user_id;
    if (!isOwner) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const body = request.body;
    const validation = validateDeleteGiftcardRefuelRequest(body);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    const giftcards = await db.queryFactory(
      "SELECT * FROM giftcard_refuels WHERE id = ?",
      [body.id]
    );
    if (!giftcards || giftcards.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "GiftcardRefuel not found",
        })
      );
    }

    await dbHelpers.transaction("factory", null, (database) => {
      const stmt = database.prepare(
        "DELETE FROM giftcard_refuels WHERE id = ?"
      );
      stmt.run(body.id);

      const userGiftcardStmt = database.prepare(
        "DELETE FROM user_giftcard_refuels WHERE giftcard_id = ?"
      );
      userGiftcardStmt.run(body.id);
    });

    return reply.status(200).send(
      createApiResponse<DeletedGiftcardRefuelData>({
        id: body.id,
        deleted: true,
      })
    );
  } catch (error) {
    request.log.error("Error in deleteGiftcardRefuelHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function redeemGiftcardRefuelHandler(
  request: FastifyRequest<{ Body: RedeemGiftcardRefuelData }>,
  reply: FastifyReply
): Promise<void> {
  try {
    // Note: Rust code has no authentication for redeem. If needed, add it here.

    const body = request.body;
    const validation = validateRedeemGiftcardRefuelRequest(body);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    const giftcards = await db.queryFactory(
      "SELECT * FROM giftcard_refuels WHERE id = ?",
      [body.giftcard_id]
    );
    if (!giftcards || giftcards.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "GiftcardRefuel not found",
        })
      );
    }

    let giftcard = giftcards[0] as GiftcardRefuel;

    if (giftcard.redeemed) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "GiftcardRefuel already redeemed",
        })
      );
    }

    const redeemCode = `REDEEM_${Date.now()}`;
    const currentTime = Date.now();
    const userId = formatUserId(body.icp_principal);

    try {
      await depositCycles(body.icp_principal, giftcard.gas_cycles_included);

      // Update giftcard as redeemed
      giftcard.redeemed = true;
      await dbHelpers.transaction("factory", null, (database) => {
        database
          .prepare(`UPDATE giftcard_refuels SET redeemed = 1 WHERE id = ?`)
          .run(giftcard.id);

        // Store redemption history
        const historyRecord: FactoryRefuelHistoryRecord = {
          id: null as any, // Auto-incremented
          note: `Redeemed giftcard ${giftcard.id} by ${userId}, deposited ${giftcard.gas_cycles_included} cycles into principal ${body.icp_principal}`,
          giftcard_id: giftcard.id,
          gas_cycles_included: giftcard.gas_cycles_included,
          timestamp_ms: currentTime,
          icp_principal: body.icp_principal,
        };
        database
          .prepare(
            `INSERT INTO factory_refuel_history (note, giftcard_id, gas_cycles_included, timestamp_ms, icp_principal)
             VALUES (?, ?, ?, ?, ?)`
          )
          .run(
            historyRecord.note,
            historyRecord.giftcard_id,
            historyRecord.gas_cycles_included,
            historyRecord.timestamp_ms,
            historyRecord.icp_principal
          );

        // Add to user_giftcard_refuels (if not already linked)
        database
          .prepare(
            `INSERT OR IGNORE INTO user_giftcard_refuels (user_id, giftcard_id) VALUES (?, ?)`
          )
          .run(userId, giftcard.id);
      });

      return reply.status(200).send(
        createApiResponse<RedeemGiftcardRefuelResult>({
          giftcard_id: giftcard.id,
          icp_principal: body.icp_principal,
          redeem_code: redeemCode,
          timestamp_ms: currentTime,
        })
      );
    } catch (depositError: any) {
      request.log.error("Error depositing cycles:", depositError);
      return reply.status(500).send(
        createApiResponse(undefined, {
          code: 500,
          message: `Failed to deposit cycles: ${depositError.message || depositError}`,
        })
      );
    }
  } catch (error) {
    request.log.error("Error in redeemGiftcardRefuelHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}
