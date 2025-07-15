import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import {
  ApiResponse,
  CreateGiftcardSpawnOrgRequestBody,
  UpdateGiftcardSpawnOrgRequestBody,
  UpsertGiftcardSpawnOrgRequestBody,
  DeleteGiftcardSpawnOrgRequestBody,
  DeletedGiftcardSpawnOrgData,
  RedeemGiftcardSpawnOrgData,
  GiftcardSpawnOrg,
  RedeemGiftcardSpawnOrgResult,
  FactorySpawnHistoryRecord,
  SpawnInitArgs,
  ListGiftcardSpawnOrgsRequestBody,
  ListGiftcardSpawnOrgsResponseData,
  SortDirection,
  IDPrefixEnum,
  DriveID,
  GenerateID,
  GroupInviteeTypeEnum,
  GroupRole,
} from "@officexapp/types";
import { db, dbHelpers, initDriveDB } from "../../../../services/database";
import { authenticateRequest } from "../../../../services/auth";
import { isValidID } from "../../../../api/helpers";
import { generateMnemonic, mnemonicToSeed } from "bip39";
import { getPublicKeyAsync } from "@noble/ed25519";
import { Principal } from "@dfinity/principal";
import { validateIcpPrincipal } from "../../../../services/validation";
import { FREE_MODE } from "../../../../constants";

// Type definitions for route params
interface GetGiftcardSpawnOrgParams {
  giftcard_id: string;
}

// Helper function for API response
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

// Helper function to validate CreateGiftcardSpawnOrgRequestBody
function validateCreateGiftcardSpawnOrgRequest(
  body: CreateGiftcardSpawnOrgRequestBody
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

// Helper function to validate UpdateGiftcardSpawnOrgRequestBody
function validateUpdateGiftcardSpawnOrgRequest(
  body: UpdateGiftcardSpawnOrgRequestBody
): { valid: boolean; error?: string } {
  if (!isValidID(IDPrefixEnum.GiftcardSpawnOrg, body.id)) {
    return { valid: false, error: "Invalid GiftcardSpawnOrg ID" };
  }
  if (!body.id.startsWith(IDPrefixEnum.GiftcardSpawnOrg)) {
    return {
      valid: false,
      error: `GiftcardSpawnOrg ID must start with '${IDPrefixEnum.GiftcardSpawnOrg}'`,
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

// Helper function to validate DeleteGiftcardSpawnOrgRequestBody
function validateDeleteGiftcardSpawnOrgRequest(
  body: DeleteGiftcardSpawnOrgRequestBody
): { valid: boolean; error?: string } {
  if (!isValidID(IDPrefixEnum.GiftcardSpawnOrg, body.id)) {
    return { valid: false, error: "Invalid GiftcardSpawnOrg ID" };
  }
  if (!body.id.startsWith(IDPrefixEnum.GiftcardSpawnOrg)) {
    return {
      valid: false,
      error: `GiftcardSpawnOrg ID must start with '${IDPrefixEnum.GiftcardSpawnOrg}'`,
    };
  }
  return { valid: true };
}

// Helper function to validate RedeemGiftcardSpawnOrgData
function validateRedeemGiftcardSpawnOrgRequest(
  body: RedeemGiftcardSpawnOrgData
): { valid: boolean; error?: string } {
  if (!body.giftcard_id.startsWith(IDPrefixEnum.GiftcardSpawnOrg)) {
    return {
      valid: false,
      error: `GiftcardSpawnOrg ID must start with '${IDPrefixEnum.GiftcardSpawnOrg}'`,
    };
  }
  if (!validateIcpPrincipal(body.owner_icp_principal)) {
    return { valid: false, error: "Invalid ICP principal" };
  }
  if (
    body.organization_name !== undefined &&
    (body.organization_name.trim().length === 0 ||
      body.organization_name.length > 64)
  ) {
    return {
      valid: false,
      error:
        "Organization name cannot be empty and must be 64 characters or less",
    };
  }
  if (
    body.owner_name !== undefined &&
    (body.owner_name.trim().length === 0 || body.owner_name.length > 64)
  ) {
    return {
      valid: false,
      error: "Owner name cannot be empty and must be 64 characters or less",
    };
  }
  return { valid: true };
}

// Helper to determine URL endpoint (mimics Rust's get_appropriate_url_endpoint)
export function getAppropriateUrlEndpoint(request: FastifyRequest): string {
  if (process.env.SERVER_DOMAIN) {
    // If SERVER_DOMAIN is provided, use it directly without port
    // Ensure it's a valid URL base, default to https if not specified
    let domain = process.env.SERVER_DOMAIN;
    if (!domain.startsWith("http://") && !domain.startsWith("https://")) {
      domain = `https://${domain}`; // Assume HTTPS if not explicitly provided
    }
    // Append the driveId as a path parameter. Ensure no double slashes.
    return `${domain.endsWith("/") ? domain.slice(0, -1) : domain}`;
  } else {
    // Fallback to dynamic detection for local/dev environments
    // LOCAL_DEV_MODE
    const protocol = request.protocol;
    const hostname = request.hostname;
    // For local dev, rely on process.env.PORT which Fastify often binds to,
    // or a sensible default.
    const port = process.env.PORT || 8888;

    // If it's localhost, include the port
    if (hostname.includes("localhost") || hostname.includes("127.0.0.1")) {
      return `${protocol}://${hostname.split(":")[0]}:${port}`;
    } else {
      // For other hostnames (e.g., custom domains in dev, without SERVER_DOMAIN set)
      // We assume it's a standard web setup where port 80/443 is implied
      return `${protocol}://${hostname}`;
    }
  }
}

async function generateIcpPrincipalForDrive(): Promise<string> {
  const mnemonic = generateMnemonic(128); // Generate a 12-word mnemonic
  const seedBuffer = await mnemonicToSeed(mnemonic);
  const seedBytes = new Uint8Array(seedBuffer);

  // Derive Ed25519 private key from the seed (first 32 bytes)
  const ed25519PrivateKey = seedBytes.slice(0, 32);

  // Derive the Ed25519 public key
  const ed25519PublicKey = await getPublicKeyAsync(ed25519PrivateKey);

  // Convert raw public key to DER format for Principal generation
  const derHeader = new Uint8Array([
    0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
  ]);
  const derKey = new Uint8Array(derHeader.length + ed25519PublicKey.length);
  derKey.set(derHeader);
  derKey.set(ed25519PublicKey, derHeader.length);

  // Compute the self-authenticating principal
  const principal = Principal.selfAuthenticating(derKey);
  return principal.toText();
}

// Placeholder for `deploy_drive_canister` - this would be an inter-canister call in a real IC setup.
// Here, it's simulated to return a dummy canister ID.
async function deployDriveCanister(
  ownerIcpPrincipal: string,
  title: string | undefined,
  ownerName: string | undefined,
  spawnRedeemCode: string,
  note: string | undefined,
  cycles: number
): Promise<string> {
  const generatedCanisterId = await generateIcpPrincipalForDrive();
  return generatedCanisterId;
}

export async function getGiftcardSpawnOrgHandler(
  request: FastifyRequest<{ Params: GetGiftcardSpawnOrgParams }>,
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
      "SELECT * FROM giftcard_spawn_orgs WHERE id = ?",
      [requestedId]
    );

    if (!giftcards || giftcards.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "GiftcardSpawnOrg not found",
        })
      );
    }

    const giftcard = giftcards[0] as GiftcardSpawnOrg;
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
    request.log.error("Error in getGiftcardSpawnOrgHandler:", error);
    console.log(error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function listGiftcardSpawnOrgsHandler(
  request: FastifyRequest<{ Body: ListGiftcardSpawnOrgsRequestBody }>,
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

    console.log(
      `isOwner: ${isOwner}, ownerId: ${request.server.factory_owner}, requesterApiKey.user_id: ${requesterApiKey.user_id}`
    );

    if (!isOwner) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const body = request.body;
    const validation = validateListGiftcardSpawnOrgsRequest(body);
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
      // In a real scenario, the cursor would need to be securely encoded/decoded
      // and represent a specific point in the sorted data. For SQLite with OFFSET,
      // it's simply the offset.
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

    // Get total count (before filtering and pagination)
    const totalResult = await db.queryFactory(
      `SELECT COUNT(*) as count FROM giftcard_spawn_orgs WHERE note LIKE ?`,
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

    let query = `SELECT * FROM giftcard_spawn_orgs WHERE note LIKE ?`;
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
      createApiResponse<ListGiftcardSpawnOrgsResponseData>({
        items: giftcards as GiftcardSpawnOrg[],
        page_size: giftcards.length,
        total: total,
        direction: direction,
        cursor: nextCursor,
      })
    );
  } catch (error) {
    request.log.error("Error in listGiftcardSpawnOrgsHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

// Helper function to validate ListGiftcardSpawnOrgsRequestBody
function validateListGiftcardSpawnOrgsRequest(
  body: ListGiftcardSpawnOrgsRequestBody
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

export async function upsertGiftcardSpawnOrgHandler(
  request: FastifyRequest<{ Body: UpsertGiftcardSpawnOrgRequestBody }>,
  reply: FastifyReply
): Promise<void> {
  try {
    let userId: string;
    if (FREE_MODE) {
      userId = "Free_Mode_Anonymous_User";
    } else {
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
      userId = requesterApiKey.user_id;
    }

    const body = request.body;

    if (body.action === "CREATE") {
      const createBody = body as CreateGiftcardSpawnOrgRequestBody;
      const validation = validateCreateGiftcardSpawnOrgRequest(createBody);
      if (!validation.valid) {
        return reply.status(400).send(
          createApiResponse(undefined, {
            code: 400,
            message: validation.error!,
          })
        );
      }

      const newGiftcard: GiftcardSpawnOrg = {
        id: `${IDPrefixEnum.GiftcardSpawnOrg}${uuidv4()}`,
        usd_revenue_cents: createBody.usd_revenue_cents,
        note: createBody.note,
        gas_cycles_included: createBody.gas_cycles_included,
        timestamp_ms: Date.now(),
        external_id: createBody.external_id,
        redeemed: false,
        disk_auth_json: createBody.disk_auth_json,
      };

      await dbHelpers.transaction("factory", null, (database) => {
        const stmt = database.prepare(
          `INSERT INTO giftcard_spawn_orgs (id, usd_revenue_cents, note, gas_cycles_included, timestamp_ms, external_id, redeemed, disk_auth_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
        );
        stmt.run(
          newGiftcard.id,
          newGiftcard.usd_revenue_cents,
          newGiftcard.note,
          newGiftcard.gas_cycles_included,
          newGiftcard.timestamp_ms,
          newGiftcard.external_id,
          newGiftcard.redeemed ? 1 : 0,
          newGiftcard.disk_auth_json || null
        );

        // Link to owner in user_giftcard_spawn_orgs
        const userGiftcardStmt = database.prepare(
          `INSERT INTO user_giftcard_spawn_orgs (user_id, giftcard_id) VALUES (?, ?)`
        );

        userGiftcardStmt.run(userId, newGiftcard.id);
      });

      return reply.status(200).send(createApiResponse(newGiftcard));
    } else if (body.action === "UPDATE") {
      const updateBody = body as UpdateGiftcardSpawnOrgRequestBody;
      const validation = validateUpdateGiftcardSpawnOrgRequest(updateBody);
      if (!validation.valid) {
        return reply.status(400).send(
          createApiResponse(undefined, {
            code: 400,
            message: validation.error!,
          })
        );
      }

      const giftcards = await db.queryFactory(
        "SELECT * FROM giftcard_spawn_orgs WHERE id = ?",
        [updateBody.id]
      );
      if (!giftcards || giftcards.length === 0) {
        return reply.status(404).send(
          createApiResponse(undefined, {
            code: 404,
            message: "GiftcardSpawnOrg not found",
          })
        );
      }
      let giftcardToUpdate = giftcards[0] as GiftcardSpawnOrg;

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
      if (updateBody.disk_auth_json !== undefined) {
        updates.push("disk_auth_json = ?");
        values.push(updateBody.disk_auth_json);
        giftcardToUpdate.disk_auth_json = updateBody.disk_auth_json;
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
          `UPDATE giftcard_spawn_orgs SET ${updates.join(", ")} WHERE id = ?`
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
    request.log.error("Error in upsertGiftcardSpawnOrgHandler:", error);
    console.log(error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function deleteGiftcardSpawnOrgHandler(
  request: FastifyRequest<{ Body: DeleteGiftcardSpawnOrgRequestBody }>,
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
    const validation = validateDeleteGiftcardSpawnOrgRequest(body);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    const giftcards = await db.queryFactory(
      "SELECT * FROM giftcard_spawn_orgs WHERE id = ?",
      [body.id]
    );
    if (!giftcards || giftcards.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "GiftcardSpawnOrg not found",
        })
      );
    }

    await dbHelpers.transaction("factory", null, (database) => {
      const stmt = database.prepare(
        "DELETE FROM giftcard_spawn_orgs WHERE id = ?"
      );
      stmt.run(body.id);

      const userGiftcardStmt = database.prepare(
        "DELETE FROM user_giftcard_spawn_orgs WHERE giftcard_id = ?"
      );
      userGiftcardStmt.run(body.id);
    });

    return reply.status(200).send(
      createApiResponse<DeletedGiftcardSpawnOrgData>({
        id: body.id,
        deleted: true,
      })
    );
  } catch (error) {
    request.log.error("Error in deleteGiftcardSpawnOrgHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function redeemGiftcardSpawnOrgHandler(
  request: FastifyRequest<{ Body: RedeemGiftcardSpawnOrgData }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const body = request.body;
    const validation = validateRedeemGiftcardSpawnOrgRequest(body);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    const giftcards = await db.queryFactory(
      "SELECT * FROM giftcard_spawn_orgs WHERE id = ?",
      [body.giftcard_id]
    );
    if (!giftcards || giftcards.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "GiftcardSpawnOrg not found",
        })
      );
    }

    let giftcard = giftcards[0] as GiftcardSpawnOrg;

    if (giftcard.redeemed) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "GiftcardSpawnOrg already redeemed",
        })
      );
    }

    const redeemCode = `REDEEM_${Date.now()}`;
    const ownerId = `UserID_${body.owner_icp_principal}`;
    const currentTime = Date.now();
    const noteForSpawn = `giftcard ${body.giftcard_id} was redeemed to spawn drive with ${giftcard.gas_cycles_included} cycles, owned by ${ownerId}, on timestamp_ms ${currentTime} ${new Date(currentTime).toISOString()}`;

    // Simulate canister deployment to get a dummy canister ID
    const deployedCanisterId = await deployDriveCanister(
      body.owner_icp_principal,
      body.organization_name,
      body.owner_name,
      redeemCode,
      noteForSpawn,
      giftcard.gas_cycles_included
    );

    const endpoint = getAppropriateUrlEndpoint(request);
    const driveId = `DriveID_${deployedCanisterId}`;

    // --- Start: New Drive DB Creation and Initialization ---
    // 1. Initialize the new drive's SQLite database
    await initDriveDB(driveId);

    // 2. Insert into the new drive's 'about_drive' table
    // Using dbHelpers.transaction for atomicity on the new drive's DB
    await dbHelpers.transaction("drive", driveId, (driveDatabase) => {
      const version = request.server.officex_version; // Get version from env
      const driveStateChecksum = "genesis"; // Initial checksum
      const driveStateTimestampNs = BigInt(currentTime) * 1_000_000n; // Convert ms to ns

      const groupID = GenerateID.Group(); // This needs to be defined BEFORE the about_drive insert

      const insertAboutDriveStmt = driveDatabase.prepare(
        `INSERT INTO about_drive (
            drive_id, drive_name, canister_id, version, drive_state_checksum,
            drive_state_timestamp_ns, owner_id, url_endpoint,
            transfer_owner_id, spawn_redeem_code, spawn_note,
            nonce_uuid_generated, default_everyone_group_id -- Add this column
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)` // Add a placeholder for the new column
      );
      insertAboutDriveStmt.run(
        driveId,
        body.organization_name,
        deployedCanisterId,
        version,
        driveStateChecksum,
        driveStateTimestampNs.toString(),
        ownerId,
        endpoint,
        "",
        redeemCode,
        noteForSpawn,
        0,
        groupID // Pass the groupID here
      );

      // Optionally, create the owner contact in the new drive's DB
      // This mimics the 'init_self_drive' logic in Rust's state.rs
      const insertContactStmt = driveDatabase.prepare(
        `INSERT INTO contacts (
            id, name, avatar, email, notifications_url, public_note,
            private_note, evm_public_address, icp_principal, seed_phrase,
            from_placeholder_user_id, redeem_code, created_at, last_online_ms,
            external_id, external_payload
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );

      insertContactStmt.run(
        ownerId,
        "Owner", // Use owner_name or a default
        null, // avatar
        null, // email
        null, // notifications_url
        null, // public_note
        null, // private_note
        "", // evm_public_address - replace with actual derivation if needed
        body.owner_icp_principal,
        null, // seed_phrase
        null, // from_placeholder_user_id
        redeemCode, // redeem_code for the owner if tied to spawn
        currentTime,
        currentTime,
        null, // external_id
        null // external_payload
      );

      // add drive to drive table

      const insertDriveStmt = driveDatabase.prepare(
        `INSERT INTO drives (
            id, name, icp_principal, public_note, private_note, endpoint_url, last_indexed_ms, created_at, external_id, external_payload
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );
      insertDriveStmt.run(
        driveId,
        body.organization_name,
        deployedCanisterId, // canister_id for `about_drive`
        null,
        null,
        endpoint,
        null,
        currentTime,
        null,
        null
      );

      // create "default everyone" group with owner as admin (include the group_invites)

      const insertGroupStmt = driveDatabase.prepare(
        `INSERT INTO groups (
            id, name, owner_user_id, avatar, public_note, private_note,
            created_at, last_modified_at, drive_id, endpoint_url,
            external_id, external_payload
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );
      insertGroupStmt.run(
        groupID,
        "Default Everyone",
        ownerId,
        null,
        null,
        null,
        currentTime,
        currentTime,
        driveId,
        endpoint,
        null,
        null
      );

      const insertContactGroupStmt = driveDatabase.prepare(
        `INSERT INTO contact_groups (
            user_id, group_id, role
          ) VALUES (?, ?, ?)`
      );
      insertContactGroupStmt.run(
        ownerId, // The owner_user_id is the user_id for this junction
        groupID, // The ID of the group just created
        GroupRole.ADMIN // The owner is an ADMIN of this group
      );

      // create group invites
      const inviteID = GenerateID.GroupInvite();
      const insertGroupInviteStmt = driveDatabase.prepare(
        `INSERT INTO group_invites (
            id, group_id, inviter_id, invitee_type, invitee_id, role, note,
            active_from, expires_at, created_at, last_modified_at, redeem_code,
            from_placeholder_invitee, external_id, external_payload
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );
      insertGroupInviteStmt.run(
        inviteID,
        groupID,
        ownerId,
        GroupInviteeTypeEnum.USER,
        ownerId,
        GroupRole.ADMIN,
        noteForSpawn,
        currentTime,
        currentTime,
        currentTime,
        currentTime,
        redeemCode,
        null,
        null,
        null
      );
    });
    // --- End: New Drive DB Creation and Initialization ---

    // Update giftcard as redeemed in the factory DB
    giftcard.redeemed = true;
    await dbHelpers.transaction("factory", null, (database) => {
      database
        .prepare(`UPDATE giftcard_spawn_orgs SET redeemed = 1 WHERE id = ?`)
        .run(giftcard.id);

      // Store deployment history in the factory DB
      const historyRecord: FactorySpawnHistoryRecord = {
        id: null as any, // Auto-incremented
        owner_id: ownerId,
        drive_id: driveId,
        endpoint: endpoint,
        version: process.env.VERSION || "1.0.0", // Assuming version from env
        note: giftcard.note,
        giftcard_id: giftcard.id,
        gas_cycles_included: giftcard.gas_cycles_included,
        timestamp_ms: currentTime,
      };
      database
        .prepare(
          `INSERT INTO factory_spawn_history (owner_id, drive_id, endpoint, version, note, giftcard_id, gas_cycles_included, timestamp_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
        )
        .run(
          historyRecord.owner_id,
          historyRecord.drive_id,
          historyRecord.endpoint,
          historyRecord.version,
          historyRecord.note,
          historyRecord.giftcard_id,
          historyRecord.gas_cycles_included,
          historyRecord.timestamp_ms
        );

      // Link to owner in user_giftcard_spawn_orgs (if not already linked)
      database
        .prepare(
          `INSERT OR IGNORE INTO user_giftcard_spawn_orgs (user_id, giftcard_id) VALUES (?, ?)`
        )
        .run(ownerId, giftcard.id);
    });

    return reply.status(200).send(
      createApiResponse<RedeemGiftcardSpawnOrgResult>({
        owner_id: ownerId,
        drive_id: driveId,
        endpoint: endpoint,
        redeem_code: redeemCode,
        disk_auth_json: giftcard.disk_auth_json,
      })
    );
  } catch (error) {
    request.log.error("Error in redeemGiftcardSpawnOrgHandler:", error);
    console.log(error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}
