import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import {
  FactoryApiResponse,
  IRequestReplayDrive,
  SearchDriveRequestBody,
  IResponseReplayDrive,
  IResponseSearchDrive,
  IResponseReindexDrive,
  IRequestReindexDrive,
  IRequestTransferDriveOwnership,
  IResponseTransferDriveOwnership,
  IRequestSuperswapUser,
  IResponseSuperswapUser,
  IRequestRedeemOrg,
  IResponseRedeemOrg,
  IRequestInboxOrg,
  IResponseInboxOrg,
  IResponseWhoAmI,
  ExternalIDsDriveRequestBody, // Assuming a new type for this request body
  ExternalIDsDriveResponse, // Assuming a new type for this response
  ExternalIDvsInternalIDMap, // Assuming this type is available
  AboutDriveResponseData,
  SearchCategoryEnum,
  SortDirection, // Assuming this type is available
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import { authenticateRequest, generateApiKey } from "../../../../services/auth";
import { DriveID, UserID, IDPrefixEnum } from "@officexapp/types"; // Import necessary types
import { OrgIdParams } from "../../types";

// TODO: Replace with actual database interactions and business logic for the new services
// These placeholders return mock data or simplified operations.

// Helper for consistent API response structure
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

// TODO: Implement getOwnerId and isLocalEnvironment based on your TS application's structure
// For now, these are placeholders.
async function getOwnerId(driveId: DriveID): Promise<UserID> {
  // In a multi-tenant setup, the owner ID would likely be stored in the drive's specific DB.
  // For now, returning a mock owner.
  // TODO: Implement actual owner retrieval from the 'about_drive' table
  const result = await db.queryDrive(
    driveId,
    `SELECT owner_id FROM about_drive LIMIT 1`
  );
  if (result.length > 0) {
    return result[0].owner_id as UserID;
  }
  return "UserID_mock_owner" as UserID; // Placeholder
}

function isLocalEnvironment(): boolean {
  // TODO: Implement logic to determine if the environment is local (e.g., check process.env.NODE_ENV)
  return (
    process.env.NODE_ENV === "development" || process.env.NODE_ENV === "test"
  );
}

/**
 * Handles the /organization/about route.
 * Returns information about the current drive/organization.
 */
export async function aboutDriveHandler(
  request: FastifyRequest<{ Params: OrgIdParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const ownerId = await getOwnerId(org_id); // Get the actual owner ID for the drive
    const isOwner = requesterApiKey.user_id === ownerId;

    // TODO: Implement permission checks based on your `check_system_permissions` equivalent in TS.
    // For now, simplified: only owner or users with 'VIEW' on 'DRIVES' table/record can access.
    let hasDrivePermission = isOwner;
    if (!hasDrivePermission) {
      // In a real scenario, you'd query the permissions_system table for the drive.
      // For now, assuming if not owner, access is restricted unless specific VIEW permission.
      // TODO: Replace with actual permission service call if available in TS.
      // Example: const userHasViewPermission = await permissionService.checkSystemPermission(org_id, requesterApiKey.user_id, SystemPermissionType.VIEW);
      // hasDrivePermission = userHasViewPermission;
      request.log.warn(
        `User ${requesterApiKey.user_id} is not owner of ${org_id}. Assuming no view permission without explicit check.`
      );
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    // Retrieve data from SQLite `about_drive` table
    const result = await db.queryDrive(
      org_id,
      `SELECT drive_id, drive_name, canister_id, version, drive_state_checksum,
              drive_state_timestamp_ns, owner_id, url_endpoint, transfer_owner_id,
              spawn_redeem_code, spawn_note, nonce_uuid_generated
       FROM about_drive LIMIT 1`
    );

    if (result.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Drive information not found",
        })
      );
    }

    const driveInfo = result[0];

    // Get current cycle balance (mocking for now)
    const gasCycles = "1000000000000"; // Mock value

    // Get daily idle cycle burn rate and controllers (mocking for now)
    const dailyIdleCycleBurnRate = "10000000"; // Mock value
    const controllers: string[] = [
      "Principal_mock_controller_1",
      "Principal_mock_controller_2",
    ]; // Mock value

    const responseData: AboutDriveResponseData = {
      gas_cycles: gasCycles,
      organization_name: driveInfo.drive_name,
      organization_id: driveInfo.drive_id,
      owner: driveInfo.owner_id,
      endpoint: driveInfo.url_endpoint,
      canister_id: driveInfo.canister_id,
      daily_idle_cycle_burn_rate: dailyIdleCycleBurnRate,
      controllers: controllers,
      version: driveInfo.version,
    };

    reply.status(200).send(createApiResponse(responseData));
  } catch (error) {
    request.log.error("Error in aboutDriveHandler:", error);
    reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

/**
 * Handles the /organization/snapshot route.
 * Returns a snapshot of the entire drive state.
 */
export async function snapshotDriveHandler(
  request: FastifyRequest<{ Params: OrgIdParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;

    // Authenticate request (disabled for local environment testing in Rust, replicating that behavior)
    if (!isLocalEnvironment()) {
      const requesterApiKey = await authenticateRequest(
        request,
        "drive",
        org_id
      );
      if (!requesterApiKey) {
        return reply
          .status(401)
          .send(
            createApiResponse(undefined, { code: 401, message: "Unauthorized" })
          );
      }
      const isOwner = requesterApiKey.user_id === (await getOwnerId(org_id));
      if (!isOwner) {
        return reply
          .status(403)
          .send(
            createApiResponse(undefined, { code: 403, message: "Forbidden" })
          );
      }
    }

    // TODO: Implement snapshot_entire_state and convert_state_to_serializable.
    // This will involve reading all relevant tables from the drive's SQLite DB.
    // For now, return a mock snapshot.
    const mockSnapshot = {
      // System info
      canister_id: "mock_canister_id",
      version: "mock_version",
      owner_id: "UserID_mock_owner",
      endpoint_url: "https://mock.icp0.io",
      // API keys state
      apikeys_by_value: { mock_api_value: "ApiKeyID_mock_id" },
      apikeys_by_id: {
        ApiKeyID_mock_id: {
          id: "ApiKeyID_mock_id",
          value: "mock_api_value",
          user_id: "UserID_mock_user",
          name: "Mock API Key",
          created_at: Date.now(),
          expires_at: -1,
          is_revoked: false,
        },
      },
      users_apikeys: { UserID_mock_user: ["ApiKeyID_mock_id"] },
      apikeys_history: [],
      // GiftcardSpawnOrg state
      deployments_by_giftcard_id: {},
      historical_giftcards: [],
      drive_to_giftcard_hashtable: {},
      user_to_giftcards_hashtable: {},
      giftcard_by_id: {},
      // Timestamp
      timestamp_ns: Date.now() * 1_000_000,
    };

    reply.status(200).send(createApiResponse(mockSnapshot));
  } catch (error) {
    request.log.error("Error in snapshotDriveHandler:", error);
    reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

/**
 * Handles the /organization/replay route.
 * Applies state diffs to the drive.
 */
export async function replayDriveHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestReplayDrive }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }
    const isOwner = requesterApiKey.user_id === (await getOwnerId(org_id));
    if (!isOwner) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const replayRequest = request.body;

    if (!replayRequest.diffs || replayRequest.diffs.length === 0) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "No diffs provided for replay",
        })
      );
    }

    // TODO: Implement `safely_apply_diffs`. This is a complex operation that
    // requires deserializing and applying diffs to your SQLite database.
    // For now, we'll mock the success.
    const appliedCount = replayRequest.diffs.length;
    const lastDiffId = replayRequest.diffs[appliedCount - 1]?.id || null;
    const finalChecksum = "mock_checksum_after_replay"; // TODO: Calculate actual checksum

    // Update drive state timestamp in 'about_drive' table
    const currentTimestampNs = Date.now() * 1_000_000;
    await db.queryDrive(
      org_id,
      `UPDATE about_drive SET drive_state_timestamp_ns = ?, drive_state_checksum = ?`,
      [String(currentTimestampNs), finalChecksum]
    );

    const responseData: IResponseReplayDrive["ok"]["data"] = {
      timestamp_ns: currentTimestampNs,
      diffs_applied: appliedCount,
      checkpoint_diff_id: lastDiffId,
      final_checksum: finalChecksum,
    };

    reply.status(200).send(createApiResponse(responseData));
  } catch (error) {
    request.log.error("Error in replayDriveHandler:", error);
    reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

/**
 * Handles the /organization/search route.
 * Performs a search across drive resources.
 */
export async function searchDriveHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: SearchDriveRequestBody;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const searchRequest = request.body;

    if (!searchRequest.query || searchRequest.query.trim().length === 0) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Search query cannot be empty",
        })
      );
    }

    // TODO: Implement actual search logic using SQLite FTS (Full-Text Search) or similar.
    // This would involve querying 'files', 'folders', 'contacts', etc., based on `categories`.
    // For now, return mock search results.
    const mockSearchResults: IResponseSearchDrive["ok"]["data"]["items"] = [
      {
        title: `Mock File - ${searchRequest.query}`,
        preview: "This is a mock file preview.",
        score: 0.9,
        resource_id: "FileID_mock_file_1",
        category: SearchCategoryEnum.FILES,
        created_at: Date.now() - 50000,
        updated_at: Date.now() - 10000,
      },
      {
        title: `Mock Folder - ${searchRequest.query}`,
        preview: "This is a mock folder preview.",
        score: 0.8,
        resource_id: "FolderID_mock_folder_1",
        category: SearchCategoryEnum.FOLDERS,
        created_at: Date.now() - 60000,
        updated_at: Date.now() - 5000,
      },
    ];

    // Apply pagination and sorting as per request
    const pageSize = searchRequest.page_size || 50;
    const startIndex = searchRequest.cursor
      ? parseInt(searchRequest.cursor, 10)
      : 0;
    const paginatedResults = mockSearchResults.slice(
      startIndex,
      startIndex + pageSize
    );
    const nextCursor =
      startIndex + pageSize < mockSearchResults.length
        ? (startIndex + pageSize).toString()
        : undefined;

    const responseData: IResponseSearchDrive["ok"]["data"] = {
      items: paginatedResults,
      page_size: paginatedResults.length,
      total: mockSearchResults.length,
      direction: searchRequest.direction || SortDirection.ASC, // Defaulting to ASC
      cursor: nextCursor,
    };

    reply.status(200).send(createApiResponse(responseData));
  } catch (error) {
    request.log.error("Error in searchDriveHandler:", error);
    reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

/**
 * Handles the /organization/reindex route.
 * Triggers reindexing of drive content for search.
 */
export async function reindexDriveHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestReindexDrive }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }
    const isOwner = requesterApiKey.user_id === (await getOwnerId(org_id));

    // TODO: Implement permission checks. For now, only owner can reindex.
    if (!isOwner) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const reindexRequest = request.body;
    const forceReindex = reindexRequest.force || false;

    // TODO: Implement actual reindexing logic. This would involve iterating
    // through files, folders, contacts, etc., and updating their search indices.
    // For now, just mock the reindex process.
    const lastIndexTime = 0; // TODO: Fetch from a persistent store, e.g., 'about_drive' or a separate search-specific table
    const currentTime = Date.now(); // Milliseconds

    if (
      !forceReindex &&
      lastIndexTime > 0 &&
      currentTime - lastIndexTime < 5 * 60 * 1000
    ) {
      return reply.status(429).send(
        createApiResponse(undefined, {
          code: 429,
          message:
            "Reindex was performed recently. Use 'force: true' to override.",
        })
      );
    }

    const indexedCount = 1234; // Mock value for number of items indexed

    // TODO: Update the last_indexed_ms in the `about_drive` table
    await db.queryDrive(org_id, `UPDATE about_drive SET last_indexed_ms = ?`, [
      currentTime,
    ]);

    const responseData: IResponseReindexDrive["ok"]["data"] = {
      success: true,
      timestamp_ms: currentTime,
      indexed_count: indexedCount,
    };

    reply.status(200).send(createApiResponse(responseData));
  } catch (error) {
    request.log.error("Error in reindexDriveHandler:", error);
    reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

/**
 * Handles the /organization/external_id route.
 * Retrieves internal IDs mapped to external IDs.
 */
export async function externalIdDriveHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: ExternalIDsDriveRequestBody;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }
    const isOwner = requesterApiKey.user_id === (await getOwnerId(org_id));

    // TODO: Implement permission checks. For now, only owner can access.
    if (!isOwner) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const externalIdRequest = request.body;

    if (!externalIdRequest.external_ids) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "external_ids array is required",
        })
      );
    }

    const results: ExternalIDvsInternalIDMap[] = [];

    // TODO: Implement external ID mapping logic. This would involve querying a mapping table.
    // For now, return mock results.
    for (const externalId of externalIdRequest.external_ids) {
      // In real implementation, query `external_id_mappings` table.
      const mockInternalIds: string[] =
        externalId === "ExternalID_test_123"
          ? ["FileID_abc", "FolderID_xyz"]
          : [];

      results.push({
        success: mockInternalIds.length > 0,
        message:
          mockInternalIds.length > 0
            ? "External ID found"
            : "External ID not found",
        external_id: externalId,
        internal_ids: mockInternalIds,
      });
    }

    const responseData: ExternalIDsDriveResponse["ok"]["data"] = {
      results,
    };

    reply.status(200).send(createApiResponse(responseData));
  } catch (error) {
    request.log.error("Error in externalIdDriveHandler:", error);
    reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

/**
 * Handles the /organization/transfer_ownership route.
 * Initiates or completes drive ownership transfer.
 */
export async function transferOwnershipDriveHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestTransferDriveOwnership;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }
    const isOwner = requesterApiKey.user_id === (await getOwnerId(org_id));
    if (!isOwner) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const transferRequest = request.body;

    if (
      !transferRequest.next_owner_id ||
      !transferRequest.next_owner_id.startsWith(IDPrefixEnum.User)
    ) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Invalid next_owner_id format. Must start with UserID_",
        })
      );
    }

    const currentTimestampMs = Date.now();
    const oneDayMs = 24 * 60 * 60 * 1000;

    let status: IResponseTransferDriveOwnership["ok"]["data"]["status"] =
      "REQUESTED";
    let readyMs = currentTimestampMs + oneDayMs;

    // Check existing transfer_owner_id in about_drive
    const existingTransfer = await db.queryDrive(
      org_id,
      `SELECT transfer_owner_id FROM about_drive LIMIT 1`
    );

    const currentTransferValue =
      existingTransfer.length > 0 ? existingTransfer[0].transfer_owner_id : "";

    if (currentTransferValue) {
      const parts = currentTransferValue.split("::");
      if (parts.length === 2) {
        const existingOwnerId = parts[0];
        const transferTimestampMs = parseInt(parts[1], 10);

        if (
          existingOwnerId === transferRequest.next_owner_id &&
          currentTimestampMs - transferTimestampMs > oneDayMs
        ) {
          // Complete the transfer
          await db.queryDrive(
            org_id,
            `UPDATE about_drive SET owner_id = ?, transfer_owner_id = ?`,
            [transferRequest.next_owner_id, ""]
          );
          status = "COMPLETED";
          readyMs = currentTimestampMs;
        }
      }
    }

    if (status === "REQUESTED") {
      // Set or update the transfer request in about_drive
      const newTransferValue = `${transferRequest.next_owner_id}::${currentTimestampMs}`;
      await db.queryDrive(
        org_id,
        `UPDATE about_drive SET transfer_owner_id = ?`,
        [newTransferValue]
      );
    }

    const responseData: IResponseTransferDriveOwnership["ok"]["data"] = {
      status,
      ready_ms: readyMs,
    };

    reply.status(200).send(createApiResponse(responseData));
  } catch (error) {
    request.log.error("Error in transferOwnershipDriveHandler:", error);
    reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

/**
 * Handles the /organization/update_allowed_domains route.
 * This route is likely for a Factory canister and not directly for a Drive.
 * TODO: Re-evaluate if this handler belongs here or in a Factory-specific route.
 * For now, providing a mock implementation.
 */
export async function updateAllowedDomainsDriveHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: { allowed_domains: string };
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params; // org_id might not be relevant for this handler if it's a factory setting
    // TODO: Adjust if this is a Factory-level setting

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id); // Assuming 'drive' context for auth
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }
    const isOwner = requesterApiKey.user_id === (await getOwnerId(org_id)); // Check if owner of this drive
    if (!isOwner) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const { allowed_domains } = request.body;

    if (typeof allowed_domains !== "string" || allowed_domains.length > 256) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Invalid allowed_domains format or length.",
        })
      );
    }

    // TODO: Implement logic to update allowed domains.
    // This is likely a configuration update for the canister/drive.
    // For a multi-tenant SQLite setup, this might be a field in the `about_drive` table
    // or a configuration management system.
    request.log.info(
      `Updating allowed domains for ${org_id}: ${allowed_domains}`
    );

    // Mock update:
    // await db.queryDrive(org_id, `UPDATE about_drive SET allowed_domains = ?`, [allowed_domains]);

    reply.status(200).send(
      createApiResponse({
        success: true,
        message: `Allowed domains for ${org_id} updated successfully to: ${allowed_domains}`,
      })
    );
  } catch (error) {
    request.log.error("Error in updateAllowedDomainsDriveHandler:", error);
    reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

/**
 * Handles the /organization/whoami route.
 * Returns information about the authenticated user and the drive.
 */
export async function whoAmIDriveHandler(
  request: FastifyRequest<{ Params: OrgIdParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = requesterApiKey.user_id === (await getOwnerId(org_id));

    // Get drive nickname from `drives` table
    const driveInfo = await db.queryDrive(
      org_id,
      `SELECT name, icp_principal FROM drives WHERE id = ?`,
      [org_id]
    );

    const driveNickname =
      driveInfo.length > 0 ? driveInfo[0].name : "Unnamed Organization";
    const driveIcpPrincipal =
      driveInfo.length > 0 ? driveInfo[0].icp_principal : "";

    // Get contact information for the authenticated user from `contacts` table
    const contactInfo = await db.queryDrive(
      org_id,
      `SELECT name, evm_public_address, icp_principal FROM contacts WHERE id = ?`,
      [requesterApiKey.user_id]
    );

    const nickname =
      contactInfo.length > 0 ? contactInfo[0].name : "Anonymous User";
    const evmPublicAddress =
      contactInfo.length > 0 ? contactInfo[0].evm_public_address : null;
    const icpPrincipal =
      contactInfo.length > 0
        ? contactInfo[0].icp_principal
        : requesterApiKey.user_id.replace("UserID_", "");

    const whoAmIReport: IResponseWhoAmI = {
      nickname: nickname,
      userID: requesterApiKey.user_id,
      driveID: org_id,
      icp_principal: icpPrincipal,
      evm_public_address: evmPublicAddress,
      is_owner: isOwner,
      drive_nickname: driveNickname,
    };

    reply.status(200).send(createApiResponse(whoAmIReport));
  } catch (error) {
    request.log.error("Error in whoAmIDriveHandler:", error);
    reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

/**
 * Handles the /organization/superswap_user route.
 * Swaps all references from an old user ID to a new user ID.
 */
export async function superswapUserIdDriveHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestSuperswapUser }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }
    const isOwner = requesterApiKey.user_id === (await getOwnerId(org_id));
    if (!isOwner) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const superswapRequest = request.body;

    if (superswapRequest.current_user_id === superswapRequest.new_user_id) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "New user ID must be different from current user ID",
        })
      );
    }
    if (
      !superswapRequest.current_user_id.startsWith(IDPrefixEnum.User) ||
      !superswapRequest.new_user_id.startsWith(IDPrefixEnum.User)
    ) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "User IDs must start with UserID_",
        })
      );
    }

    // Perform the superswap operation within a transaction
    let updatedCount = 0;
    try {
      await dbHelpers.transaction("drive", org_id, (database) => {
        // 1. Update `contacts` table
        const contactUpdate = database.prepare(
          `UPDATE contacts SET id = ?, icp_principal = ? WHERE id = ?`
        );
        const contactResult = contactUpdate.run(
          superswapRequest.new_user_id,
          superswapRequest.new_user_id.replace(IDPrefixEnum.User, ""), // Assuming ICP principal derived from UserID
          superswapRequest.current_user_id
        );
        updatedCount += contactResult.changes || 0;

        // Add old_user_id to `contact_past_ids`
        if (contactResult.changes > 0) {
          database
            .prepare(
              `INSERT OR IGNORE INTO contact_past_ids (user_id, past_user_id) VALUES (?, ?)`
            )
            .run(
              superswapRequest.new_user_id,
              superswapRequest.current_user_id
            );

          // Update contact_id_superswap_history
          database
            .prepare(
              `INSERT INTO contact_id_superswap_history (old_user_id, new_user_id, swapped_at) VALUES (?, ?, ?)`
            )
            .run(
              superswapRequest.current_user_id,
              superswapRequest.new_user_id,
              Date.now()
            );
        }

        // 2. Update `api_keys` table (user_id)
        const apiKeysUpdate = database.prepare(
          `UPDATE api_keys SET user_id = ? WHERE user_id = ?`
        );
        const apiKeysResult = apiKeysUpdate.run(
          superswapRequest.new_user_id,
          superswapRequest.current_user_id
        );
        updatedCount += apiKeysResult.changes || 0;

        // 3. Update `folders` table (created_by_user_id, last_updated_by_user_id)
        const foldersUpdate1 = database.prepare(
          `UPDATE folders SET created_by_user_id = ? WHERE created_by_user_id = ?`
        );
        updatedCount +=
          foldersUpdate1.run(
            superswapRequest.new_user_id,
            superswapRequest.current_user_id
          ).changes || 0;

        const foldersUpdate2 = database.prepare(
          `UPDATE folders SET last_updated_by_user_id = ? WHERE last_updated_by_user_id = ?`
        );
        updatedCount +=
          foldersUpdate2.run(
            superswapRequest.new_user_id,
            superswapRequest.current_user_id
          ).changes || 0;

        // 4. Update `files` table (created_by_user_id, last_updated_by_user_id)
        const filesUpdate1 = database.prepare(
          `UPDATE files SET created_by_user_id = ? WHERE created_by_user_id = ?`
        );
        updatedCount +=
          filesUpdate1.run(
            superswapRequest.new_user_id,
            superswapRequest.current_user_id
          ).changes || 0;

        const filesUpdate2 = database.prepare(
          `UPDATE files SET last_updated_by_user_id = ? WHERE last_updated_by_user_id = ?`
        );
        updatedCount +=
          filesUpdate2.run(
            superswapRequest.new_user_id,
            superswapRequest.current_user_id
          ).changes || 0;

        // 5. Update `file_versions` table (created_by_user_id)
        const fileVersionsUpdate = database.prepare(
          `UPDATE file_versions SET created_by_user_id = ? WHERE created_by_user_id = ?`
        );
        updatedCount +=
          fileVersionsUpdate.run(
            superswapRequest.new_user_id,
            superswapRequest.current_user_id
          ).changes || 0;

        // 6. Update `groups` table (owner_user_id)
        const groupsUpdate = database.prepare(
          `UPDATE groups SET owner_user_id = ? WHERE owner_user_id = ?`
        );
        updatedCount +=
          groupsUpdate.run(
            superswapRequest.new_user_id,
            superswapRequest.current_user_id
          ).changes || 0;

        // 7. Update `group_invites` table (inviter_user_id, invitee_id)
        const groupInvitesUpdate1 = database.prepare(
          `UPDATE group_invites SET inviter_user_id = ? WHERE inviter_user_id = ?`
        );
        updatedCount +=
          groupInvitesUpdate1.run(
            superswapRequest.new_user_id,
            superswapRequest.current_user_id
          ).changes || 0;

        const groupInvitesUpdate2 = database.prepare(
          `UPDATE group_invites SET invitee_id = ? WHERE invitee_id = ?`
        );
        updatedCount +=
          groupInvitesUpdate2.run(
            superswapRequest.new_user_id,
            superswapRequest.current_user_id
          ).changes || 0;

        // 8. Update `labels` table (created_by_user_id)
        const labelsUpdate = database.prepare(
          `UPDATE labels SET created_by_user_id = ? WHERE created_by_user_id = ?`
        );
        updatedCount +=
          labelsUpdate.run(
            superswapRequest.new_user_id,
            superswapRequest.current_user_id
          ).changes || 0;

        // 9. Update `permissions_directory` table (granted_by_user_id, grantee_id)
        const permDirUpdate1 = database.prepare(
          `UPDATE permissions_directory SET granted_by_user_id = ? WHERE granted_by_user_id = ?`
        );
        updatedCount +=
          permDirUpdate1.run(
            superswapRequest.new_user_id,
            superswapRequest.current_user_id
          ).changes || 0;

        const permDirUpdate2 = database.prepare(
          `UPDATE permissions_directory SET grantee_id = ? WHERE grantee_id = ?`
        );
        updatedCount +=
          permDirUpdate2.run(
            superswapRequest.new_user_id,
            superswapRequest.current_user_id
          ).changes || 0;

        // 10. Update `permissions_system` table (granted_by_user_id, grantee_id)
        const permSysUpdate1 = database.prepare(
          `UPDATE permissions_system SET granted_by_user_id = ? WHERE granted_by_user_id = ?`
        );
        updatedCount +=
          permSysUpdate1.run(
            superswapRequest.new_user_id,
            superswapRequest.current_user_id
          ).changes || 0;

        const permSysUpdate2 = database.prepare(
          `UPDATE permissions_system SET grantee_id = ? WHERE grantee_id = ?`
        );
        updatedCount +=
          permSysUpdate2.run(
            superswapRequest.new_user_id,
            superswapRequest.current_user_id
          ).changes || 0;
      });
    } catch (dbError: any) {
      request.log.error("Database error during superswap:", dbError);
      return reply.status(500).send(
        createApiResponse(undefined, {
          code: 500,
          message: `Database error during superswap: ${dbError.message || dbError}`,
        })
      );
    }

    // TODO: Trigger webhook if implemented
    // fire_superswap_user_webhook(...);

    const responseData: IResponseSuperswapUser["ok"]["data"] = {
      success: true,
      message: `'${superswapRequest.current_user_id}' superswapped to '${superswapRequest.new_user_id}', updated ${updatedCount} records`,
    };

    reply.status(200).send(createApiResponse(responseData));
  } catch (error) {
    request.log.error("Error in superswapUserIdDriveHandler:", error);
    reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

/**
 * Handles the /organization/redeem route.
 * Redeems a spawn code for the drive.
 */
export async function redeemOrganizationDriveHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestRedeemOrg }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params; // This org_id refers to the drive that is being redeemed

    const redeemRequest = request.body;

    if (
      !redeemRequest.redeem_code ||
      redeemRequest.redeem_code.trim().length === 0
    ) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Redeem code is required",
        })
      );
    }

    // Get stored redeem code and spawn note from `about_drive` table
    const driveAboutInfo = await db.queryDrive(
      org_id,
      `SELECT spawn_redeem_code, spawn_note, owner_id, url_endpoint FROM about_drive LIMIT 1`
    );

    if (driveAboutInfo.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Drive information not found for redemption.",
        })
      );
    }

    const storedRedeemCode = driveAboutInfo[0].spawn_redeem_code;
    const spawnNote = driveAboutInfo[0].spawn_note;
    const driveOwnerId = driveAboutInfo[0].owner_id;
    const driveEndpointUrl = driveAboutInfo[0].url_endpoint;

    if (!storedRedeemCode || storedRedeemCode.length === 0) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Spawn code has already been redeemed or was never set.",
        })
      );
    }

    if (redeemRequest.redeem_code !== storedRedeemCode) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Invalid redeem code.",
        })
      );
    }

    // Get the owner's primary API key for this drive (assuming one exists or creating one)
    // TODO: This part needs more robust logic. In a real system,
    // how the "admin api key" for a newly spawned drive is generated/retrieved
    // would be critical. For now, we'll generate a new one and link it to the owner.
    const adminApiKeyId = `${IDPrefixEnum.ApiKey}${uuidv4()}`;
    const adminApiKeyValue = await generateApiKey(); // This generates a base64 encoded value

    const newApiKey = {
      id: adminApiKeyId,
      value: adminApiKeyValue,
      user_id: driveOwnerId,
      name: "Default Admin Key for Spawned Drive",
      created_at: Date.now(),
      begins_at: Date.now(),
      expires_at: -1,
      is_revoked: false,
      labels: [], // No labels initially
    };

    await dbHelpers.transaction("drive", org_id, (database) => {
      // Insert the new admin API key
      database
        .prepare(
          `INSERT INTO api_keys (id, value, user_id, name, created_at, begins_at, expires_at, is_revoked, labels)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
        )
        .run(
          newApiKey.id,
          newApiKey.value,
          newApiKey.user_id,
          newApiKey.name,
          newApiKey.created_at,
          newApiKey.begins_at,
          newApiKey.expires_at,
          newApiKey.is_revoked ? 1 : 0,
          JSON.stringify(newApiKey.labels) // Store labels as JSON string
        );

      // Reset the spawn_redeem_code in `about_drive` to mark it as redeemed
      database
        .prepare(
          `UPDATE about_drive SET spawn_redeem_code = ? WHERE drive_id = ?`
        )
        .run("", org_id);
    });

    // Construct the admin login password
    const adminLoginPassword = `${org_id}:${adminApiKeyValue}@${driveEndpointUrl}`;

    const responseData: IResponseRedeemOrg["ok"]["data"] = {
      drive_id: org_id,
      endpoint_url: driveEndpointUrl,
      api_key: adminApiKeyValue,
      note: spawnNote,
      admin_login_password: adminLoginPassword,
    };

    reply.status(200).send(createApiResponse(responseData));
  } catch (error) {
    request.log.error("Error in redeemOrganizationDriveHandler:", error);
    reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

/**
 * Handles the /organization/inbox route.
 * Forwards an inbox notification to registered webhooks.
 */
export async function inboxDriveHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestInboxOrg }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }
    const isOwner = requesterApiKey.user_id === (await getOwnerId(org_id));

    // TODO: Implement permission checks. In Rust, it checks for `SystemPermissionType::Create` on `SystemTableEnum::Inbox`.
    // For now, only owner or if the `Inbox` table can be "created" by the user.
    if (!isOwner) {
      // Assuming a simplified permission check for now.
      // TODO: Replace with actual permission service integration.
      // const hasCreatePermission = await permissionService.checkSystemPermission(
      //   org_id, requesterApiKey.user_id, SystemTableEnum.Inbox, SystemPermissionType.Create
      // );
      // if (!hasCreatePermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
      // }
    }

    const inboxRequest = request.body;

    // Generate unique ID for the notification
    const inboxNotifId = `${IDPrefixEnum.InboxNotifID}${uuidv4()}`;
    const timestampMs = Date.now();

    // TODO: Implement webhook firing logic (fire_org_inbox_new_notif_webhook).
    // This would involve querying `webhooks` table, filtering by event and alt_index (topic),
    // and then making HTTP calls to the webhook URLs.
    request.log.info(
      `Inbox notification received for drive ${org_id}. Topic: ${inboxRequest.topic}, Payload: ${JSON.stringify(inboxRequest.payload)}`
    );

    // Mock webhook invocation:
    // const activeWebhooks = await db.queryDrive(org_id, `SELECT * FROM webhooks WHERE event = ? AND alt_index = ?`, ['organization.inbox.new_notif', inboxRequest.topic]);
    // activeWebhooks.forEach(webhook => {
    //   // Simulate sending webhook payload
    //   console.log(`Sending webhook to ${webhook.url} for event 'organization.inbox.new_notif' with topic '${inboxRequest.topic}'`);
    //   // In a real app, you'd use an HTTP client here.
    // });

    const responseData: IResponseInboxOrg["ok"]["data"] = {
      inbox_notif_id: inboxNotifId,
      drive_id: org_id,
      timestamp_ms: timestampMs,
      note: "Inbox notification received",
    };

    reply.status(200).send(createApiResponse(responseData));
  } catch (error) {
    request.log.error("Error in inboxDriveHandler:", error);
    reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}
