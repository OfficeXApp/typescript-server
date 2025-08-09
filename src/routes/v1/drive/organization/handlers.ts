import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import {
  ISuccessResponse,
  IRequestReplayDrive,
  IRequestSearchDrive,
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
  IRequestExternalIDsDrive,
  IResponseExternalIDsDrive,
  ExternalIDvsInternalIDMap,
  IAboutDriveResponseData,
  SearchCategoryEnum,
  SortDirection,
  SystemPermissionType, // Import SystemPermissionType
  SystemTableValueEnum,
  SystemResourceID,
  IErrorResponse,
  IRequestShortLink,
  IResponseShortLink, // Import SystemTableValueEnum
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import {
  authenticateRequest,
  generateApiKey,
  urlSafeBase64Encode,
} from "../../../../services/auth";
import { DriveID, UserID, IDPrefixEnum } from "@officexapp/types";
import { createApiResponse, getDriveOwnerId, OrgIdParams } from "../../types";
import { checkSystemPermissions } from "../../../../services/permissions/system"; // Import permission checks
import { getFactorySnapshot } from "../../../../services/snapshot/factory";
import {
  DriveStateSnapshot,
  getDriveSnapshot,
} from "../../../../services/snapshot/drive";
import { LOCAL_DEV_MODE } from "../../../../constants";

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

    const ownerId = await getDriveOwnerId(org_id);
    const isOwner = requesterApiKey.user_id === ownerId;

    const hasViewPermission = (
      await checkSystemPermissions({
        resourceTable: `TABLE_${SystemTableValueEnum.DRIVES}`,
        resourceId: `${org_id}` as SystemResourceID,
        granteeId: requesterApiKey.user_id,
        orgId: org_id,
      })
    ).includes(SystemPermissionType.VIEW);

    if (!isOwner && !hasViewPermission) {
      request.log.warn(
        `User ${requesterApiKey.user_id} does not have VIEW permission for the PERMISSIONS table in ${org_id}.`
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
              timestamp_ns, owner_id, host_url, transfer_owner_id,
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

    const responseData: IAboutDriveResponseData = {
      gas_cycles: gasCycles,
      organization_name: driveInfo.drive_name,
      organization_id: driveInfo.drive_id,
      owner: driveInfo.owner_id,
      host: driveInfo.host_url,
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
        message: `Internal server error - ${error}`,
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
    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));
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

    // DRIVE: Implement `safely_apply_diffs`. This is a complex operation that
    // requires deserializing and applying diffs to your SQLite database.
    // For now, we'll mock the success.
    const appliedCount = replayRequest.diffs.length;
    const lastDiffId = replayRequest.diffs[appliedCount - 1]?.id || null;
    const finalChecksum = "mock_checksum_after_replay"; // DRIVE: Calculate actual checksum

    // Update drive state timestamp in 'about_drive' table
    const currentTimestampNs = Date.now() * 1_000_000;
    await db.queryDrive(
      org_id,
      `UPDATE about_drive SET timestamp_ns = ?, drive_state_checksum = ?`,
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
        message: `Internal server error - ${error}`,
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
    Body: IRequestSearchDrive;
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

    const searchResults = await db.fuzzySearch(org_id, searchRequest);
    if ((searchResults as IResponseSearchDrive).ok) {
      return reply.status(200).send(searchResults);
    } else {
      return reply.status(500).send(searchResults);
    }
  } catch (error) {
    request.log.error("Error in searchDriveHandler:", error);
    reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
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
    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    // PERMIT: Implement permission checks using `checkPermissionsTableAccess`
    const hasEditPermission = (
      await checkSystemPermissions({
        resourceTable: `TABLE_${SystemTableValueEnum.DRIVES}`,
        resourceId: `${org_id}` as SystemResourceID,
        granteeId: requesterApiKey.user_id,
        orgId: org_id,
      })
    ).includes(SystemPermissionType.VIEW);

    if (!isOwner && !hasEditPermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const reindexRequest = request.body;
    const forceReindex = reindexRequest.force || false;

    // SQLite always keeps our index up to date
    const responseData: IResponseReindexDrive["ok"]["data"] = {
      success: true,
      timestamp_ms: Date.now(),
      indexed_count: 0,
    };

    reply.status(200).send(createApiResponse(responseData));
  } catch (error) {
    request.log.error("Error in reindexDriveHandler:", error);
    reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
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
    Body: IRequestExternalIDsDrive;
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
    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    // PERMIT: Implement permission checks using `checkPermissionsTableAccess`
    const hasViewPermission = (
      await checkSystemPermissions({
        resourceTable: `TABLE_${SystemTableValueEnum.DRIVES}`,
        resourceId: `${org_id}` as SystemResourceID,
        granteeId: requesterApiKey.user_id,
        orgId: org_id,
      })
    ).includes(SystemPermissionType.VIEW);

    if (!isOwner && !hasViewPermission) {
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

    // DRIVE: Implement external ID mapping logic. This would involve querying a mapping table.
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

    const responseData: IResponseExternalIDsDrive["ok"]["data"] = {
      results,
    };

    reply.status(200).send(createApiResponse(responseData));
  } catch (error) {
    request.log.error("Error in externalIdDriveHandler:", error);
    reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
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
    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));
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
        message: `Internal server error - ${error}`,
      })
    );
  }
}

/**
 * Handles the /organization/update_allowed_domains route.
 * This route is likely for a Factory canister and not directly for a Drive.
 * DRIVE: Re-evaluate if this handler belongs here or in a Factory-specific route.
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
    // DRIVE: Adjust if this is a Factory-level setting

    // Authenticate request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id); // Assuming 'drive' context for auth
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }
    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));
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

    // DRIVE: Implement logic to update allowed domains.
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
        message: `Internal server error - ${error}`,
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

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

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

    const whoAmIReport = {
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
        message: `Internal server error - ${error}`,
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
    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));
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

        // 3. Update `folders` table (created_by, last_updated_by)
        const foldersUpdate1 = database.prepare(
          `UPDATE folders SET created_by = ? WHERE created_by = ?`
        );
        updatedCount +=
          foldersUpdate1.run(
            superswapRequest.new_user_id,
            superswapRequest.current_user_id
          ).changes || 0;

        const foldersUpdate2 = database.prepare(
          `UPDATE folders SET last_updated_by = ? WHERE last_updated_by = ?`
        );
        updatedCount +=
          foldersUpdate2.run(
            superswapRequest.new_user_id,
            superswapRequest.current_user_id
          ).changes || 0;

        // 4. Update `files` table (created_by, last_updated_by)
        const filesUpdate1 = database.prepare(
          `UPDATE files SET created_by = ? WHERE created_by = ?`
        );
        updatedCount +=
          filesUpdate1.run(
            superswapRequest.new_user_id,
            superswapRequest.current_user_id
          ).changes || 0;

        const filesUpdate2 = database.prepare(
          `UPDATE files SET last_updated_by = ? WHERE last_updated_by = ?`
        );
        updatedCount +=
          filesUpdate2.run(
            superswapRequest.new_user_id,
            superswapRequest.current_user_id
          ).changes || 0;

        // 5. Update `file_versions` table (created_by)
        const fileVersionsUpdate = database.prepare(
          `UPDATE file_versions SET created_by = ? WHERE created_by = ?`
        );
        updatedCount +=
          fileVersionsUpdate.run(
            superswapRequest.new_user_id,
            superswapRequest.current_user_id
          ).changes || 0;

        // 6. Update `groups` table (owner)
        const groupsUpdate = database.prepare(
          `UPDATE groups SET owner = ? WHERE owner = ?`
        );
        updatedCount +=
          groupsUpdate.run(
            superswapRequest.new_user_id,
            superswapRequest.current_user_id
          ).changes || 0;

        // 7. Update `group_invites` table (inviter_id, invitee_id)
        const groupInvitesUpdate1 = database.prepare(
          `UPDATE group_invites SET inviter_id = ? WHERE inviter_id = ?`
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

        // 8. Update `labels` table (created_by)
        const labelsUpdate = database.prepare(
          `UPDATE labels SET created_by = ? WHERE created_by = ?`
        );
        updatedCount +=
          labelsUpdate.run(
            superswapRequest.new_user_id,
            superswapRequest.current_user_id
          ).changes || 0;

        // 9. Update `permissions_directory` table (granted_by, grantee_id)
        const permDirUpdate1 = database.prepare(
          `UPDATE permissions_directory SET granted_by = ? WHERE granted_by = ?`
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

        // 10. Update `permissions_system` table (granted_by, grantee_id)
        const permSysUpdate1 = database.prepare(
          `UPDATE permissions_system SET granted_by = ? WHERE granted_by = ?`
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

    // WEBHOOK: Trigger webhook if implemented
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
        message: `Internal server error - ${error}`,
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
      `SELECT spawn_redeem_code, spawn_note, owner_id, host_url, drive_name FROM about_drive LIMIT 1`
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
    const driveEndpointUrl = driveAboutInfo[0].host_url;
    const driveName = driveAboutInfo[0].drive_name;

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
    // DRIVE: This part needs more robust logic. In a real system,
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
          `INSERT INTO api_keys (id, value, user_id, name, created_at, begins_at, expires_at, is_revoked)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
        )
        .run(
          newApiKey.id,
          newApiKey.value,
          newApiKey.user_id,
          newApiKey.name,
          newApiKey.created_at,
          newApiKey.begins_at,
          newApiKey.expires_at,
          newApiKey.is_revoked ? 1 : 0
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
    const auto_login_details = {
      org_name: driveName,
      org_id: org_id,
      org_host: driveEndpointUrl,
      profile_id: adminApiKeyId,
      profile_name: "Admin",
      profile_api_key: adminApiKeyValue,
      profile_seed_phrase: undefined,
    };
    const auto_login_redeem_token = urlSafeBase64Encode(
      JSON.stringify(auto_login_details)
    );
    const frontend_endpoint = LOCAL_DEV_MODE
      ? "http://localhost:5173"
      : "https://officex.app";
    const autoLoginUrl = `${frontend_endpoint}/auto-login?token=${auto_login_redeem_token}`;
    const responseData: IResponseRedeemOrg["ok"]["data"] = {
      drive_id: org_id,
      host_url: driveEndpointUrl,
      api_key: adminApiKeyValue,
      note: spawnNote,
      admin_login_password: adminLoginPassword,
      auto_login_url: autoLoginUrl,
    };

    reply.status(200).send(createApiResponse(responseData));
  } catch (error) {
    request.log.error("Error in redeemOrganizationDriveHandler:", error);
    reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
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
    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    const hasCreatePermission = (
      await checkSystemPermissions({
        resourceTable: `TABLE_${SystemTableValueEnum.INBOX}`,
        granteeId: requesterApiKey.user_id,
        orgId: org_id,
      })
    ).includes(SystemPermissionType.CREATE);

    if (!isOwner && !hasCreatePermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const inboxRequest = request.body;

    // Generate unique ID for the notification
    const inboxNotifId = `${IDPrefixEnum.InboxNotifID}${uuidv4()}`;
    const timestampMs = Date.now();

    // WEBHOOK: Implement webhook firing logic (fire_org_inbox_new_notif_webhook).
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
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function snapshotDriveHandler(
  request: FastifyRequest<{ Params: OrgIdParams }>,
  reply: FastifyReply
): Promise<void> {
  const { org_id: driveId } = request.params; // org_id maps to DriveID

  try {
    request.log.info(`Incoming snapshot request for drive: ${driveId}`);

    // Authenticate request and check if owner
    const requesterApiKey = await authenticateRequest(
      request,
      "drive",
      driveId
    );
    if (!requesterApiKey) {
      return reply.status(401).send(
        createApiResponse<undefined>(undefined, {
          code: 401,
          message: "Unauthorized",
        })
      );
    }
    const isOwner =
      requesterApiKey.user_id === (await getDriveOwnerId(driveId));

    if (!isOwner) {
      return reply.status(403).send(
        createApiResponse<undefined>(undefined, {
          code: 403,
          message: "Forbidden",
        })
      );
    }

    // Call the refactored function to get the full snapshot
    const stateSnapshot: DriveStateSnapshot = await getDriveSnapshot(
      driveId as DriveID // Cast to DriveID as it comes from params
    );

    reply
      .status(200)
      .send(createApiResponse<DriveStateSnapshot>(stateSnapshot));
  } catch (error: any) {
    request.log.error("Error in snapshotDriveHandler:", error);
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

export async function shortlinkHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestShortLink }>,
  reply: FastifyReply
): Promise<IResponseShortLink> {
  console.log(`Incoming shortlink request for drive`);
  const { org_id: driveId } = request.params;

  try {
    request.log.info(`Incoming shortlink request for drive: ${driveId}`);
    //
    const slug = request.body.slug;
    const original_url = request.body.original_url;

    console.log(`org_id: ${driveId}, body: ${JSON.stringify(request.body)}`);
    if (slug && !original_url) {
      // handle slug to return original url, just write the SQL
      const sql = `SELECT url FROM shortlinks WHERE id = ?`;
      const result = await db.queryDrive(driveId, sql, [slug]);
      console.log(`result`, result);
      if (result.length === 0) {
        return reply.status(404).send(
          createApiResponse<undefined>(undefined, {
            code: 404,
            message: "Shortlink not found",
          })
        );
      }
      return reply.status(200).send(
        createApiResponse({
          slug,
          original_url: result[0].url,
        })
      );
    } else if (!slug && original_url) {
      const requesterApiKey = await authenticateRequest(
        request,
        "drive",
        driveId
      );
      if (!requesterApiKey) {
        return reply.status(401).send(
          createApiResponse<undefined>(undefined, {
            code: 401,
            message: "Unauthorized",
          })
        );
      }
      // handle url to insert new entry into sqlite and return slug + url
      const slug = uuidv4();
      console.log(`slug: ${slug}, original_url: ${original_url}`);
      const sql = `INSERT INTO shortlinks (id, url, created_by, created_at) VALUES (?, ?, ?, ?)`;
      const result = await db.runDrive(driveId, sql, [
        slug,
        original_url,
        requesterApiKey.user_id,
        Date.now(),
      ]);
      return reply.status(200).send(
        createApiResponse({
          slug,
          original_url,
        })
      );
    } else if (slug && original_url) {
      const requesterApiKey = await authenticateRequest(
        request,
        "drive",
        driveId
      );
      if (!requesterApiKey) {
        return reply.status(401).send(
          createApiResponse<undefined>(undefined, {
            code: 401,
            message: "Unauthorized",
          })
        );
      }
      // assume this is requesting a deletion of shortlink, but only owner or original poster can delete
      const get_sql = `SELECT * FROM shortlinks WHERE id = ?`;
      const result = await db.queryDrive(driveId, get_sql, [slug]);
      console.log(`result`, result);
      if (result.length === 0) {
        return reply.status(404).send(
          createApiResponse<undefined>(undefined, {
            code: 404,
            message: "Shortlink not found",
          })
        );
      }
      const shortlink = result[0];
      const isOwner =
        requesterApiKey.user_id === (await getDriveOwnerId(driveId));
      const hasEditPermission = (
        await checkSystemPermissions({
          resourceTable: `TABLE_DRIVES`,
          resourceId: driveId,
          granteeId: requesterApiKey.user_id,
          orgId: driveId,
        })
      ).includes(SystemPermissionType.EDIT);
      if (
        shortlink.created_by !== requesterApiKey.user_id &&
        !isOwner &&
        !hasEditPermission
      ) {
        return reply.status(403).send(
          createApiResponse<undefined>(undefined, {
            code: 403,
            message:
              "Forbidden. You are not the owner of this shortlink or organization, or have edit permissions on this drive.",
          })
        );
      }
      const del_sql = `DELETE FROM shortlinks WHERE id = ?`;
      const del_result = await db.runDrive(driveId, del_sql, [slug]);
      console.log(`del_result`, del_result);
      return reply.status(200).send(
        createApiResponse({
          slug,
          original_url,
        })
      );
    } else {
      return reply.status(400).send(
        createApiResponse<undefined>(undefined, {
          code: 400,
          message:
            "Invalid request, can only send a slug or a url, not both or none.",
        })
      );
    }
  } catch (error: any) {
    return reply.status(500).send(
      createApiResponse<undefined>(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}
