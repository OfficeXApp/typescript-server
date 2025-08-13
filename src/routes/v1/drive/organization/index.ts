// src/routes/v1/drive/organization/index.ts

import { FastifyPluginAsync } from "fastify";
import {
  aboutDriveHandler,
  snapshotDriveHandler,
  replayDriveHandler,
  searchDriveHandler,
  reindexDriveHandler,
  transferOwnershipDriveHandler,
  updateAllowedDomainsDriveHandler,
  whoAmIDriveHandler,
  superswapUserIdDriveHandler,
  redeemOrganizationDriveHandler,
  inboxDriveHandler,
  shortlinkHandler,
} from "./handlers";
import { driveRateLimitPreHandler } from "../../../../services/rate-limit";
import { OrgIdParams } from "../../types"; // Assuming this path is correct for OrgIdParams
import {
  IAboutDriveResponseData,
  IRequestReplayDrive,
  IRequestSearchDrive,
  IRequestReindexDrive,
  IRequestExternalIDsDrive,
  IRequestTransferDriveOwnership,
  IRequestSuperswapUser,
  IRequestRedeemOrg,
  IRequestInboxOrg,
  IResponseWhoAmI,
  IRequestShortLink,
} from "@officexapp/types"; // Adjust this path if your types are elsewhere

const organizationRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/drive/:org_id/organization/about
  fastify.get<{ Params: OrgIdParams; Reply: IAboutDriveResponseData }>(
    `/about`,
    { preHandler: [driveRateLimitPreHandler] },
    aboutDriveHandler
  );

  // GET /v1/drive/:org_id/organization/snapshot
  // Assuming snapshotDriveHandler has no specific request body/params beyond OrgIdParams,
  // and its response type is not explicitly provided, keeping it generic for now.
  fastify.get<{ Params: OrgIdParams }>(
    `/snapshot`,
    { preHandler: [driveRateLimitPreHandler] },
    snapshotDriveHandler
  );

  // POST /v1/drive/:org_id/organization/replay
  fastify.post<{ Params: OrgIdParams; Body: IRequestReplayDrive }>(
    `/replay`,
    { preHandler: [driveRateLimitPreHandler] },
    replayDriveHandler
  );

  // POST /v1/drive/:org_id/organization/search
  fastify.post<{ Params: OrgIdParams; Body: IRequestSearchDrive }>(
    `/search`,
    { preHandler: [driveRateLimitPreHandler] },
    searchDriveHandler
  );

  // POST /v1/drive/:org_id/organization/reindex
  fastify.post<{ Params: OrgIdParams; Body: IRequestReindexDrive }>(
    `/reindex`,
    { preHandler: [driveRateLimitPreHandler] },
    reindexDriveHandler
  );

  // POST /v1/drive/:org_id/organization/transfer_ownership
  fastify.post<{ Params: OrgIdParams; Body: IRequestTransferDriveOwnership }>(
    `/transfer_ownership`,
    { preHandler: [driveRateLimitPreHandler] },
    transferOwnershipDriveHandler
  );

  // POST /v1/drive/:org_id/organization/update_allowed_domains
  // Note: IRequestUpdateAllowedDomainsDrive type is assumed. If it's not present, you'll need to define it.
  fastify.post(
    `/update_allowed_domains`,

    updateAllowedDomainsDriveHandler
  );

  // GET /v1/drive/:org_id/organization/whoami
  fastify.get<{ Params: OrgIdParams; Reply: IResponseWhoAmI }>(
    `/whoami`,
    { preHandler: [driveRateLimitPreHandler] },
    whoAmIDriveHandler
  );

  // POST /v1/drive/:org_id/organization/superswap_user
  // Corrected type to IRequestSuperswapUser based on your provided types.
  fastify.post<{ Params: OrgIdParams; Body: IRequestSuperswapUser }>(
    `/superswap_user`,
    { preHandler: [driveRateLimitPreHandler] },
    superswapUserIdDriveHandler
  );

  // POST /v1/drive/:org_id/organization/redeem
  // Corrected type to IRequestRedeemOrg based on your provided types.
  fastify.post<{ Params: OrgIdParams; Body: IRequestRedeemOrg }>(
    `/redeem`,
    { preHandler: [driveRateLimitPreHandler] },
    redeemOrganizationDriveHandler
  );

  // POST /v1/drive/:org_id/organization/inbox
  // Corrected type to IRequestInboxOrg based on your provided types.
  fastify.post<{ Params: OrgIdParams; Body: IRequestInboxOrg }>(
    `/inbox`,
    { preHandler: [driveRateLimitPreHandler] },
    inboxDriveHandler
  );

  // POST /v1/drive/:org_id/organization/shortlink
  fastify.post<{ Params: OrgIdParams; Body: IRequestShortLink }>(
    `/shortlink`,
    { preHandler: [driveRateLimitPreHandler] },
    shortlinkHandler
  );
};

export default organizationRoutes;
