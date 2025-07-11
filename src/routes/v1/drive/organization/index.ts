import { FastifyPluginAsync } from "fastify";
import {
  aboutDriveHandler,
  snapshotDriveHandler,
  replayDriveHandler,
  searchDriveHandler,
  reindexDriveHandler,
  externalIdDriveHandler,
  transferOwnershipDriveHandler,
  updateAllowedDomainsDriveHandler,
  whoAmIDriveHandler,
  superswapUserIdDriveHandler,
  redeemOrganizationDriveHandler,
  inboxDriveHandler,
} from "./handlers";

const organizationRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/drive/:org_id/organization/about
  fastify.get(`/organization/about`, aboutDriveHandler);

  // GET /v1/drive/:org_id/organization/snapshot
  fastify.get(`/organization/snapshot`, snapshotDriveHandler);

  // POST /v1/drive/:org_id/organization/replay
  fastify.post(`/organization/replay`, replayDriveHandler);

  // POST /v1/drive/:org_id/organization/search
  fastify.post(`/organization/search`, searchDriveHandler);

  // POST /v1/drive/:org_id/organization/reindex
  fastify.post(`/organization/reindex`, reindexDriveHandler);

  // POST /v1/drive/:org_id/organization/external_id
  fastify.post(`/organization/external_id`, externalIdDriveHandler);

  // POST /v1/drive/:org_id/organization/transfer_ownership
  fastify.post(
    `/organization/transfer_ownership`,
    transferOwnershipDriveHandler
  );

  // POST /v1/drive/:org_id/organization/update_allowed_domains
  fastify.post(
    `/organization/update_allowed_domains`,
    updateAllowedDomainsDriveHandler
  );

  // GET /v1/drive/:org_id/organization/whoami
  fastify.get(`/organization/whoami`, whoAmIDriveHandler);

  // POST /v1/drive/:org_id/organization/superswap_user
  fastify.post(`/organization/superswap_user`, superswapUserIdDriveHandler);

  // POST /v1/drive/:org_id/organization/redeem
  fastify.post(`/organization/redeem`, redeemOrganizationDriveHandler);

  // POST /v1/drive/:org_id/organization/inbox
  fastify.post(`/organization/inbox`, inboxDriveHandler);
};

export default organizationRoutes;
