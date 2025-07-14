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
  fastify.get(`/about`, aboutDriveHandler);

  // GET /v1/drive/:org_id/organization/snapshot
  fastify.get(`/snapshot`, snapshotDriveHandler);

  // POST /v1/drive/:org_id/organization/replay
  fastify.post(`/replay`, replayDriveHandler);

  // POST /v1/drive/:org_id/organization/search
  fastify.post(`/search`, searchDriveHandler);

  // POST /v1/drive/:org_id/organization/reindex
  fastify.post(`/reindex`, reindexDriveHandler);

  // POST /v1/drive/:org_id/organization/external_id
  fastify.post(`/external_id`, externalIdDriveHandler);

  // POST /v1/drive/:org_id/organization/transfer_ownership
  fastify.post(`/transfer_ownership`, transferOwnershipDriveHandler);

  // POST /v1/drive/:org_id/organization/update_allowed_domains
  fastify.post(`/update_allowed_domains`, updateAllowedDomainsDriveHandler);

  // GET /v1/drive/:org_id/organization/whoami
  fastify.get(`/whoami`, whoAmIDriveHandler);

  // POST /v1/drive/:org_id/organization/superswap_user
  fastify.post(`/superswap_user`, superswapUserIdDriveHandler);

  // POST /v1/drive/:org_id/organization/redeem
  fastify.post(`/redeem`, redeemOrganizationDriveHandler);

  // POST /v1/drive/:org_id/organization/inbox
  fastify.post(`/inbox`, inboxDriveHandler);
};

export default organizationRoutes;
