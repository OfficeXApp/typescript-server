// src/routes/v1/drive/contacts/index.ts

import { FastifyPluginAsync } from "fastify";
import {
  getContactHandler,
  listContactsHandler,
  createContactHandler,
  updateContactHandler,
  deleteContactHandler,
  redeemContactHandler,
} from "./handlers";

const contactsRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // Routes will now include a mandatory :org_id parameter
  const baseRoute = "/:org_id/contacts";

  // GET /v1/drive/:org_id/contacts/get/:contact_id
  fastify.get(`/get/:contact_id`, getContactHandler);

  // POST /v1/drive/:org_id/contacts/list
  fastify.post(`/list`, listContactsHandler);

  // POST /v1/drive/:org_id/contacts/create
  fastify.post(`/create`, createContactHandler);

  // POST /v1/drive/:org_id/contacts/update
  fastify.post(`/update`, updateContactHandler);

  // POST /v1/drive/:org_id/contacts/delete
  fastify.post(`/delete`, deleteContactHandler);

  // POST /v1/drive/:org_id/contacts/redeem
  fastify.post(`/redeem`, redeemContactHandler);
};

export default contactsRoutes;
