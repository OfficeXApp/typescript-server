// src/routes/v1/drive/contacts/index.ts

import { FastifyPluginAsync } from "fastify";
import {
  getContactHandler,
  listContactsHandler,
  createContactHandler,
  updateContactHandler,
  deleteContactHandler,
  redeemContactHandler,
  generateAutoLoginLinkHandler,
  generateCryptoIdentityHandler,
} from "./handlers";
import { driveRateLimitPreHandler } from "../../../../services/rate-limit"; // Import the preHandler
import {
  OrgIdParams, // Assuming you might need this for consistency, though not explicitly used in your provided contacts routes
} from "../../types"; // Assuming this path is correct for OrgIdParams
import {
  UserID, // Assuming you have a UserID type
  IRequestCreateContact, // Assuming these types exist for your handlers
  IRequestDeleteContact,
  IRequestListContacts,
  IRequestUpdateContact,
  IRequestRedeemContact,
  IRequestAutoLoginLink,
  IRequestGenerateCryptoIdentity,
  IResponseGetContact,
  IResponseGenerateCryptoIdentity,
  IResponseAutoLoginLink,
  IResponseRedeemContact,
  IResponseDeleteContact,
  IResponseUpdateContact,
  IResponseCreateContact,
  IResponseListContacts,
} from "@officexapp/types"; // Adjust this path if your types are elsewhere

// Define interfaces for params and body if they are not already defined in @officexapp/types
// Example for getContactHandler
interface GetContactParams {
  org_id: string; // From parent plugin prefix
  contact_id: UserID;
}

const contactsRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // GET /v1/drive/:org_id/contacts/get/:contact_id
  fastify.get<{ Params: GetContactParams; Reply: IResponseGetContact }>(
    `/get/:contact_id`,
    { preHandler: [driveRateLimitPreHandler] }, // Add the preHandler here
    getContactHandler
  );

  // POST /v1/drive/:org_id/contacts/list
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestListContacts;
    Reply: IResponseListContacts;
  }>( // Assuming IRequestListContacts exists
    `/list`,
    { preHandler: [driveRateLimitPreHandler] }, // Add the preHandler here
    listContactsHandler
  );

  // POST /v1/drive/:org_id/contacts/create
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestCreateContact;
    Reply: IResponseCreateContact;
  }>( // Assuming IRequestCreateContact exists
    `/create`,
    { preHandler: [driveRateLimitPreHandler] }, // Add the preHandler here
    createContactHandler
  );

  // POST /v1/drive/:org_id/contacts/update
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestUpdateContact;
    Reply: IResponseUpdateContact;
  }>( // Assuming IRequestUpdateContact exists
    `/update`,
    { preHandler: [driveRateLimitPreHandler] }, // Add the preHandler here
    updateContactHandler
  );

  // POST /v1/drive/:org_id/contacts/delete
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestDeleteContact;
    Reply: IResponseDeleteContact;
  }>( // Assuming IRequestDeleteContact exists
    `/delete`,
    { preHandler: [driveRateLimitPreHandler] }, // Add the preHandler here
    deleteContactHandler
  );

  // POST /v1/drive/:org_id/contacts/redeem
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestRedeemContact;
    Reply: IResponseRedeemContact;
  }>( // Assuming IRequestRedeemContact exists
    `/redeem`,
    { preHandler: [driveRateLimitPreHandler] }, // Add the preHandler here
    redeemContactHandler
  );

  // POST /v1/drive/:org_id/contacts/helpers/generate-auto-login-link
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestAutoLoginLink;
    Reply: IResponseAutoLoginLink;
  }>(
    `/helpers/generate-auto-login-link`,
    { preHandler: [driveRateLimitPreHandler] }, // Add the preHandler here
    generateAutoLoginLinkHandler
  );

  // POST /v1/drive/:org_id/contacts/helpers/generate-crypto-identity
  fastify.post<{
    Params: OrgIdParams;
    Body: IRequestGenerateCryptoIdentity;
    Reply: IResponseGenerateCryptoIdentity;
  }>(
    `/helpers/generate-crypto-identity`,
    { preHandler: [driveRateLimitPreHandler] }, // Add the preHandler here
    generateCryptoIdentityHandler
  );
};

export default contactsRoutes;
