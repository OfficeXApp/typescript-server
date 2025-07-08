import { FastifyPluginAsync } from "fastify";

const contactRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // This route will be at /v1/drive/:org_id/contacts
  fastify.get("/", async function (request, reply) {
    const params = request.params as { org_id: string };
    return { message: `Listing contacts for org ${params.org_id}` };
  });

  // This route will be at /v1/drive/:org_id/contacts/:contact_id
  fastify.get("/:contact_id", async function (request, reply) {
    const params = request.params as { org_id: string; contact_id: string };
    return {
      message: `Getting contact ${params.contact_id} for org ${params.org_id}`,
    };
  });
};

export default contactRoutes;
