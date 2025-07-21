import { FastifyPluginAsync } from "fastify";

const healthRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // This route will be at /v1/health
  fastify.get("/health", async function (request, reply) {
    const sanity_check_env = process.env.SANITY_CHECK_ENV;
    return { message: `OK - ${sanity_check_env}` };
  });
};

export default healthRoutes;
