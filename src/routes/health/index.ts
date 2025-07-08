import { FastifyPluginAsync } from "fastify";

const healthRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // This route will be at /v1/health
  fastify.get("/health", async function (request, reply) {
    return { message: "OK" };
  });
};

export default healthRoutes;
