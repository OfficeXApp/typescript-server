import { FastifyPluginAsync } from "fastify";
import healthRoutes from "./health";

const root: FastifyPluginAsync = async (fastify, opts): Promise<void> => {
  fastify.get("/", async function (request, reply) {
    return { root: true };
  });

  fastify.register(healthRoutes);
};

export default root;
