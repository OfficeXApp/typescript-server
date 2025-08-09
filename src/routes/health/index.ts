import { FastifyPluginAsync } from "fastify";
import * as Sentry from "@sentry/node";

const healthRoutes: FastifyPluginAsync = async (
  fastify,
  opts
): Promise<void> => {
  // This route will be at /v1/health
  fastify.get("/health", async function (request, reply) {
    const sanity_check_env = process.env.SANITY_CHECK_ENV;
    // console.log("Log from health check, sending sentry error");
    // Sentry.captureException(new Error("Test error from health check"));
    // throw new Error("New Test error from health check");

    return { message: `OK - ${sanity_check_env}` };
  });
};

export default healthRoutes;
