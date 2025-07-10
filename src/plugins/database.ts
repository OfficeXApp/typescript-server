// src/plugins/database.ts
import fp from "fastify-plugin";
import { FastifyPluginAsync } from "fastify";
import { initFactoryDB, dbHelpers } from "../services/database"; // Changed initializeDatabase to initFactoryDB

declare module "fastify" {
  interface FastifyInstance {
    db: typeof dbHelpers;
  }
}

const databasePlugin: FastifyPluginAsync = async (fastify, options) => {
  // Initialize database schema on startup
  await initFactoryDB(); // Call the new async initialization function

  // Decorate fastify instance with database helpers
  fastify.decorate("db", dbHelpers);

  fastify.log.info("Database plugin initialized");
};

export default fp(databasePlugin, {
  name: "database",
});
