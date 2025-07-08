// src/plugins/database.ts
import fp from "fastify-plugin";
import { FastifyPluginAsync } from "fastify";
import { initializeDatabase, dbHelpers } from "../services/database";

declare module "fastify" {
  interface FastifyInstance {
    db: typeof dbHelpers;
  }
}

const databasePlugin: FastifyPluginAsync = async (fastify, options) => {
  // Initialize database schema on startup
  initializeDatabase();

  // Decorate fastify instance with database helpers
  fastify.decorate("db", dbHelpers);

  fastify.log.info("Database plugin initialized");
};

export default fp(databasePlugin, {
  name: "database",
});
