// src/server.ts

import "./instrument";

import { fastify } from "fastify";
import { app } from "./app";
import Fastify from "fastify";
import { LOCAL_DEV_MODE } from "./constants";
import * as Sentry from "@sentry/node";

import * as dotenv from "dotenv";
dotenv.config();

if (LOCAL_DEV_MODE) {
  Fastify({
    logger: {
      level: "debug", // 'info', 'warn', 'error'
      transport: {
        target: "pino-pretty", // Makes logs readable in development console
        options: {
          translateTime: "HH:MM:ss Z",
          ignore: "pid,hostname",
        },
      },
      // For production, you might remove transport or send to a file/service
      // file: '/var/log/myapp.log', // Example for file logging
    },
  });
}

const server = fastify({
  logger: {
    level: process.env.LOG_LEVEL || "info",
  },
});

Sentry.setupFastifyErrorHandler(server);

// Register your application
server.register(app);

// Start listening
const start = async () => {
  try {
    const port = parseInt(process.env.PORT || "8888");
    const host = process.env.HOST || "0.0.0.0";

    await server.listen({ port, host });

    console.log(`Server listening on ${host}:${port}`);
    console.log(`Data directory: ${process.env.DATA_DIR || "/data"}`);
  } catch (err) {
    server.log.error(err);
    process.exit(1);
  }
};

// Handle graceful shutdown
process.on("SIGTERM", async () => {
  console.log("SIGTERM received, shutting down gracefully...");
  await server.close();
  process.exit(0);
});

start();
