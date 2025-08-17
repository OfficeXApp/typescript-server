// src/server.ts

import "./instrument";
import "./services/analytics";

import mixpanel from "mixpanel";
import { fastify } from "fastify";
import { app } from "./app";
import { LOCAL_DEV_MODE } from "./constants";

if (process.env.MIXPANEL_TOKEN) {
  mixpanel.init(process.env.MIXPANEL_TOKEN);
}

// Configure server options based on environment
const serverOptions = LOCAL_DEV_MODE
  ? {
      logger: {
        level: "debug", // 'info', 'warn', 'error'
        transport: {
          target: "pino-pretty", // Makes logs readable in development console
          options: {
            translateTime: "HH:MM:ss Z",
            ignore: "pid,hostname",
          },
        },
      },
    }
  : {
      logger: {
        level: process.env.LOG_LEVEL || "info",
      },
    };

const server = fastify(serverOptions);

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
