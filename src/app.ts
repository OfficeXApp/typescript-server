// src/app.ts

import { join } from "node:path";
import AutoLoad, { AutoloadPluginOptions } from "@fastify/autoload";
import fastifyCors from "@fastify/cors";
import {
  FastifyPluginAsync,
  FastifyServerOptions,
  FastifyInstance,
} from "fastify";
import { v4 as uuidv4 } from "uuid";
import { FactoryApiKey, IDPrefixEnum, UserID } from "@officexapp/types";
import { isValidUserId } from "./api/helpers";
import { generateApiKey } from "./services/auth";
import { dbHelpers, initFactoryDB, db } from "./services/database";

export interface AppOptions
  extends FastifyServerOptions,
    Partial<AutoloadPluginOptions> {
  factory_owner?: string;
}

const options: AppOptions = {};

const app: FastifyPluginAsync<AppOptions> = async (
  fastify,
  opts
): Promise<void> => {
  // Register CORS
  await fastify.register(fastifyCors, {
    origin: "*", // Allow all origins
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"], // Allowed methods
    allowedHeaders: ["Content-Type", "Authorization"], // Allowed headers
  });

  fastify.decorate("officex_version", "OfficeX.Web2_Beta.1.0");

  // Init factory db - this is crucial to ensure factoryDbInstance is set in database.ts
  await initFactoryDB();

  let currentAdminUserId: UserID;
  const ownerFromEnv = process.env.OWNER;

  // 1. Query Factory_Admins table
  // Using db.queryFactory as it's a simple select and returns a promise
  const existingAdmins = (await db.queryFactory(
    "SELECT admin_user_id FROM Factory_Admins"
  )) as { admin_user_id: string }[];

  if (ownerFromEnv) {
    if (!isValidUserId(ownerFromEnv)) {
      throw new Error(
        `Invalid owner ID provided in environment variable: ${ownerFromEnv}`
      );
    }
    // Check if ownerFromEnv matches any existing admin
    const ownerMatchesExistingAdmin = existingAdmins.some(
      (admin) => admin.admin_user_id === ownerFromEnv
    );

    if (ownerMatchesExistingAdmin) {
      currentAdminUserId = ownerFromEnv as UserID;
      fastify.log.info(
        `Owner from environment variable "${currentAdminUserId}" matches an existing admin.`
      );
    } else {
      // If ownerFromEnv doesn't match and there are existing admins, log a warning
      if (existingAdmins.length > 0) {
        fastify.log.warn(
          `Owner from environment variable "${ownerFromEnv}" does not match any existing admin. Adding it as a new admin.`
        );
      }
      // Add ownerFromEnv as a new admin and create an API key for it
      currentAdminUserId = ownerFromEnv as UserID;
      await createAdminAndApiKey(
        fastify,
        currentAdminUserId,
        "Admin from ENV on startup"
      );
      fastify.log.info(
        `Added new admin "${currentAdminUserId}" and created API key based on environment variable.`
      );
    }
  } else if (opts.factory_owner) {
    if (!isValidUserId(opts.factory_owner)) {
      throw new Error("Invalid owner ID provided in AppOptions.");
    }
    // Check if opts.factory_owner matches any existing admin
    const optsOwnerMatchesExistingAdmin = existingAdmins.some(
      (admin) => admin.admin_user_id === opts.factory_owner
    );

    if (optsOwnerMatchesExistingAdmin) {
      currentAdminUserId = opts.factory_owner as UserID;
      fastify.log.info(
        `Owner from AppOptions "${currentAdminUserId}" matches an existing admin.`
      );
    } else {
      // If opts.factory_owner doesn't match and there are existing admins, log a warning
      if (existingAdmins.length > 0) {
        fastify.log.warn(
          `Owner from AppOptions "${opts.factory_owner}" does not match any existing admin. Adding it as a new admin.`
        );
      }
      // Add opts.factory_owner as a new admin and create an API key for it
      currentAdminUserId = opts.factory_owner as UserID;
      await createAdminAndApiKey(
        fastify,
        currentAdminUserId,
        "Admin from AppOptions on startup"
      );
      fastify.log.info(
        `Added new admin "${currentAdminUserId}" and created API key based on AppOptions.`
      );
    }
  } else {
    // No owner provided via ENV or AppOptions
    if (existingAdmins.length > 0) {
      // If DB has admins, use the first one
      currentAdminUserId = existingAdmins[0].admin_user_id as UserID;
      fastify.log.info(
        `No owner provided. Using first admin from database: "${currentAdminUserId}".`
      );
    } else {
      // If DB is empty, create a new admin user and API key
      currentAdminUserId = `${IDPrefixEnum.User}${uuidv4()}` as UserID;
      await createAdminAndApiKey(
        fastify,
        currentAdminUserId,
        "Newly generated admin on startup (DB was empty)"
      );
      fastify.log.info(
        `No owner provided and database is empty. Generated new admin: "${currentAdminUserId}" and created API key.`
      );
    }
  }

  // Ensure opts.factory_owner is set for later use and for decoration
  opts.factory_owner = currentAdminUserId;
  fastify.decorate("factory_owner", currentAdminUserId);

  // Do not touch the following lines
  void fastify.register(AutoLoad, {
    dir: join(__dirname, "plugins"),
    options: opts,
  });

  void fastify.register(AutoLoad, {
    dir: join(__dirname, "routes"),
    options: opts,
  });
};

export default app;
export { app, options };

// Helper function to create an admin and an API key
async function createAdminAndApiKey(
  fastify: FastifyInstance, // Use FastifyInstance type here
  userId: UserID,
  note: string
) {
  const newApiKey: FactoryApiKey = {
    id: `${IDPrefixEnum.ApiKey}${uuidv4()}` as any,
    value: await generateApiKey(),
    user_id: userId,
    name: "Factory API Key",
    created_at: Date.now(),
    expires_at: -1,
    is_revoked: false,
  };

  // Use dbHelpers.transaction to ensure atomicity
  await dbHelpers.transaction("factory", null, (database) => {
    // Insert into Factory_Admins using INSERT OR IGNORE
    const insertAdminStmt = database.prepare(
      `INSERT OR IGNORE INTO Factory_Admins (admin_user_id, note)
       VALUES (?, ?)`
    );
    insertAdminStmt.run(userId, note);

    // Insert into factory_api_keys
    const insertApiKeyStmt = database.prepare(
      `INSERT INTO factory_api_keys (id, value, user_id, name, created_at, expires_at, is_revoked)
       VALUES (?, ?, ?, ?, ?, ?, ?)`
    );
    insertApiKeyStmt.run(
      newApiKey.id,
      newApiKey.value,
      newApiKey.user_id,
      newApiKey.name,
      newApiKey.created_at,
      newApiKey.expires_at,
      newApiKey.is_revoked ? 1 : 0
    );
  });

  fastify.log.info(
    `API Key for admin "${userId}" created: "${newApiKey.value}"`
  );
}

// Optionally, if you want to extend FastifyInstance to include 'owner'
declare module "fastify" {
  interface FastifyInstance {
    factory_owner: UserID;
    officex_version: string;
  }
}
