import { join } from "node:path";
import AutoLoad, { AutoloadPluginOptions } from "@fastify/autoload";
import { FastifyPluginAsync, FastifyServerOptions } from "fastify";
import { v4 as uuidv4 } from "uuid";
import { FactoryApiKey, IDPrefixEnum, UserID } from "@officexapp/types";
import { isValidUserId } from "./api/helpers";
import { generateApiKey } from "./services/auth";
import { dbHelpers, initFactoryDB } from "./services/database";

export interface AppOptions
  extends FastifyServerOptions,
    Partial<AutoloadPluginOptions> {
  // We still keep this for type safety, but the actual value will come from env
  factory_owner?: string;
}

// Pass --options via CLI arguments in command to enable these options.
const options: AppOptions = {
  // You can set a default here if you want, but the logic below will handle it
};

const app: FastifyPluginAsync<AppOptions> = async (
  fastify,
  opts
): Promise<void> => {
  fastify.decorate("version", "OfficeX.Web2_Beta.1.0");

  // init factory db
  await initFactoryDB();

  // Get owner from environment variable, or use the generated one if not provided
  const ownerFromEnv = process.env.OWNER;

  if (ownerFromEnv) {
    if (!isValidUserId(ownerFromEnv)) {
      throw new Error("Invalid owner ID");
    }
    opts.factory_owner = ownerFromEnv;
    fastify.log.info(
      `Owner provided via environment variable: ${opts.factory_owner}`
    );
  } else if (opts.factory_owner) {
    if (!isValidUserId(opts.factory_owner)) {
      throw new Error("Invalid owner ID");
    }
    fastify.log.info(`Owner provided via options: ${opts.factory_owner}`);
  } else {
    opts.factory_owner = `${IDPrefixEnum.User}${uuidv4()}`;
    const newApiKey: FactoryApiKey = {
      id: `${IDPrefixEnum.ApiKey}${uuidv4()}` as any,
      value: await generateApiKey(),
      user_id: opts.factory_owner,
      name: "Factory API Key",
      created_at: Date.now(),
      expires_at: -1,
      is_revoked: false,
    };
    // Insert into database using transaction
    await dbHelpers.transaction("factory", null, (database) => {
      const stmt = database.prepare(
        `INSERT INTO factory_api_keys (id, value, user_id, name, created_at, expires_at, is_revoked) 
               VALUES (?, ?, ?, ?, ?, ?, ?)`
      );
      stmt.run(
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
      `No owner provided. Generated new owner: "${opts.factory_owner}" and API key: "${newApiKey.value}"`
    );
  }

  // Make the owner accessible throughout your application
  fastify.decorate("factory_owner", opts.factory_owner);

  // Do not touch the following lines

  // This loads all plugins defined in plugins
  // those should be support plugins that are reused
  // through your application
  // eslint-disable-next-line no-void
  void fastify.register(AutoLoad, {
    dir: join(__dirname, "plugins"),
    options: opts,
  });

  // This loads all plugins defined in routes
  // define your routes in one of these
  // eslint-disable-next-line no-void
  void fastify.register(AutoLoad, {
    dir: join(__dirname, "routes"),
    options: opts,
  });
};

export default app;
export { app, options };

// Optionally, if you want to extend FastifyInstance to include 'owner'
declare module "fastify" {
  interface FastifyInstance {
    factory_owner: UserID;
  }
}
