import Database from "better-sqlite3";
import path from "path";
import fs from "fs";

const DATA_DIR = process.env.DATA_DIR || "/data";

// Helper to ensure directory exists
function ensureDirectorySync(dir: string): void {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

// Helper to configure database with optimal settings
function configureDatabase(db: Database.Database): void {
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.pragma("cache_size = -2000"); // 2GB cache
  db.pragma("temp_store = MEMORY");
  db.pragma("mmap_size = 30000000"); // 30MB mmap
  db.pragma("busy_timeout = 5000"); // 5 seconds
}

// Get factory database path
function getFactoryDbPath(): string {
  return path.join(DATA_DIR, "factory.db");
}

// Get drive database path with sharding
function getDriveDbPath(driveId: string): string {
  if (!driveId.startsWith("DriveID_")) {
    throw new Error(`Invalid drive ID format: ${driveId}`);
  }

  const idPart = driveId.substring(8);
  if (idPart.length < 3) {
    throw new Error(`Drive ID suffix too short for sharding: ${idPart}`);
  }

  const shard1 = idPart.substring(0, 2);
  const shard2 = idPart.substring(2, 3);

  return path.join(DATA_DIR, "drives", shard1, shard2, `${driveId}.db`);
}

// --- Read SQL schema files ---
const MIGRATIONS_DIR = path.join(__dirname, "..", "migrations"); // Adjust path as needed based on your compiled JS output location

let FACTORY_SCHEMA: string;
let DRIVE_SCHEMA: string;

try {
  FACTORY_SCHEMA = fs.readFileSync(
    path.join(MIGRATIONS_DIR, "factory", "schema_factory.sql"),
    "utf8"
  );
  DRIVE_SCHEMA = fs.readFileSync(
    path.join(MIGRATIONS_DIR, "drive", "schema_drive.sql"),
    "utf8"
  );
} catch (error) {
  console.error("Error reading SQL schema files:", error);
  // Depending on your application's startup requirements, you might want to exit here
  // process.exit(1);
  FACTORY_SCHEMA = ""; // Set to empty string to prevent errors if files are missing during development/testing
  DRIVE_SCHEMA = "";
}
// --- End Read SQL schema files ---

// Main database service interface for compatibility with your handlers
export const db = {
  // Query method that your handlers expect. This method will ALWAYS target the factory DB.
  queryFactory: async (sql: string, params?: any[]): Promise<any[]> => {
    const database = new Database(getFactoryDbPath());
    try {
      configureDatabase(database);
      const stmt = database.prepare(sql);
      return stmt.all(...(params || []));
    } finally {
      database.close();
    }
  },

  // NEW: Query method that targets a specific drive DB.
  queryDrive: async (
    driveId: string,
    sql: string,
    params?: any[]
  ): Promise<any[]> => {
    const dbPath = getDriveDbPath(driveId);
    const dbDir = path.dirname(dbPath);
    ensureDirectorySync(dbDir); // Ensure the directory for the drive DB exists

    const database = new Database(dbPath);
    try {
      configureDatabase(database);

      // Check if the drive database is newly created and apply its schema
      const tables = database
        .prepare("SELECT name FROM sqlite_master WHERE type='table';")
        .all();
      if (tables.length === 0 && DRIVE_SCHEMA.trim().length > 0) {
        console.log(
          `Initializing drive database schema for ${driveId} during queryDrive from file...`
        );
        database.exec(DRIVE_SCHEMA);
      }

      const stmt = database.prepare(sql);
      return stmt.all(...(params || []));
    } finally {
      database.close();
    }
  },
};

// Additional helpers for more control
export const dbHelpers = {
  // For factory database operations
  withFactory<T>(callback: (db: Database.Database) => T): T {
    const dbPath = getFactoryDbPath();
    const database = new Database(dbPath);
    try {
      configureDatabase(database);
      // Ensure factory schema is applied if the database is new or empty
      const tables = database
        .prepare(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='factory_api_keys';"
        )
        .get();
      if (!tables) {
        console.log("Initializing factory database schema from file...");
        database.exec(FACTORY_SCHEMA);
      }
      return callback(database);
    } finally {
      database.close();
    }
  },

  // For drive database operations
  withDrive<T>(driveId: string, callback: (db: Database.Database) => T): T {
    const dbPath = getDriveDbPath(driveId);
    const dbDir = path.dirname(dbPath);
    ensureDirectorySync(dbDir);

    const database = new Database(dbPath);
    try {
      configureDatabase(database);
      // Check if the drive database is newly created and apply its schema
      const tables = database
        .prepare("SELECT name FROM sqlite_master WHERE type='table';")
        .all();
      if (tables.length === 0 && DRIVE_SCHEMA.trim().length > 0) {
        // Only apply if the DB is empty and schema exists
        console.log(
          `Initializing drive database schema for ${driveId} from file...`
        );
        database.exec(DRIVE_SCHEMA);
      }
      return callback(database);
    } finally {
      database.close();
    }
  },

  // For transactions
  transaction<T>(
    dbType: "factory" | "drive",
    identifier: string | null, // driveId for 'drive' type, null for 'factory'
    callback: (db: Database.Database) => T // This is the function you want to execute within the transaction
  ): T {
    let dbPath: string;
    if (dbType === "drive") {
      if (!identifier) {
        throw new Error(
          "driveId must be provided for 'drive' database type transaction."
        );
      }
      dbPath = getDriveDbPath(identifier);
      const dbDir = path.dirname(dbPath);
      ensureDirectorySync(dbDir);
    } else {
      // dbType === "factory"
      dbPath = getFactoryDbPath();
    }

    const database = new Database(dbPath);
    try {
      configureDatabase(database);

      // Apply schema if new database (simplified check)
      const tables = database
        .prepare("SELECT name FROM sqlite_master WHERE type='table';")
        .all();
      if (tables.length === 0) {
        if (dbType === "factory") {
          console.log(
            "Initializing factory database schema during transaction from file..."
          );
          database.exec(FACTORY_SCHEMA);
        } else if (dbType === "drive" && DRIVE_SCHEMA.trim().length > 0) {
          console.log(
            `Initializing drive database schema for ${identifier} during transaction from file...`
          );
          database.exec(DRIVE_SCHEMA);
        }
      }

      // Create a wrapper function for the transaction.
      // This wrapper will call your original `callback`, passing the `database` instance.
      // The transaction function returned by `database.transaction` will be designed
      // to take no arguments itself.
      const runTransaction = database.transaction(() => {
        // Here, 'this' refers to the database instance within better-sqlite3's context,
        // but it's safer and clearer to use the 'database' variable from the outer scope
        // or explicitly pass it if the transaction wrapper itself takes arguments.
        // Since the inner anonymous function takes no args, we'll just call your callback
        // with the 'database' instance we already have in scope.
        return callback(database);
      });

      // Now, when you call `runTransaction()`, it will execute the anonymous function
      // which in turn calls your `callback` with the `database` instance.
      // The `runTransaction` function itself (as typed by better-sqlite3 for this setup)
      // will correctly expect 0 arguments.
      return runTransaction();
    } finally {
      database.close();
    }
  },
};

// Initialize factory database schema on startup
export function initializeDatabase(): void {
  ensureDirectorySync(DATA_DIR);
  ensureDirectorySync(path.join(DATA_DIR, "drives"));

  // This will ensure the factory DB and its schema are set up on app start
  dbHelpers.withFactory(() => {
    console.log("Factory database initialized or already exists.");
  });
}
