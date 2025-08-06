// src/services/database.ts
import Database from "better-sqlite3";
import path from "path";
import fs from "fs";
import {
  DriveID,
  IErrorResponse,
  IRequestSearchDrive,
  IResponseSearchDrive,
  SearchCategoryEnum,
  SearchResult,
  SearchSortByEnum,
  SortDirection,
  UserID,
} from "@officexapp/types";

const DATA_DIR = process.env.DATA_DIR || "/data";

// Store a reference to the factory database connection
// This will be null until initFactoryDB is called.
let factoryDbInstance: Database.Database | null = null;

// Main database service interface for compatibility with your handlers
export const db = {
  // Query method that your handlers expect. This method will ALWAYS target the factory DB.
  queryFactory: async (sql: string, params?: any[]): Promise<any[]> => {
    // Ensure the factory DB is initialized before querying
    await initFactoryDB();
    if (!factoryDbInstance) {
      throw new Error("Factory database not initialized.");
    }

    try {
      // We are using a pre-existing, long-lived connection for the factory DB
      const stmt = factoryDbInstance.prepare(sql);
      return stmt.all(...(params || []));
    } catch (error) {
      console.error("Error in queryFactory:", error);
      throw error;
    }
  },
  runFactory: async (sql: string, params?: any[]): Promise<void> => {
    await initFactoryDB();
    if (!factoryDbInstance) {
      throw new Error("Factory database not initialized.");
    }
    try {
      const stmt = factoryDbInstance.prepare(sql);
      stmt.run(...(params || []));
    } catch (error) {
      console.error("Error in runFactory:", error);
      throw error;
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

  runDrive: async (
    driveId: string,
    sql: string,
    params?: any[]
  ): Promise<void> => {
    const dbPath = getDriveDbPath(driveId);
    const dbDir = path.dirname(dbPath);
    ensureDirectorySync(dbDir);

    const database = new Database(dbPath);
    try {
      configureDatabase(database);

      const tables = database
        .prepare("SELECT name FROM sqlite_master WHERE type='table';")
        .all();
      if (tables.length === 0 && DRIVE_SCHEMA.trim().length > 0) {
        database.exec(DRIVE_SCHEMA);
      }

      const stmt = database.prepare(sql);
      stmt.run(...(params || []));
    } finally {
      database.close();
    }
  },

  /**
   * Performs a fuzzy text search on the specified drive database.
   *
   * @param driveId The ID of the drive to search within.
   * @param payload The search request payload containing query, categories, pagination, and sorting options.
   * @returns A promise that resolves with the search results.
   */
  fuzzySearch: async (
    driveId: string,
    payload: IRequestSearchDrive
  ): Promise<IResponseSearchDrive | IErrorResponse> => {
    const {
      query,
      categories,
      page_size = 20, // Default page size
      cursor,
      sort_by = SearchSortByEnum.RELEVANCE, // Default sort by relevance
      direction = SortDirection.DESC, // Default sort direction
    } = payload;

    const dbPath = getDriveDbPath(driveId);
    const dbDir = path.dirname(dbPath);
    ensureDirectorySync(dbDir);

    const database = new Database(dbPath);
    try {
      configureDatabase(database);

      // --- Build the WHERE clause for categories ---
      let categoryWhereClause = "";
      const queryParams: any[] = [`${query}`]; // Start with the FTS query parameter

      if (
        categories &&
        categories.length > 0 &&
        !categories.includes(SearchCategoryEnum.ALL)
      ) {
        // Filter by specific categories
        const placeholders = categories.map(() => "?").join(", ");
        categoryWhereClause = `AND category IN (${placeholders})`;
        queryParams.push(...categories);
      }

      // --- Build the ORDER BY clause ---
      let orderByClause = "";
      switch (sort_by) {
        case SearchSortByEnum.RELEVANCE:
        case SearchSortByEnum.SCORE:
          // FTS5's rank function provides relevance scoring
          orderByClause = `ORDER BY rank ${direction}`;
          break;
        case SearchSortByEnum.ALPHABETICAL:
          orderByClause = `ORDER BY title COLLATE NOCASE ${direction}`;
          break;
        case SearchSortByEnum.CREATED_AT:
          orderByClause = `ORDER BY created_at ${direction}`;
          break;
        case SearchSortByEnum.UPDATED_AT:
          orderByClause = `ORDER BY updated_at ${direction}`;
          break;
        default:
          orderByClause = `ORDER BY rank ${direction}`; // Default to relevance
          break;
      }

      // --- Handle Pagination (LIMIT and OFFSET) ---
      const limit = Math.max(1, Math.min(page_size, 1000)); // Ensure page_size is within bounds
      let offset = 0;
      if (cursor) {
        // Assuming cursor is a string representation of the offset for simplicity
        const parsedCursor = parseInt(cursor, 10);
        if (!isNaN(parsedCursor) && parsedCursor >= 0) {
          offset = parsedCursor;
        }
      }

      // --- Construct the main search query ---
      const searchSql = `
        SELECT
            resource_id,
            title,
            preview,
            category,
            metadata,
            created_at,
            updated_at,
            searchable_string,
            rank
        FROM search_fts
        WHERE search_fts MATCH ? ${categoryWhereClause}
        ${orderByClause}
        LIMIT ? OFFSET ?;
      `;

      // Add limit and offset to query parameters
      const finalSearchParams = [...queryParams, limit, offset];

      const stmt = database.prepare(searchSql);
      const results = stmt.all(...finalSearchParams);

      // --- Map results to SearchResult interface ---
      const items: SearchResult[] = results.map((row: any) => ({
        resource_id: row.resource_id,
        title: row.title,
        preview: row.preview,
        score: row.rank, // Map FTS5's rank to SearchResult's score
        category: row.category,
        metadata: row.metadata,
        created_at: row.created_at,
        updated_at: row.updated_at,
        // searchable_string is not part of the public SearchResult interface,
        // but it's useful for debugging or internal use if needed.
      }));

      // --- Get total count for pagination metadata ---
      const countSql = `
        SELECT COUNT(*) as total
        FROM search_fts
        WHERE search_fts MATCH ? ${categoryWhereClause};
      `;
      const countParams = [...queryParams]; // Use the same query params as the search

      const totalResult = database.prepare(countSql).get(...countParams) as {
        total: number;
      };
      const total = totalResult.total;

      // --- Calculate next cursor ---
      const nextCursor =
        offset + items.length < total
          ? String(offset + items.length)
          : undefined;

      return {
        ok: {
          data: {
            items,
            page_size: limit,
            total,
            direction,
            cursor: nextCursor,
          },
        },
      };
    } catch (error: any) {
      console.error("Error in fuzzySearch:", error);
      // Return a failure response in case of an error
      return {
        err: {
          code: 500,
          message: error.message || "Internal server error.",
        },
      };
    }
  },
};

// Additional helpers for more control
export const dbHelpers = {
  // For factory database operations
  withFactory<T>(callback: (db: Database.Database) => T): T {
    // Ensure the factory DB is initialized and its instance is available
    if (!factoryDbInstance) {
      throw new Error(
        "Factory database has not been initialized. Call initFactoryDB first."
      );
    }
    try {
      return callback(factoryDbInstance);
    } catch (error) {
      console.error("Error in withFactory:", error);
      throw error;
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
    let database: Database.Database;

    if (dbType === "drive") {
      if (!identifier) {
        throw new Error(
          "driveId must be provided for 'drive' database type transaction."
        );
      }
      dbPath = getDriveDbPath(identifier);
      const dbDir = path.dirname(dbPath);
      ensureDirectorySync(dbDir);
      database = new Database(dbPath);
    } else {
      // dbType === "factory"
      if (!factoryDbInstance) {
        throw new Error(
          "Factory database has not been initialized. Call initFactoryDB first."
        );
      }
      database = factoryDbInstance; // Use the long-lived factory DB instance
    }

    try {
      configureDatabase(database);

      // Apply schema if new database (simplified check) - this block is now mostly for drive DBs
      // as factory DB schema application is handled by initFactoryDB
      if (dbType === "drive") {
        const tables = database
          .prepare("SELECT name FROM sqlite_master WHERE type='table';")
          .all();
        if (tables.length === 0 && DRIVE_SCHEMA.trim().length > 0) {
          console.log(
            `Initializing drive database schema for ${identifier} during transaction from file...`
          );
          database.exec(DRIVE_SCHEMA);
        }
      }

      const runTransaction = database.transaction(() => {
        return callback(database);
      });

      return runTransaction();
    } finally {
      // Only close the database if it's a drive DB, as factoryDbInstance is long-lived
      if (dbType === "drive") {
        database.close();
      }
    }
  },
};

/**
 * Initializes the factory database.
 * 1. Checks if factory DB already exists.
 * 2. If it already exists, sets the global factoryDbInstance and returns it.
 * 3. If it doesn't exist, creates it from the SQL schema, sets the global factoryDbInstance, and returns it.
 * This function ensures that the factory database connection is a singleton.
 * @returns Promise<Database.Database> A promise that resolves with the factory database instance.
 */
export async function initFactoryDB(): Promise<Database.Database> {
  if (factoryDbInstance) {
    console.log(
      "Factory database already initialized, returning existing instance."
    );
    return factoryDbInstance;
  }

  const factoryDbPath = getFactoryDbPath();
  const dbExists = fs.existsSync(factoryDbPath);

  // Ensure the data directory and drives directory exist
  ensureDirectorySync(DATA_DIR);
  ensureDirectorySync(path.join(DATA_DIR, "drives"));
  ensureDirectorySync(path.dirname(factoryDbPath));

  const database = new Database(factoryDbPath);
  configureDatabase(database);

  if (!dbExists) {
    console.log(
      "Factory database does not exist. Creating and initializing schema..."
    );
    try {
      database.exec(FACTORY_SCHEMA);
      console.log("Factory database schema applied.");
    } catch (error) {
      console.error("Error applying factory database schema:", error);
      database.close(); // Close on error
      throw error;
    }
  } else {
    // If it exists, ensure the factory schema is applied (e.g., for migrations or first run with existing empty DB)
    // This is a safety check. For proper migrations, you'd use a dedicated migration system.
    const tables = database
      .prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='factory_api_keys';"
      )
      .get();
    if (!tables && FACTORY_SCHEMA.trim().length > 0) {
      console.log(
        "Factory database exists but schema seems missing. Applying schema..."
      );
      try {
        database.exec(FACTORY_SCHEMA);
        console.log("Factory database schema applied (post-existence check).");
      } catch (error) {
        console.error(
          "Error applying factory database schema on existing DB:",
          error
        );
        database.close(); // Close on error
        throw error;
      }
    } else if (tables) {
      console.log("Factory database already exists and schema is present.");
    } else {
      console.log(
        "Factory database exists but no schema to apply (FACTORY_SCHEMA is empty)."
      );
    }
  }

  factoryDbInstance = database; // Store the long-lived instance
  return database;
}

/**
 * Initializes a specific drive database.
 * 1. Checks if the drive DB already exists.
 * 2. If it doesn't exist, creates it from the SQL schema.
 * This function does NOT cache the database connection and closes it after the operation.
 * @param driveId The ID of the drive for which to initialize the database.
 * @returns Promise<void> A promise that resolves when the drive database is initialized.
 */
export async function initDriveDB(driveId: DriveID): Promise<void> {
  const driveDbPath = getDriveDbPath(driveId);
  const dbExists = fs.existsSync(driveDbPath);

  // Ensure the necessary directory structure for the drive DB exists
  const dbDir = path.dirname(driveDbPath);
  ensureDirectorySync(dbDir); // Still ensure directory exists as it's a prerequisite for any DB operation

  if (!dbExists) {
    // CRITICAL CHANGE: If the database does not exist, throw an error instead of creating it.
    throw new Error(
      `Drive database for ${driveId} not found at ${driveDbPath}. It must be created by the redeemGiftcardSpawnOrgHandler.`
    );
  } else {
    console.log(`Drive database for ${driveId} already exists.`);
    // Optionally, you could still open and close to ensure it's a valid SQLite DB
    // and potentially run PRAGMAs, but for simply "initializing" (meaning, ensuring existence
    // for subsequent operations), just checking `dbExists` is sufficient.
    // For robustness, you might consider opening and immediately closing to validate.
    let database: Database.Database | null = null;
    try {
      database = new Database(driveDbPath);
      configureDatabase(database); // Apply pragmas even if it exists
    } catch (error) {
      console.error(
        `Error opening existing drive database for ${driveId}:`,
        error
      );
      throw new Error(`Failed to open existing drive database for ${driveId}.`);
    } finally {
      if (database) {
        database.close();
      }
    }
  }
}

// Helper to ensure directory exists
export function ensureDirectorySync(dir: string): void {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

// Helper to configure database with optimal settings
export function configureDatabase(db: Database.Database): void {
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.pragma("cache_size = -2000"); // 2GB cache
  db.pragma("temp_store = MEMORY");
  db.pragma("mmap_size = 30000000"); // 30MB mmap
  db.pragma("busy_timeout = 5000"); // 5 seconds
}

// Get factory database path
export function getFactoryDbPath(): string {
  return path.join(DATA_DIR, "factory", "factory.db");
}

// Get drive database path with sharding
export function getDriveDbPath(driveId: DriveID): string {
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
export const MIGRATIONS_DIR = path.join(__dirname, "..", "migrations"); // Adjust path as needed based on your compiled JS output location

export let FACTORY_SCHEMA: string;
export let DRIVE_SCHEMA: string;

// TODO: Check if this is safe, does it auto-skip unapplied migrations? also theres N many databases to update
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
