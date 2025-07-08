// src/services/database.ts
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
  db.pragma("cache_size = -2000");
  db.pragma("temp_store = MEMORY");
  db.pragma("mmap_size = 30000000");
  db.pragma("busy_timeout = 5000");
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

// Main database service interface for compatibility with your handlers
export const db = {
  // Query method that your handlers expect
  query: async (sql: string, params?: any[]): Promise<any[]> => {
    const database = new Database(getFactoryDbPath());
    try {
      configureDatabase(database);
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
    const database = new Database(getFactoryDbPath());
    try {
      configureDatabase(database);
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
      return callback(database);
    } finally {
      database.close();
    }
  },

  // For transactions
  transaction<T>(
    driveId: string | null,
    callback: (db: Database.Database) => T
  ): T {
    const dbPath = driveId ? getDriveDbPath(driveId) : getFactoryDbPath();
    if (driveId) {
      const dbDir = path.dirname(dbPath);
      ensureDirectorySync(dbDir);
    }

    const database = new Database(dbPath);
    try {
      configureDatabase(database);
      const transaction = database.transaction(callback);
      return transaction(database);
    } finally {
      database.close();
    }
  },
};

// Initialize factory database schema on startup
export function initializeDatabase(): void {
  ensureDirectorySync(DATA_DIR);
  ensureDirectorySync(path.join(DATA_DIR, "drives"));

  dbHelpers.withFactory((database) => {
    database.exec(`
      CREATE TABLE IF NOT EXISTS factory_api_keys (
        id TEXT PRIMARY KEY,
        value TEXT UNIQUE NOT NULL,
        user_id TEXT NOT NULL,
        name TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        expires_at INTEGER NOT NULL,
        is_revoked INTEGER NOT NULL DEFAULT 0
      );
      
      CREATE INDEX IF NOT EXISTS idx_api_keys_value ON factory_api_keys(value);
      CREATE INDEX IF NOT EXISTS idx_api_keys_user_id ON factory_api_keys(user_id);
      
      -- Add other tables as needed
    `);
  });
}
