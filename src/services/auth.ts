// src/services/auth.ts

import { Principal } from "@dfinity/principal";
import * as ed from "@noble/ed25519";
import { sign, verify, getPublicKey, utils } from "@noble/ed25519";
import { mnemonicToSeed, validateMnemonic } from "bip39";
import { getAddress, HDNodeWallet } from "ethers";
import {
  toByteArray as base64Decode,
  fromByteArray as base64Encode,
} from "base64-js";
import { FastifyRequest } from "fastify"; // Import FastifyRequest
import {
  ApiKey,
  ApiKeyID,
  ApiKeyProof,
  ApiKeyValue,
  AuthJsonDecoded,
  AuthTypeEnum,
  DriveID,
  FactoryApiKey,
  IDPrefixEnum,
  SignatureProof,
  UserID,
} from "@officexapp/types";
import { v4 as uuidv4 } from "uuid";
import { db } from "./database";
import { webcrypto } from "node:crypto";
import { sha512 } from "@noble/hashes/sha512";

ed.etc.sha512Sync = (...m) => sha512(ed.etc.concatBytes(...m));

export interface HttpResponse {
  statusCode: number;
  body: string;
  headers?: [string, string][];
}

export class ErrorResponse {
  code: number;
  message: string;

  constructor(code: number, message: string) {
    this.code = code;
    this.message = message;
  }

  static unauthorized(): ErrorResponse {
    return new ErrorResponse(401, "Unauthorized");
  }

  static err(code: number, message: string): ErrorResponse {
    return new ErrorResponse(code, message);
  }

  encode(): Uint8Array {
    // Simple JSON encoding for demonstration.
    // In a real application, you might have a more structured error response.
    return new TextEncoder().encode(JSON.stringify(this));
  }
}

export interface WalletAddresses {
  icp_principal: string;
  evm_public_address: string;
}

export class SeedPhraseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SeedPhraseError";
  }
}

// --- Canister State (Replace with actual backend/DB integration) ---
// In a real application, these would be fetched from a database or a persistent store.
const APIKEYS_BY_ID_HASHTABLE = new Map<ApiKeyID, ApiKey>();
const APIKEYS_BY_VALUE_HASHTABLE = new Map<ApiKeyValue, ApiKeyID>();

// Helper function to get current time in nanoseconds (BigInt)
function get_current_nanoseconds(): bigint {
  return BigInt(Date.now()) * 1_000_000n; // Convert milliseconds to nanoseconds
}

// --- Utility Functions ---

function debug_log(...args: any[]): void {
  console.log("[DEBUG]", ...args);
}

function format_user_id(principalText: string): UserID {
  // Implement your user ID formatting logic here, likely based on the principal
  return `${IDPrefixEnum.User}${principalText}`;
}

function create_response(
  statusCode: number,
  body: string,
  headers: [string, string][] = []
): HttpResponse {
  return { statusCode, body, headers };
}

// --- Main Authentication Logic ---

export async function authenticateRequest(
  req: FastifyRequest, // Changed to FastifyRequest
  appType: "factory" | "drive",
  orgID?: DriveID
): Promise<ApiKey | FactoryApiKey | null> {
  let btoaToken: string | null = null;

  // Access Authorization header directly from req.headers
  const authHeader = req.headers.authorization;
  if (authHeader) {
    if (authHeader.startsWith("Bearer ")) {
      btoaToken = authHeader.substring("Bearer ".length).trim();
      debug_log("Found token in Authorization header");
    } else {
      debug_log("Authorization header not in Bearer format");
    }
  }

  // If no token from header, try query parameter
  if (btoaToken === null) {
    // Fastify automatically parses query parameters into req.query
    // We assume 'auth' query parameter is a string. Cast for type safety.
    const queryAuth = (req.query as { auth?: string }).auth;
    if (queryAuth) {
      debug_log(`Found auth query parameter: ${queryAuth}`);
      btoaToken = queryAuth;
    }
  }

  // If no token found in either place, return null
  if (btoaToken === null) {
    debug_log("No authentication token found in header or query parameter");
    return null;
  }

  // Pad the Base64 token
  const paddedToken =
    btoaToken.length % 4 === 0
      ? btoaToken
      : `${btoaToken}${"=".repeat(4 - (btoaToken.length % 4))}`;

  let stringifiedToken: string;
  try {
    const decoded = base64Decode(paddedToken);
    stringifiedToken = new TextDecoder().decode(decoded);
  } catch (e) {
    debug_log(`Failed to decode base64 btoa_token: ${e}`);
    return null;
  }

  let authJson: AuthJsonDecoded;
  try {
    authJson = JSON.parse(stringifiedToken);
  } catch (e) {
    debug_log(`Failed to parse JSON proof: ${e}`);
    return null;
  }

  debug_log("auth_json:", authJson);

  // Handle different authentication types
  if (authJson.auth_type === AuthTypeEnum.Signature) {
    const proof = authJson as SignatureProof;

    // Check challenge timestamp (must be within 30 seconds)
    const now = Number(get_current_nanoseconds() / 1_000_000n); // Convert ns to ms
    if (now > proof.challenge.timestamp_ms + 30_000) {
      debug_log("Signature challenge expired");
      return null;
    }

    // Serialize the challenge as was signed.
    const challengeBytes = new TextEncoder().encode(
      JSON.stringify(proof.challenge)
    );

    // The raw public key (32 bytes) as provided in the challenge.
    const publicKeyBytes = new Uint8Array(proof.challenge.self_auth_principal);
    if (publicKeyBytes.length !== 32) {
      debug_log(
        `Expected 32-byte raw public key, got ${publicKeyBytes.length} bytes`
      );
      return null;
    }

    try {
      const signatureBytes = new Uint8Array(proof.signature);
      const isValid = await verify(
        signatureBytes,
        challengeBytes,
        publicKeyBytes
      );

      if (!isValid) {
        debug_log("Signature verification failed");
        return null;
      }

      debug_log("Signature verification successful");

      // To compute the canonical principal that matches getPrincipal(),
      // first convert the raw public key into DER format by prepending the header.
      const derHeader = new Uint8Array([
        0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
      ]);
      const derKey = new Uint8Array(derHeader.length + publicKeyBytes.length);
      derKey.set(derHeader);
      derKey.set(publicKeyBytes, derHeader.length);

      // Compute the canonical principal using the DER-encoded key.
      const computedPrincipal = Principal.selfAuthenticating(derKey).toText();

      // Compare with the canonical_principal included in the challenge.
      if (computedPrincipal !== proof.challenge.canonical_principal) {
        debug_log(
          `Mismatch between computed and provided canonical principal: ${computedPrincipal} vs ${proof.challenge.canonical_principal}`
        );
        return null;
      }
      debug_log(`Successfully authenticated user: ${computedPrincipal}`);

      if (orgID) {
        update_last_online_at(format_user_id(computedPrincipal), orgID);
      }

      // Create and return an API key based on the computed principal.
      return {
        id: `sig_auth_${now}`,
        value: `signature_auth_${computedPrincipal}`,
        user_id: format_user_id(computedPrincipal),
        name: `Signature Authenticated User ${computedPrincipal}`,
        private_note: undefined,
        created_at: now,
        begins_at: 0,
        expires_at: -1,
        is_revoked: false,
        labels: [],
        external_id: undefined,
        external_payload: undefined,
      } as ApiKey;
    } catch (e) {
      debug_log(`Signature verification failed: ${e}`);
      return null;
    }
  } else if (authJson.auth_type === AuthTypeEnum.ApiKey) {
    // API key authentication
    const apiKeyValue = btoaToken; // The full btoa_token is the API key value
    debug_log(`Looking up API key from payload: ${apiKeyValue}`);

    let fullApiKey: ApiKey | FactoryApiKey | null = null;
    const now = Number(get_current_nanoseconds() / 1_000_000n);

    if (appType === "factory") {
      try {
        const result = await db.queryFactory(
          `SELECT id, value, user_id, name, created_at, expires_at, is_revoked FROM factory_api_keys WHERE value = ?`,
          [apiKeyValue]
        );
        if (result.length > 0) {
          fullApiKey = result[0] as FactoryApiKey;
        }
      } catch (e) {
        debug_log(`Error querying factory_api_keys: ${e}`);
        return null;
      }
    } else if (appType === "drive") {
      // Now use the provided orgID directly
      if (!orgID) {
        debug_log(
          "orgID (DriveID) not provided for drive appType API key authentication."
        );
        return null;
      }

      try {
        const result = await db.queryDrive(
          orgID, // Use orgID here
          `SELECT id, value, user_id, name, private_note, created_at, begins_at, expires_at, is_revoked, external_id, external_payload FROM api_keys WHERE value = ?`,
          [apiKeyValue]
        );
        if (result.length > 0) {
          fullApiKey = result[0] as ApiKey;
        }
      } catch (e) {
        debug_log(`Error querying api_keys for drive ${orgID}: ${e}`);
        return null;
      }
    }

    if (fullApiKey) {
      debug_log(
        `Found API Key: ${fullApiKey.id} for user: ${fullApiKey.user_id}`
      );
      debug_log(`Key expires at: ${fullApiKey.expires_at}, now: ${now}`);
      debug_log(`Is revoked: ${fullApiKey.is_revoked}`);
      if (appType === "drive") {
        debug_log(`Begins at: ${(fullApiKey as ApiKey).begins_at}`);
      }

      const isBeginsValid =
        appType === "factory" || (fullApiKey as ApiKey).begins_at <= now;

      if (
        (fullApiKey.expires_at <= 0 || now < fullApiKey.expires_at) &&
        !fullApiKey.is_revoked &&
        isBeginsValid
      ) {
        // Pass appType and orgID to update_last_online_at
        if (orgID) {
          await update_last_online_at(fullApiKey.user_id, orgID);
        }
        return fullApiKey;
      }
    }
    debug_log("API key authentication failed: Key invalid or expired/revoked.");
    return null;
  }

  return null;
}

export function create_auth_error_response(): HttpResponse {
  const body = new TextDecoder().decode(ErrorResponse.unauthorized().encode());
  return create_response(401, body);
}

export function create_raw_upload_error_response(
  error_msg: string
): HttpResponse {
  const errorStruct = ErrorResponse.err(400, error_msg);
  const body = new TextDecoder().decode(errorStruct.encode());
  return create_response(400, body);
}

// --- Wallet Address Derivation ---

/**
 * Converts a BIP39 seed phrase to ICP principal and EVM address.
 *
 * This function handles the cryptographic derivation of blockchain addresses
 * from a standard mnemonic seed phrase. It uses industry-standard libraries
 * for both ICP (Ed25519) and EVM (secp256k1) key derivation.
 */
export async function seed_phrase_to_wallet_addresses(
  seed_phrase: string
): Promise<WalletAddresses> {
  // Validate the mnemonic phrase
  if (!validateMnemonic(seed_phrase)) {
    // Use validateMnemonic directly
    throw new SeedPhraseError("Invalid mnemonic seed phrase");
  }

  // Generate the 512-bit seed from the mnemonic
  const seedBuffer = await mnemonicToSeed(seed_phrase); // Use mnemonicToSeed directly
  const seedBytes = new Uint8Array(seedBuffer); // Convert to Uint8Array for consistency

  // ---- ICP Principal Generation (Ed25519) ----

  // The ICP self-authenticating principal typically uses the first 32 bytes of the seed
  // as the Ed25519 private key.
  const ed25519PrivateKey = seedBytes.slice(0, 32);

  // Derive the Ed25519 public key from the private key using getPublicKey
  const ed25519PublicKey = await getPublicKey(ed25519PrivateKey); // This is a Uint8Array (32 bytes)

  // To compute the canonical principal, convert the raw public key into DER format
  // by prepending the standard Ed25519 DER header.
  const derHeader = new Uint8Array([
    0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
  ]);
  const derKey = new Uint8Array(derHeader.length + ed25519PublicKey.length);
  derKey.set(derHeader);
  derKey.set(ed25519PublicKey, derHeader.length);

  // Compute the self-authenticating principal using the DER-encoded key.
  const principal = Principal.selfAuthenticating(derKey);
  const icp_principal = principal.toText();

  // ---- EVM Address Generation (secp256k1 - using ethers.js BIP39/BIP44) ----

  // ethers.js's HDNodeWallet can directly derive from the BIP39 seed.
  // It handles the secp256k1 key derivation according to standard paths (e.g., m/44'/60'/0'/0/0 for Ethereum).
  const ethWallet = HDNodeWallet.fromSeed(seedBuffer);

  // The `address` property of the Wallet object provides the checksummed Ethereum address.
  const evm_public_address = getAddress(ethWallet.address); // Use getAddress for checksumming

  return {
    icp_principal,
    evm_public_address,
  };
}

export async function generateApiKey(): Promise<string> {
  const input = `${IDPrefixEnum.ApiKey}${uuidv4()}`;
  const salt = Date.now();
  const combined = `${input}${salt}`;

  function arrayBufferToHex(buffer: ArrayBuffer): string {
    const uint8Array = new Uint8Array(buffer);
    return Array.from(uint8Array)
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  }

  // Use Web Crypto API for hashing
  const encoder = new TextEncoder();
  const data = encoder.encode(combined);
  const hashBuffer = await crypto.subtle.digest("SHA-256", data);
  const apiKeyInnerValue = arrayBufferToHex(hashBuffer);

  const apiKeyProof: ApiKeyProof = {
    auth_type: AuthTypeEnum.ApiKey,
    value: apiKeyInnerValue,
  };

  // Serialize to JSON
  const jsonPayload = JSON.stringify(apiKeyProof);

  // Base64 encode the JSON
  return btoa(jsonPayload);
}

async function update_last_online_at(
  userId: UserID,
  orgID: DriveID
): Promise<void> {
  const now = Date.now(); // Milliseconds
  // Update last_online_ms in the contacts table of the specific drive DB
  try {
    await db.queryDrive(
      orgID,
      `UPDATE contacts SET last_online_ms = ? WHERE id = ?`,
      [now, userId]
    );
    debug_log(`Updated last online for user ${userId} in drive ${orgID}`);
  } catch (error) {
    console.error(
      `Failed to update last online for drive user ${userId} in drive ${orgID}:`,
      error
    );
  }
}
