// src/lib/aws-s3.ts
import { createHmac, createHash } from "crypto";
import fetch, {
  RequestInit,
  Response as NodeFetchResponse,
  HeadersInit,
} from "node-fetch";
import { getContentTypeFromExtension } from "../../api/helpers";

// Define the types needed for the functions
export interface AwsBucketAuth {
  access_key: string;
  secret_key: string;
  region: string;
  bucket: string;
  endpoint?: string; // Optional, for S3-compatible endpoints like Cloudflare R2 or MinIO
}

export type DiskID = string;
export type FileID = string;
export type DriveID = string;

export interface DiskUploadResponse {
  url: string;
  fields: Record<string, string>;
}

// Helper type for Rust-like Result
type Result<T, E> = { ok: T; err?: never } | { ok?: never; err: E };

const DEFAULT_EXPIRATION_SECONDS = 60 * 60 * 24; // 24 hours

// --- UTILITY FUNCTIONS ---

/**
 * Gets the current time in nanoseconds.
 * In a Node.js environment, this is an approximation. For a real IC environment, use `ic.time()`.
 * @returns Current time in nanoseconds as a BigInt.
 */
function get_current_time_nanos(): bigint {
  // Date.now() is in milliseconds. Convert to nanoseconds.
  return BigInt(Date.now()) * 1_000_000n;
}

/**
 * Formats a nanosecond timestamp into YYYYMMDD format (UTC).
 * @param timeNs - Timestamp in nanoseconds.
 */
function format_date(timeNs: bigint): string {
  const date = new Date(Number(timeNs / 1_000_000n)); // Convert ns to ms
  const year = date.getUTCFullYear();
  const month = (date.getUTCMonth() + 1).toString().padStart(2, "0");
  const day = date.getUTCDate().toString().padStart(2, "0");
  return `${year}${month}${day}`;
}

/**
 * Formats a nanosecond timestamp into YYYYMMDDTHHMMSSZ format (UTC).
 * @param timeNs - Timestamp in nanoseconds.
 */
function format_datetime(timeNs: bigint): string {
  const date = new Date(Number(timeNs / 1_000_000n)); // Convert ns to ms
  const year = date.getUTCFullYear();
  const month = (date.getUTCMonth() + 1).toString().padStart(2, "0");
  const day = date.getUTCDate().toString().padStart(2, "0");
  const hours = date.getUTCHours().toString().padStart(2, "0");
  const minutes = date.getUTCMinutes().toString().padStart(2, "0");
  const seconds = date.getUTCSeconds().toString().padStart(2, "0");
  return `${year}${month}${day}T${hours}${minutes}${seconds}Z`;
}

/**
 * Formats a nanosecond timestamp into full ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ).
 * @param timeNs - Timestamp in nanoseconds.
 */
function formatIso8601(timeNs: bigint): string {
  const date = new Date(Number(timeNs / 1_000_000n)); // Convert ns to ms
  return date.toISOString().replace(/\.\d{3}Z$/, "Z"); // Remove milliseconds
}

/**
 * SHA256 hash function.
 * @param data - The data to hash.
 * @returns The SHA256 hash as a Buffer.
 */
function sha256Hash(data: Buffer | string): Buffer {
  return createHash("sha256").update(data).digest();
}

/**
 * HMAC-SHA256 function.
 * @param key - The key for the HMAC.
 * @param data - The data to sign.
 * @returns The HMAC-SHA256 signature as a Buffer.
 */
function hmacSha256(key: Buffer, data: Buffer | string): Buffer {
  return createHmac("sha256", key).update(data).digest();
}

/**
 * Derives the AWS Signature V4 signing key.
 */
function deriveSigningKey(
  secret: string,
  date: string,
  region: string,
  service: string
): Buffer {
  const kDate = hmacSha256(Buffer.from(`AWS4${secret}`), date);
  const kRegion = hmacSha256(kDate, region);
  const kService = hmacSha256(kRegion, service);
  return hmacSha256(kService, "aws4_request");
}

/**
 * Custom URL encoding that matches AWS's specific requirements.
 * Avoids encoding unreserved characters: A-Z a-z 0-9 - _ . ~
 */
function url_encode(s: string): string {
  let encoded = "";
  const bytes = Buffer.from(s, "utf-8");

  for (const byte of bytes) {
    if (
      (byte >= 0x41 && byte <= 0x5a) || // A-Z
      (byte >= 0x61 && byte <= 0x7a) || // a-z
      (byte >= 0x30 && byte <= 0x39) || // 0-9
      byte === 0x2d || // - (hyphen)
      byte === 0x2e || // . (period)
      byte === 0x5f || // _ (underscore) <-- THIS WAS THE FIX
      byte === 0x7e // ~ (tilde)
    ) {
      encoded += String.fromCharCode(byte);
    } else {
      encoded += `%${byte.toString(16).toUpperCase().padStart(2, "0")}`;
    }
  }
  return encoded;
}

// --- CORE S3 FUNCTIONS ---

/**
 * Generates a pre-signed URL for uploading a file to S3 using a POST request.
 * @returns A `DiskUploadResponse` containing the URL and form fields.
 */
export function generate_s3_upload_url(
  fileId: FileID,
  fileExtension: string,
  auth: AwsBucketAuth,
  driveId: DriveID,
  maxSize: bigint,
  expiresIn: bigint,
  diskId: DiskID,
  downloadFilename: string
): Result<DiskUploadResponse, string> {
  const currentTime = get_current_time_nanos();
  const expirationTime = currentTime + BigInt(expiresIn) * 1_000_000_000n;

  const date = format_date(currentTime);
  const dateTime = format_datetime(currentTime);
  const expiration = formatIso8601(expirationTime);

  const targetKey = `${driveId}/${diskId}/${fileId}/${fileId}.${fileExtension}`;
  const credential = `${auth.access_key}/${date}/${auth.region}/s3/aws4_request`;

  const _policy = {
    expiration: expiration,
    conditions: [
      { bucket: auth.bucket },
      { key: targetKey },
      { acl: "private" },
      ["content-length-range", 0, maxSize.toString()],
      { "x-amz-algorithm": "AWS4-HMAC-SHA256" },
      { "x-amz-credential": credential },
      { "x-amz-date": dateTime },
      { "Content-Disposition": "inline" },
    ],
  };

  // Use the helper function to get the content type
  const contentType = getContentTypeFromExtension(fileExtension);

  // Conditionally add Content-Type to fields and policy
  if (contentType) {
    // @ts-ignore
    _policy.conditions.push({ "Content-Type": contentType });
  }

  const policy = JSON.stringify(_policy);

  const policyBase64 = Buffer.from(policy).toString("base64");
  const signingKey = deriveSigningKey(auth.secret_key, date, auth.region, "s3");
  const signature = hmacSha256(signingKey, policyBase64).toString("hex");

  const fields: Record<string, string> = {
    key: targetKey,
    acl: "private",
    "Content-Disposition": "inline",
    "x-amz-algorithm": "AWS4-HMAC-SHA256",
    "x-amz-credential": credential,
    "x-amz-date": dateTime,
    policy: policyBase64,
    "x-amz-signature": signature,
  };
  if (contentType) {
    fields["Content-Type"] = contentType;
  }

  const url = auth.endpoint
    ? `${auth.endpoint}/${auth.bucket}`
    : `https://${auth.bucket}.s3.${auth.region}.amazonaws.com`;

  return { ok: { url, fields } };
}

function hmac_sha256(key: Buffer | string, data: Buffer | string): Buffer {
  return createHmac("sha256", key).update(data).digest();
}

function sha256_hash(data: Buffer | string): Buffer {
  return createHash("sha256").update(data).digest();
}

function derive_signing_key(
  secret: string,
  date: string,
  region: string,
  service: string
): Buffer {
  const kDate = hmac_sha256(`AWS4${secret}`, date);
  const kRegion = hmac_sha256(kDate, region);
  const kService = hmac_sha256(kRegion, service);
  const kSigning = hmac_sha256(kService, "aws4_request");
  return kSigning;
}

/**
 * Generates a pre-signed URL for viewing/downloading an S3 object.
 * @returns A pre-signed URL as a string.
 */
export function generate_s3_view_url(
  file_id: FileID,
  file_extension: string,
  auth: AwsBucketAuth,
  drive_id: DriveID,
  expires_in: number,
  download_filename: string,
  disk_id: DiskID
): string {
  const currentTime = get_current_time_nanos();
  const date = format_date(currentTime);
  const dateTime = format_datetime(currentTime);

  const credential = `${auth.access_key}/${date}/${auth.region}/s3/aws4_request`;
  const expiration = (expires_in ?? DEFAULT_EXPIRATION_SECONDS).toString();
  const host = `${auth.bucket}.s3.${auth.region}.amazonaws.com`;
  const s3Key = `${drive_id}/${disk_id}/${file_id}/${file_id}.${file_extension}`;
  const contentDisposition = "inline";

  // 1. Build query parameters
  const queryParams: [string, string][] = [
    ["X-Amz-Algorithm", "AWS4-HMAC-SHA256"],
    ["X-Amz-Credential", credential],
    ["X-Amz-Date", dateTime],
    ["X-Amz-Expires", expiration],
    ["X-Amz-SignedHeaders", "host"],
    ["response-content-disposition", contentDisposition],
  ];

  // 2. Sort query parameters by key name (byte order)
  queryParams.sort((a, b) => (a[0] < b[0] ? -1 : 1));

  // 3. Create the canonical query string from the sorted parameters
  const canonicalQueryString = queryParams
    .map(([k, v]) => `${url_encode(k)}=${url_encode(v)}`)
    .join("&");

  // 4. Create the canonical request
  const canonicalHeaders = `host:${host}\n`;
  const signedHeaders = "host";
  const canonicalRequest = `GET\n/${s3Key}\n${canonicalQueryString}\n${canonicalHeaders}\n${signedHeaders}\nUNSIGNED-PAYLOAD`;

  // 5. Create the string to sign
  const stringToSign = `AWS4-HMAC-SHA256\n${dateTime}\n${date}/${auth.region}/s3/aws4_request\n${sha256_hash(canonicalRequest).toString("hex")}`;

  // 6. Calculate the signature
  const signingKey = derive_signing_key(
    auth.secret_key,
    date,
    auth.region,
    "s3"
  );
  const signature = hmac_sha256(signingKey, stringToSign).toString("hex");

  // 7. Construct the final URL
  return `https://${host}/${s3Key}?${canonicalQueryString}&X-Amz-Signature=${signature}`;
}
