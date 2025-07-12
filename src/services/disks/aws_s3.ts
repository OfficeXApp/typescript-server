import { createHmac, createHash } from "crypto";
import fetch, {
  RequestInit,
  Response as NodeFetchResponse,
  HeadersInit,
} from "node-fetch"; // Using node-fetch
import { DiskID, FileID, DriveID, DiskUploadResponse } from "@officexapp/types";

export interface AwsBucketAuth {
  access_key: string;
  secret_key: string;
  region: string;
  bucket: string;
  endpoint?: string; // Optional, for custom S3-compatible endpoints
}

const DEFAULT_EXPIRATION_SECONDS = 60 * 60 * 24; // 24 hours

/**
 * Gets the current time in nanoseconds (approximated for Node.js).
 * For a real IC environment, you would use `ic_cdk.api.time()`.
 * @returns Current time in nanoseconds as a BigInt.
 */
function get_current_time_nanos(): bigint {
  // Using process.hrtime.bigint() for high-resolution time in Node.js
  // Note: This is an approximation of IC's nanosecond timestamp.
  // `Date.now()` gives milliseconds, so we multiply to get rough nanoseconds.
  // For consistency with Rust's `ic_cdk::api::time()`, which returns nanoseconds
  // since the Unix epoch, we combine Date.now() with hrtime for better precision
  // although hrtime itself is relative to an arbitrary past point.
  // For simple timestamping that needs to align with AWS signatures, Date.now()
  // converted to nanoseconds might be sufficient.
  // The Rust code's `OffsetDateTime::from_unix_timestamp(seconds)` implies
  // Unix epoch based time.
  const nowMs = Date.now();
  return BigInt(nowMs) * 1_000_000n;
}

/**
 * Formats a Unix timestamp (nanoseconds) into YYYYMMDD.
 * @param timeNs - Unix timestamp in nanoseconds.
 * @returns Date string in YYYYMMDD format.
 */
function format_date(timeNs: bigint): string {
  const date = new Date(Number(timeNs / 1_000_000n)); // Convert ns to ms
  const year = date.getFullYear();
  const month = (date.getMonth() + 1).toString().padStart(2, "0");
  const day = date.getDate().toString().padStart(2, "0");
  return `${year}${month}${day}`;
}

/**
 * Formats a Unix timestamp (nanoseconds) into YYYYMMDDTHHMMSSZ.
 * @param timeNs - Unix timestamp in nanoseconds.
 * @returns DateTime string in YYYYMMDDTHHMMSSZ format.
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
 * Formats a Unix timestamp (nanoseconds) into ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ).
 * @param timeNs - Unix timestamp in nanoseconds.
 * @returns ISO 8601 formatted string.
 */
function format_iso8601(timeNs: bigint): string {
  const date = new Date(Number(timeNs / 1_000_000n)); // Convert ns to ms
  return date.toISOString().replace(/\.\d{3}Z$/, "Z"); // Remove milliseconds, ensuring 'Z' suffix
}

/**
 * URL encode function that follows AWS rules.
 * @param s - The string to encode.
 * @returns The URL-encoded string.
 */
function url_encode(s: string): string {
  let encoded = "";
  for (let i = 0; i < s.length; i++) {
    const char = s[i];
    if (
      (char >= "A" && char <= "Z") ||
      (char >= "a" && char <= "z") ||
      (char >= "0" && char <= "9") ||
      char === "*" ||
      char === "-" ||
      char === "~" ||
      char === "."
    ) {
      encoded += char;
    } else {
      // Encode as uppercase hex
      encoded += `%${char.charCodeAt(0).toString(16).toUpperCase().padStart(2, "0")}`;
    }
  }
  return encoded;
}

/**
 * SHA256 hash function.
 * @param data - The data to hash.
 * @returns The SHA256 hash as a Buffer.
 */
function sha256_hash(data: Buffer): Buffer {
  return createHash("sha256").update(data).digest();
}

/**
 * HMAC-SHA256 function.
 * @param key - The key for HMAC.
 * @param data - The data to sign.
 * @returns The HMAC-SHA256 signature as a Buffer.
 */
function hmac_sha256(key: Buffer, data: Buffer): Buffer {
  return createHmac("sha256", key).update(data).digest();
}

/**
 * Derives the signing key for AWS Signature V4.
 * @param secret - AWS Secret Access Key.
 * @param date - Date in YYYYMMDD format.
 * @param region - AWS region.
 * @param service - AWS service (e.g., 's3').
 * @returns The derived signing key as a Buffer.
 */
function derive_signing_key(
  secret: string,
  date: string,
  region: string,
  service: string
): Buffer {
  const kDate = hmac_sha256(Buffer.from(`AWS4${secret}`), Buffer.from(date));
  const kRegion = hmac_sha256(kDate, Buffer.from(region));
  const kService = hmac_sha256(kRegion, Buffer.from(service));
  const kSigning = hmac_sha256(kService, Buffer.from("aws4_request"));
  return kSigning;
}

/**
 * Signs the policy for S3 uploads.
 * @param policy - The policy document as a base64 encoded string.
 * @param secret - AWS Secret Access Key.
 * @param date - Date in YYYYMMDD format.
 * @param region - AWS region.
 * @returns The signature as a hexadecimal string.
 */
function sign_policy(
  policy: string,
  secret: string,
  date: string,
  region: string
): string {
  const dateKey = hmac_sha256(Buffer.from(`AWS4${secret}`), Buffer.from(date));
  const regionKey = hmac_sha256(dateKey, Buffer.from(region));
  const serviceKey = hmac_sha256(regionKey, Buffer.from("s3"));
  const signingKey = hmac_sha256(serviceKey, Buffer.from("aws4_request"));

  return hmac_sha256(signingKey, Buffer.from(policy)).toString("hex");
}

/**
 * Generates a pre-signed URL for viewing an S3 object.
 * @param file_id - The ID of the file.
 * @param file_extension - The extension of the file (e.g., "pdf", "jpg").
 * @param auth - AWS bucket authentication details.
 * @param drive_id - The ID of the drive.
 * @param expires_in - Optional expiration time in seconds. Defaults to 24 hours.
 * @param download_filename - Optional filename for download. If provided, will set Content-Disposition.
 * @param disk_id - The ID of the disk.
 * @returns A pre-signed URL as a string.
 */
export function generate_s3_view_url(
  file_id: string,
  file_extension: string,
  auth: AwsBucketAuth,
  drive_id: DriveID, // Passed as argument
  expires_in: number | undefined,
  download_filename: string | undefined,
  disk_id: DiskID
): string {
  const currentTime = get_current_time_nanos(); // Unix timestamp in nanoseconds

  const date = format_date(currentTime); // YYYYMMDD
  const dateTime = format_datetime(currentTime); // YYYYMMDDTHHMMSSZ

  const credential = `${auth.access_key}/${date}/${auth.region}/s3/aws4_request`;
  const expiration = (expires_in ?? DEFAULT_EXPIRATION_SECONDS).toString();

  const host = `${auth.bucket}.s3.${auth.region}.amazonaws.com`;
  const s3Key = `${drive_id}/${disk_id}/${file_id}/${file_id}.${file_extension}`;

  // Content-Disposition logic (currently hardcoded to inline, but adaptable)
  const contentDisposition = "inline";
  // let contentDisposition: string | undefined;
  // if (download_filename) {
  //   const encodedFilename = url_encode(download_filename);
  //   contentDisposition = `attachment; filename="${encodedFilename}"`;
  // }

  const queryParams: [string, string][] = [
    ["X-Amz-Algorithm", "AWS4-HMAC-SHA256"],
    ["X-Amz-Credential", credential],
    ["X-Amz-Date", dateTime],
    ["X-Amz-Expires", expiration],
    ["X-Amz-SignedHeaders", "host"],
  ];

  if (contentDisposition) {
    queryParams.push(["response-content-disposition", contentDisposition]);
  }

  // Sort query parameters by key
  queryParams.sort((a, b) => a[0].localeCompare(b[0]));

  const canonicalQueryString = queryParams
    .map(([k, v]) => `${url_encode(k)}=${url_encode(v)}`)
    .join("&");

  const canonicalHeaders = `host:${host}\n`;

  const canonicalRequest = `GET\n/${s3Key}\n${canonicalQueryString}\n${canonicalHeaders}\nhost\nUNSIGNED-PAYLOAD`;

  const stringToSign = `AWS4-HMAC-SHA256\n${dateTime}\n${date}/${
    auth.region
  }/s3/aws4_request\n${sha256_hash(Buffer.from(canonicalRequest)).toString(
    "hex"
  )}`;

  const signingKey = derive_signing_key(
    auth.secret_key,
    date,
    auth.region,
    "s3"
  );
  const signature = hmac_sha256(signingKey, Buffer.from(stringToSign)).toString(
    "hex"
  );

  return `https://${host}/${s3Key}?${canonicalQueryString}&X-Amz-Signature=${signature}`;
}

/**
 * Generates a pre-signed URL and form fields for uploading to S3.
 * @param file_id - The ID of the file.
 * @param file_extension - The extension of the file.
 * @param auth - AWS bucket authentication details.
 * @param drive_id - The ID of the drive.
 * @param max_size - The maximum allowed size of the file in bytes.
 * @param expires_in - The expiration time for the upload URL in seconds.
 * @param disk_id - The ID of the disk.
 * @returns A Result containing DiskUploadResponse on success or an error string.
 */
export function generate_s3_upload_url(
  file_id: string,
  file_extension: string,
  auth: AwsBucketAuth,
  drive_id: DriveID, // Passed as argument
  max_size: number,
  expires_in: number,
  disk_id: DiskID
): Result<DiskUploadResponse, string> {
  const currentTime = get_current_time_nanos(); // Unix timestamp in nanoseconds
  const expirationTime = currentTime + BigInt(expires_in) * 1_000_000_000n; // Convert seconds to nanoseconds and add

  const date = format_date(currentTime);
  const dateTime = format_datetime(currentTime);
  const expiration = format_iso8601(expirationTime);

  const targetKey = `${drive_id}/${disk_id}/${file_id}/${file_id}.${file_extension}`;

  const policy = JSON.stringify({
    expiration: expiration,
    conditions: [
      { bucket: auth.bucket },
      { key: targetKey },
      { acl: "private" },
      ["content-length-range", 0, max_size],
      { "x-amz-algorithm": "AWS4-HMAC-SHA256" },
      {
        "x-amz-credential": `${auth.access_key}/${date}/${auth.region}/s3/aws4_request`,
      },
      { "x-amz-date": dateTime },
      { "Content-Disposition": "inline" },
    ],
  });

  const policyBase64 = Buffer.from(policy).toString("base64");
  const signature = sign_policy(
    policyBase64,
    auth.secret_key,
    date,
    auth.region
  );

  const fields: Record<string, string> = {
    key: targetKey,
    acl: "private",
    "x-amz-algorithm": "AWS4-HMAC-SHA256",
    "x-amz-credential": `${auth.access_key}/${date}/${auth.region}/s3/aws4_request`,
    "x-amz-date": dateTime,
    policy: policyBase64,
    "x-amz-signature": signature,
    "Content-Disposition": "inline",
  };

  return {
    ok: {
      url: auth.endpoint
        ? `${auth.endpoint}/${auth.bucket}`
        : `https://${auth.bucket}.s3.${auth.region}.amazonaws.com`,
      fields,
    },
  };
}

/**
 * Copies an object within S3 using the AWS S3 Copy Object API.
 * @param source_key - The key of the source object.
 * @param destination_key - The key of the destination object.
 * @param auth - AWS bucket authentication details.
 * @returns A Promise that resolves with Result.ok(null) on success or Result.err(string) on failure.
 */
export async function copy_s3_object(
  source_key: string,
  destination_key: string,
  auth: AwsBucketAuth
): Promise<Result<null, string>> {
  const host = `${auth.bucket}.s3.${auth.region}.amazonaws.com`;
  const currentTime = get_current_time_nanos();
  const date = format_date(currentTime);
  const dateTime = format_datetime(currentTime);

  const credential = `${auth.access_key}/${date}/${auth.region}/s3/aws4_request`;
  const copySource = `${auth.bucket}/${source_key}`;

  const headers: HeadersInit = {
    Host: host,
    "x-amz-date": dateTime,
    "x-amz-copy-source": copySource,
    "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
  };

  const canonicalUri = `/${destination_key}`;
  const canonicalHeaders =
    `host:${host}\n` +
    `x-amz-content-sha256:UNSIGNED-PAYLOAD\n` +
    `x-amz-copy-source:${copySource}\n` +
    `x-amz-date:${dateTime}\n`;
  const signedHeaders =
    "host;x-amz-content-sha256;x-amz-copy-source;x-amz-date";

  const canonicalRequest = `PUT\n${canonicalUri}\n\n${canonicalHeaders}\n${signedHeaders}\nUNSIGNED-PAYLOAD`;

  const stringToSign = `AWS4-HMAC-SHA256\n${dateTime}\n${date}/${
    auth.region
  }/s3/aws4_request\n${sha256_hash(Buffer.from(canonicalRequest)).toString(
    "hex"
  )}`;

  const signature = sign_policy(
    stringToSign,
    auth.secret_key,
    date,
    auth.region
  );

  const authorization = `AWS4-HMAC-SHA256 Credential=${credential},SignedHeaders=${signedHeaders},Signature=${signature}`;

  (headers as Record<string, string>)["Authorization"] = authorization; // Add Authorization header

  const requestInit: RequestInit = {
    method: "PUT",
    headers: headers,
    body: undefined, // No body for copy
  };

  try {
    const response: NodeFetchResponse = await fetch(
      `https://${host}${canonicalUri}`,
      requestInit
    );
    const status_u16 = response.status;

    if (status_u16 < 200 || status_u16 >= 300) {
      const errorBody = await response.text();
      return {
        err: `S3 copy failed with status ${status_u16}: ${errorBody}`,
      };
    } else {
      return { ok: null };
    }
  } catch (error: any) {
    return { err: `HTTP request failed: ${error.message}` };
  }
}

/**
 * Deletes an object from S3. Uses POST with `?delete` due to potential DELETE method limitations or preference.
 * @param file_key - The key of the file to delete.
 * @param auth - AWS bucket authentication details.
 * @returns A Promise that resolves with Result.ok(null) on success or Result.err(string) on failure.
 */
export async function delete_s3_object(
  file_key: string,
  auth: AwsBucketAuth
): Promise<Result<null, string>> {
  console.log(
    `Deleting S3 object: ${file_key} using endpoint: ${
      auth.endpoint || "standard"
    }`
  );

  const host = auth.endpoint
    ? auth.endpoint.replace(/^https?:\/\//, "").replace(/\/$/, "")
    : `${auth.bucket}.s3.${auth.region}.amazonaws.com`;

  console.log(`Host for signing: ${host}`);

  const currentTime = get_current_time_nanos();
  const date = format_date(currentTime);
  const dateTime = format_datetime(currentTime);

  const credential = `${auth.access_key}/${date}/${auth.region}/s3/aws4_request`;

  const url = auth.endpoint
    ? `${auth.endpoint.replace(/\/$/, "")}/${auth.bucket}/${file_key}?delete`
    : `https://${auth.bucket}.s3.${auth.region}.amazonaws.com/${file_key}?delete`;

  console.log(`Delete request URL: ${url}`);

  const deleteBody = `<?xml version="1.0" encoding="UTF-8"?>
<Delete>
  <Object>
    <Key>${file_key}</Key>
  </Object>
  <Quiet>true</Quiet>
</Delete>`;

  const contentMd5 = createHash("md5").update(deleteBody).digest("base64");
  console.log(`Content-MD5: ${contentMd5}`);

  const payloadHash = sha256_hash(Buffer.from(deleteBody)).toString("hex");
  console.log(`Payload hash: ${payloadHash}`);

  const canonicalUri = `/${file_key}`;
  const canonicalQueryString = "delete=";

  const headers: HeadersInit = {
    Host: host,
    "Content-Type": "application/xml",
    "Content-MD5": contentMd5,
    "Content-Length": deleteBody.length.toString(),
    "x-amz-date": dateTime,
    "x-amz-content-sha256": payloadHash,
  };

  const canonicalHeaders =
    `content-length:${deleteBody.length}\n` +
    `content-md5:${contentMd5}\n` +
    `content-type:application/xml\n` +
    `host:${host}\n` +
    `x-amz-content-sha256:${payloadHash}\n` +
    `x-amz-date:${dateTime}\n`;

  const signedHeaders =
    "content-length;content-md5;content-type;host;x-amz-content-sha256;x-amz-date";

  const canonicalRequest = `POST\n${canonicalUri}\n${canonicalQueryString}\n${canonicalHeaders}\n${signedHeaders}\n${payloadHash}`;

  console.log(`Canonical request: ${canonicalRequest}`);

  const stringToSign = `AWS4-HMAC-SHA256\n${dateTime}\n${date}/${
    auth.region
  }/s3/aws4_request\n${sha256_hash(Buffer.from(canonicalRequest)).toString(
    "hex"
  )}`;

  console.log(`String to sign: ${stringToSign}`);

  const signingKey = derive_signing_key(
    auth.secret_key,
    date,
    auth.region,
    "s3"
  );
  const signature = hmac_sha256(signingKey, Buffer.from(stringToSign)).toString(
    "hex"
  );

  const authorization = `AWS4-HMAC-SHA256 Credential=${credential},SignedHeaders=${signedHeaders},Signature=${signature}`;

  (headers as Record<string, string>)["Authorization"] = authorization; // Add Authorization header

  for (const headerName in headers) {
    console.log(`Header: ${headerName} = ${headers[headerName]}`);
  }

  const requestInit: RequestInit = {
    method: "POST",
    headers: headers,
    body: deleteBody,
  };

  console.log("Sending delete request...");

  try {
    const response: NodeFetchResponse = await fetch(url, requestInit);
    const status_u16 = response.status;

    console.log(`Delete response status: ${status_u16}`);
    const responseBody = await response.text();
    console.log(`Delete response body: ${responseBody}`);

    if (status_u16 >= 200 && status_u16 < 300) {
      console.log("Object deleted successfully");
      return { ok: null };
    } else {
      const errorMsg = `S3 delete failed with status ${status_u16}: ${responseBody}`;
      console.log(`Delete failed: ${errorMsg}`);
      return { err: errorMsg };
    }
  } catch (error: any) {
    const errorMsg = `HTTP request failed: ${error.message}`;
    console.log(`Delete request error: ${errorMsg}`);
    return { err: errorMsg };
  }
}

// Helper type for Result (Rust-like Enum for success/error)
interface Ok<T> {
  ok: T;
  err?: never;
}

interface Err<E> {
  ok?: never;
  err: E;
}

type Result<T, E> = Ok<T> | Err<E>;
