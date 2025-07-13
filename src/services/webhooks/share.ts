import {
  GenerateID,
  ShareTrackHash,
  ShareTrackID,
  UserID,
} from "@officexapp/types";

/**
 * Internal interface for the data stored within the ShareTrackHash.
 * This mirrors the Rust `ShareTrackHashData` struct.
 */
interface ShareTrackHashData {
  id: ShareTrackID; // The ShareTrackID itself, including its prefix
  from_user: UserID; // The UserID of the user who initiated the share
}

/**
 * Generates a unique ShareTrackID and a corresponding ShareTrackHash.
 * The hash is a base64-encoded JSON string containing the ShareTrackID and the UserID.
 * This function is designed to be a TypeScript equivalent of the Rust `generate_share_track_hash`.
 *
 * @param userId The UserID of the user for whom the share track is being generated.
 * @returns A tuple containing the generated ShareTrackID and ShareTrackHash.
 */
export function generateShareTrackHash(
  userId: UserID
): [ShareTrackID, ShareTrackHash] {
  // Generate a unique ShareTrackID using the existing project utility.
  // This internally uses uuidv4 and applies the correct prefix.
  const shareTrackId: ShareTrackID = GenerateID.ShareTrackID();

  // Create the data object to be encoded in the hash.
  const hashData: ShareTrackHashData = {
    id: shareTrackId,
    from_user: userId,
  };

  // Serialize the data object to a JSON string.
  const jsonData = JSON.stringify(hashData);

  // Base64 encode the JSON string.
  // Node.js Buffer is used for base64 encoding/decoding.
  const hash: ShareTrackHash = Buffer.from(jsonData).toString(
    "base64"
  ) as ShareTrackHash;

  return [shareTrackId, hash];
}

/**
 * Decodes a ShareTrackHash back into its original ShareTrackID and UserID components.
 * This function is designed to be a TypeScript equivalent of the Rust `decode_share_track_hash`.
 * It handles potential decoding and parsing errors by returning empty strings wrapped in the respective types.
 *
 * @param hash The ShareTrackHash string to decode.
 * @returns A tuple containing the decoded ShareTrackID and UserID.
 */
export function decodeShareTrackHash(
  hash: ShareTrackHash
): [ShareTrackID, UserID] {
  try {
    // Attempt to decode the base64 string.
    const decodedBytes = Buffer.from(hash, "base64").toString("utf8");

    // Parse the JSON string back into the ShareTrackHashData interface.
    const hashData: ShareTrackHashData = JSON.parse(decodedBytes);

    // Return the extracted ShareTrackID and UserID.
    // Since `GenerateID.ShareTrackID()` already includes the prefix,
    // the `hashData.id` should already be a complete ShareTrackID.
    return [hashData.id, hashData.from_user];
  } catch (error) {
    console.error("Error decoding share track hash:", error);
    // Return empty strings for ID and UserID on error, mirroring Rust's behavior.
    return ["" as ShareTrackID, "" as UserID];
  }
}
