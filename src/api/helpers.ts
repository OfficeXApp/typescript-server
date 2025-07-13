import { IDPrefixEnum } from "@officexapp/types";
import { Principal } from "@dfinity/principal";

export function isValidUserId(userId: string): boolean {
  // Regular expression for a UUID v4:
  // xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
  // where x is any hexadecimal digit and y is one of 8, 9, A, or B.
  const uuidV4Regex =
    /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

  // Combine the prefix and the UUID v4 regex
  // The '^' asserts position at the start of the string.
  // The '$' asserts position at the end of the string.
  const userIdPattern = new RegExp(`^UserID_${uuidV4Regex.source}$`);

  // Test the input string against the combined pattern
  return userIdPattern.test(userId);
}

export function isValidID(prefix: IDPrefixEnum, id: string): boolean {
  const uuidV4Regex =
    /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  const idPattern = new RegExp(`^${prefix}${uuidV4Regex.source}$`);
  return idPattern.test(id);
}

export interface ValidationError {
  field: string;
  message: string;
}
