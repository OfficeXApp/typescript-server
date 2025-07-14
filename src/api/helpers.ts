import { IDPrefixEnum } from "@officexapp/types";
import { validateIcpPrincipal } from "../services/validation";

export function isValidUserId(userId: string): boolean {
  if (!userId.startsWith("UserID_")) {
    return false;
  }
  const principal = userId.split("UserID_")[1];
  const isValidPrincipal = validateIcpPrincipal(principal);
  return isValidPrincipal.success;
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
