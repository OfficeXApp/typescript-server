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

export function getContentTypeFromExtension(
  fileExtension: string
): string | undefined {
  if (!fileExtension) return undefined;

  const normalizedExtension = fileExtension.toLowerCase();

  switch (normalizedExtension) {
    case "pdf":
      return "application/pdf";
    case "jpg":
    case "jpeg":
      return "image/jpeg";
    case "png":
      return "image/png";
    case "gif":
      return "image/gif";
    case "svg":
      return "image/svg+xml";
    case "mp4":
      return "video/mp4";
    case "webm":
      return "video/webm";
    case "mp3":
      return "audio/mpeg";
    case "wav":
      return "audio/wav";
    case "xlsx":
    case "xls":
      // Note: XLSX has a specific MIME type, but for simplicity, we use a common one.
      return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
    case "csv":
      return "text/csv";
    case "txt":
      return "text/plain";
    case "json":
      return "application/json";
    case "zip":
      return "application/zip";
    default:
      // For a file that is not a common type, we return undefined
      return undefined;
  }
}
