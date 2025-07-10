// src/services/validation.ts

// Define a ValidationError interface similar to the Rust one
export interface ValidationError {
  field: string;
  message: string;
}

/**
 * Validates an email address format.
 * @param email The email string to validate.
 * @returns True if the email is valid, false otherwise.
 */
export function validateEmail(email: string): boolean {
  // Basic regex for email validation. More robust validation might require a library or external service.
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(email)) {
    // console.error(`Validation Error: Email '${email}' is not a valid format.`);
    return false;
  }
  if (email.length > 256) {
    // Common max length for emails
    // console.error(`Validation Error: Email '${email}' exceeds 256 characters.`);
    return false;
  }
  return true;
}

/**
 * Validates an EVM public address format.
 * @param address The EVM public address string to validate.
 * @returns True if the address is valid, false otherwise.
 */
export function validateEvmAddress(address: string): boolean {
  // EVM addresses are typically 42 characters long (0x + 40 hex characters)
  const evmAddressRegex = /^0x[a-fA-F0-9]{40}$/;
  if (!evmAddressRegex.test(address)) {
    // console.error(`Validation Error: EVM address '${address}' is not a valid format.`);
    return false;
  }
  return true;
}

/**
 * Validates an external ID string.
 * @param externalId The external ID string to validate.
 * @returns True if the external ID is valid, false otherwise.
 */
export function validateExternalId(externalId: string): boolean {
  // Assuming external IDs have a max length and can contain alphanumeric, hyphens, and underscores.
  if (externalId.length === 0 || externalId.length > 256) {
    // console.error(`Validation Error: External ID '${externalId}' must be between 1 and 256 characters.`);
    return false;
  }
  // Optional: Add regex for allowed characters if needed, e.g., /^[a-zA-Z0-9_-]+$/
  return true;
}

/**
 * Validates an external payload string.
 * @param externalPayload The external payload string to validate.
 * @returns True if the external payload is valid, false otherwise.
 */
export function validateExternalPayload(externalPayload: string): boolean {
  // Assuming external payloads can be longer, up to 8192 characters as per Rust code's notes.
  if (externalPayload.length > 8192) {
    // console.error(`Validation Error: External payload exceeds 8192 characters.`);
    return false;
  }
  return true;
}

/**
 * Validates a general ID string (e.g., name, title).
 * @param idString The string to validate.
 * @param fieldName The name of the field being validated (for error messages).
 * @returns True if the string is valid, false otherwise.
 */
export function validateIdString(idString: string): boolean {
  // Assuming a max length of 256 characters for general ID strings like names.
  if (idString.length === 0 || idString.length > 256) {
    // console.error(`Validation Error: '${fieldName}' must be between 1 and 256 characters.`);
    return false;
  }
  return true;
}

/**
 * Validates a URL string.
 * @param url The URL string to validate.
 * @returns True if the URL is valid, false otherwise.
 */
export function validateUrl(url: string): boolean {
  try {
    new URL(url); // Use URL constructor for basic URL parsing and validation
    if (url.length > 2048) {
      // Common max length for URLs
      // console.error(`Validation Error: URL '${url}' exceeds 2048 characters.`);
      return false;
    }
    return true;
  } catch (e) {
    // console.error(`Validation Error: URL '${url}' is not a valid format.`, e);
    return false;
  }
}

/**
 * Validates a UserID format.
 * @param userId The UserID string to validate.
 * @returns True if the UserID is valid, false otherwise.
 */
export function validateUserId(userId: string): boolean {
  // UserID typically starts with "UserID_" followed by the ICP principal or a UUID.
  // For simplicity, we'll check the prefix and then assume the rest is valid.
  // A more robust check might involve validating the ICP principal part.
  const userIdRegex = /^UserID_[a-zA-Z0-9_-]+$/; // Adjust regex based on actual UserID structure
  if (!userIdRegex.test(userId)) {
    // console.error(`Validation Error: UserID '${userId}' is not a valid format.`);
    return false;
  }
  return true;
}
