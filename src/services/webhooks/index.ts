// src/services/webhooks/index.ts

import { db, dbHelpers } from "../database";
import {
  Webhook,
  WebhookEventLabel,
  FileID,
  FolderID,
  DirectoryWebhookData,
  FileWebhookData,
  FolderWebhookData,
  WebhookEventPayload,
  WebhookResourceData,
  DriveID,
  LabelID,
  IRequestInboxOrg,
  IDPrefixEnum,
  StateDiffRecord,
  DriveStateDiffID,
  GroupInviteWebhookData, // Added for correct typing
  LabelWebhookData, // Added for correct typing
  UserID, // Added for correct typing
} from "@officexapp/types";
import { v4 as uuidv4 } from "uuid";
import fetch from "node-fetch";

// Constants for special webhook alt indexes, matching Rust's `WebhookAltIndexID` constants
const WEBHOOK_ALT_INDEX_ALL_FILES = "ALL_FILES"; // Corresponds to file_created_slug
const WEBHOOK_ALT_INDEX_ALL_FOLDERS = "ALL_FOLDERS"; // Corresponds to folder_created_slug
const WEBHOOK_ALT_INDEX_RESTORE_TRASH = "RESTORE_TRASH"; // Corresponds to restore_trash_slug
const WEBHOOK_ALT_INDEX_STATE_DIFFS = "STATE_DIFFS"; // Corresponds to state_diffs_slug
const WEBHOOK_ALT_INDEX_SUPERSWAP_USER = "SUPERSWAP_USER"; // Corresponds to superswap_user_slug
const WEBHOOK_ALT_INDEX_INBOX_NEW_MAIL = "INBOX_NEW_MAIL"; // Corresponds to inbox_new_notif_slug

/**
 * Gets active webhooks for a file based on the event type
 * @param orgId - The organization/drive ID
 * @param fileId - The file ID to check webhooks for
 * @param event - The webhook event type
 * @returns Array of active webhooks
 */
export async function getActiveFileWebhooks(
  orgId: DriveID,
  fileId: FileID,
  event: WebhookEventLabel
): Promise<Webhook[]> {
  let allWebhooks: Webhook[] = [];

  // Get webhooks for the current file or special "ALL_FILES" slug
  const directFileWebhooks = await db.queryDrive(
    orgId,
    `SELECT * FROM webhooks WHERE (alt_index = ? OR alt_index = ?) AND event = ? AND active = 1`,
    [fileId, WEBHOOK_ALT_INDEX_ALL_FILES, event]
  );
  allWebhooks.push(...(directFileWebhooks as Webhook[]));

  // Check if we should look for parent folder webhooks
  const shouldCheckParents = [
    WebhookEventLabel.SUBFILE_VIEWED,
    WebhookEventLabel.SUBFILE_CREATED,
    WebhookEventLabel.SUBFILE_UPDATED,
    WebhookEventLabel.SUBFILE_DELETED,
    WebhookEventLabel.SUBFILE_SHARED,
  ].includes(event);

  if (!shouldCheckParents) {
    return allWebhooks;
  }

  // Rust's `file_uuid_to_metadata` is equivalent to querying the `files` table.
  const fileResult = await db.queryDrive(
    orgId,
    `SELECT parent_folder_id FROM files WHERE id = ?`,
    [fileId]
  );

  if (!fileResult || fileResult.length === 0) {
    return allWebhooks;
  }

  let currentFolderId = fileResult[0].parent_folder_id as FolderID | null;
  let currentDepth = 0;
  const parentRecursionDepth = 20; // Matches Rust constant

  // Traverse up the parent folders
  while (currentFolderId && currentDepth < parentRecursionDepth) {
    // Rust's `folder_uuid_to_metadata.get(&folder_id)` is querying the `folders` table.
    const folderResult = await db.queryDrive(
      orgId,
      `SELECT parent_folder_id, has_sovereign_permissions FROM folders WHERE id = ?`,
      [currentFolderId]
    );

    if (!folderResult || folderResult.length === 0) {
      break;
    }

    const folder = folderResult[0];

    // Stop if we hit a sovereign permissions folder
    if (folder.has_sovereign_permissions) {
      break;
    }

    // Get webhooks for this parent folder or special "ALL_FOLDERS" slug
    const parentFolderWebhooks = await db.queryDrive(
      orgId,
      `SELECT * FROM webhooks WHERE (alt_index = ? OR alt_index = ?) AND event = ? AND active = 1`,
      [currentFolderId, WEBHOOK_ALT_INDEX_ALL_FOLDERS, event]
    );

    allWebhooks.push(...(parentFolderWebhooks as Webhook[]));

    // Move to next parent
    currentFolderId = folder.parent_folder_id as FolderID | null;
    currentDepth++;
  }

  return allWebhooks;
}

/**
 * Gets active webhooks for a folder based on the event type
 * @param orgId - The organization/drive ID
 * @param folderId - The folder ID to check webhooks for
 * @param event - The webhook event type
 * @returns Array of active webhooks
 */
export async function getActiveFolderWebhooks(
  orgId: DriveID,
  folderId: FolderID,
  event: WebhookEventLabel
): Promise<Webhook[]> {
  let allWebhooks: Webhook[] = [];

  // Get webhooks for the current folder or special "ALL_FOLDERS" slug
  const directFolderWebhooks = await db.queryDrive(
    orgId,
    `SELECT * FROM webhooks WHERE (alt_index = ? OR alt_index = ?) AND event = ? AND active = 1`,
    [folderId, WEBHOOK_ALT_INDEX_ALL_FOLDERS, event]
  );
  allWebhooks.push(...(directFolderWebhooks as Webhook[]));

  // Check if we should look for parent folder webhooks
  const shouldCheckParents = [
    WebhookEventLabel.SUBFOLDER_VIEWED,
    WebhookEventLabel.SUBFOLDER_CREATED,
    WebhookEventLabel.SUBFOLDER_UPDATED,
    WebhookEventLabel.SUBFOLDER_DELETED,
    WebhookEventLabel.SUBFOLDER_SHARED,
  ].includes(event);

  if (!shouldCheckParents) {
    return allWebhooks;
  }

  // Rust's `folder_uuid_to_metadata` is equivalent to querying the `folders` table.
  const folderResult = await db.queryDrive(
    orgId,
    `SELECT parent_folder_id FROM folders WHERE id = ?`,
    [folderId]
  );

  if (!folderResult || folderResult.length === 0) {
    return allWebhooks;
  }

  let currentFolderId = folderResult[0].parent_folder_id as FolderID | null;
  let currentDepth = 0;
  const parentRecursionDepth = 20; // Matches Rust constant

  // Traverse up the parent folders
  while (currentFolderId && currentDepth < parentRecursionDepth) {
    const parentFolderResult = await db.queryDrive(
      orgId,
      `SELECT parent_folder_id, has_sovereign_permissions FROM folders WHERE id = ?`,
      [currentFolderId]
    );

    if (!parentFolderResult || parentFolderResult.length === 0) {
      break;
    }

    const parentFolder = parentFolderResult[0];

    // Stop if we hit a sovereign permissions folder
    if (parentFolder.has_sovereign_permissions) {
      break;
    }

    // Get webhooks for this parent folder or special "ALL_FOLDERS" slug
    const parentWebhooks = await db.queryDrive(
      orgId,
      `SELECT * FROM webhooks WHERE (alt_index = ? OR alt_index = ?) AND event = ? AND active = 1`,
      [currentFolderId, WEBHOOK_ALT_INDEX_ALL_FOLDERS, event]
    );

    allWebhooks.push(...(parentWebhooks as Webhook[]));

    // Move to next parent
    currentFolderId = parentFolder.parent_folder_id as FolderID | null;
    currentDepth++;
  }

  return allWebhooks;
}

/**
 * Fires directory webhooks with the given event data
 * @param orgId - The organization/drive ID (used for db access, not directly in payload)
 * @param event - The webhook event type
 * @param webhooks - Array of webhooks to fire
 * @param beforeSnap - Optional snapshot of state before the event
 * @param afterSnap - Optional snapshot of state after the event
 * @param notes - Optional notes about the event
 */
export async function fireDirectoryWebhook(
  orgId: DriveID, // orgId is needed for db.queryDrive in other functions, but not for firing
  event: WebhookEventLabel,
  webhooks: Webhook[],
  beforeSnap?: DirectoryWebhookData,
  afterSnap?: DirectoryWebhookData,
  notes?: string
): Promise<void> {
  const timestampMs = Date.now();

  // Process webhooks in parallel (matching Rust's spawn behavior)
  await Promise.all(
    webhooks.map(async (webhook) => {
      let beforeResource: WebhookResourceData | undefined;
      let afterResource: WebhookResourceData | undefined;

      // Rust's `match` for `DirectoryWebhookData`
      if (beforeSnap) {
        if ("File" in beforeSnap) {
          beforeResource = { type: "file", ...beforeSnap.File };
        } else if ("Folder" in beforeSnap) {
          beforeResource = { type: "folder", ...beforeSnap.Folder };
        } else if ("Subfile" in beforeSnap) {
          beforeResource = { type: "subfile", ...beforeSnap.Subfile };
        } else if ("Subfolder" in beforeSnap) {
          beforeResource = { type: "subfolder", ...beforeSnap.Subfolder };
        } else if ("ShareTracking" in beforeSnap) {
          beforeResource = {
            type: "share_tracking",
            ...beforeSnap.ShareTracking,
          };
        }
      }

      if (afterSnap) {
        if ("File" in afterSnap) {
          afterResource = { type: "file", ...afterSnap.File };
        } else if ("Folder" in afterSnap) {
          afterResource = { type: "folder", ...afterSnap.Folder };
        } else if ("Subfile" in afterSnap) {
          afterResource = { type: "subfile", ...afterSnap.Subfile };
        } else if ("Subfolder" in afterSnap) {
          afterResource = { type: "subfolder", ...afterSnap.Subfolder };
        } else if ("ShareTracking" in afterSnap) {
          afterResource = {
            type: "share_tracking",
            ...afterSnap.ShareTracking,
          };
        }
      }

      const payload: WebhookEventPayload = {
        event: event.toString(),
        timestamp_ms: timestampMs,
        nonce: timestampMs,
        notes,
        webhook_id: webhook.id,
        webhook_alt_index: webhook.alt_index,
        payload: {
          before: beforeResource,
          after: afterResource,
        },
      };

      try {
        const response = await fetch(webhook.url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            signature: webhook.signature,
          },
          body: JSON.stringify(payload),
        });

        if (!response.ok) {
          console.error(
            `Webhook ${webhook.id} to ${webhook.url} failed with status ${response.status}: ${response.statusText}`
          );
        } else {
          console.log(
            `Webhook ${webhook.id} to ${webhook.url} fired successfully.`
          );
        }
      } catch (error) {
        console.error(
          `Failed to fire webhook ${webhook.id} to ${webhook.url}:`,
          error
        );
      }
    })
  );
}

/**
 * Retrieves active webhooks for a specific group invite event.
 * @param groupId The ID of the group associated with the invite.
 * @param event The webhook event type to filter by.
 * @param orgId The organization/drive ID that the group belongs to.
 * @returns Array of active webhooks matching the criteria.
 */
export async function getActiveGroupInviteWebhooks(
  orgId: DriveID,
  groupId: string, // In Rust, this is GroupID
  event: WebhookEventLabel
): Promise<Webhook[]> {
  try {
    // Rust: `WEBHOOKS_BY_ALT_INDEX_HASHTABLE.get(&WebhookAltIndexID(group_id.0.clone()))`
    // This implies that the alt_index for group invites is the GroupID itself.
    const webhooks = await db.queryDrive(
      orgId,
      `SELECT * FROM webhooks WHERE alt_index = ? AND event = ? AND active = 1`,
      [groupId, event]
    );

    return webhooks as Webhook[];
  } catch (error) {
    console.error("Error in getActiveGroupInviteWebhooks:", error);
    return [];
  }
}

/**
 * Fires group invite webhooks to their configured URLs
 * @param event The webhook event type
 * @param webhooks Array of webhooks to fire
 * @param beforeSnap Optional snapshot data before the event
 * @param afterSnap Optional snapshot data after the event
 * @param notes Optional notes about the event
 */
export async function fireGroupInviteWebhook(
  event: WebhookEventLabel,
  webhooks: Webhook[],
  beforeSnap?: GroupInviteWebhookData,
  afterSnap?: GroupInviteWebhookData,
  notes?: string
): Promise<void> {
  const timestampMs = Date.now();

  // Fire each webhook in parallel (matching Rust's spawn behavior)
  await Promise.all(
    webhooks.map(async (webhook) => {
      try {
        const payload: WebhookEventPayload = {
          event: event.toString(),
          timestamp_ms: timestampMs,
          nonce: timestampMs,
          notes,
          webhook_id: webhook.id,
          webhook_alt_index: webhook.alt_index,
          payload: {
            before: beforeSnap
              ? { type: "group_invite", ...beforeSnap }
              : undefined,
            after: afterSnap
              ? { type: "group_invite", ...afterSnap }
              : undefined,
          },
        };

        const response = await fetch(webhook.url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            signature: webhook.signature,
          },
          body: JSON.stringify(payload),
        });

        if (!response.ok) {
          console.error(
            `Webhook ${webhook.id} to ${webhook.url} failed with status ${response.status}: ${response.statusText}`
          );
        } else {
          console.log(
            `Webhook ${webhook.id} to ${webhook.url} fired successfully.`
          );
        }
      } catch (error) {
        console.error(
          `Error firing webhook ${webhook.id} to ${webhook.url}:`,
          error
        );
      }
    })
  );
}

/**
 * Retrieves all active webhooks for a specific label and event
 * @param labelId The ID of the label to get webhooks for
 * @param event The webhook event type to filter by
 * @param orgId The organization/drive ID that the label belongs to.
 * @returns Array of active webhooks matching the criteria
 */
export async function getActiveLabelWebhooks(
  orgId: DriveID, // Added orgId as it's needed for db.queryDrive
  labelId: LabelID,
  event: WebhookEventLabel
): Promise<Webhook[]> {
  try {
    // In Rust, `WEBHOOKS_BY_ALT_INDEX_HASHTABLE.get(&WebhookAltIndexID(label_id.0.clone()))`
    // This implies that the alt_index for label webhooks is the LabelID itself.
    const webhooks = await db.queryDrive(
      orgId,
      `SELECT * FROM webhooks WHERE alt_index = ? AND event = ? AND active = 1`,
      [labelId, event]
    );

    return webhooks as Webhook[];
  } catch (error) {
    console.error("Error in getActiveLabelWebhooks:", error);
    return [];
  }
}

/**
 * Fires webhooks for label-related events
 * @param event The webhook event type
 * @param webhooks Array of webhooks to trigger
 * @param beforeSnap Optional snapshot of label state before the event
 * @param afterSnap Optional snapshot of label state after the event
 * @param notes Optional notes about the event
 */
export async function fireLabelWebhook(
  event: WebhookEventLabel,
  webhooks: Webhook[],
  beforeSnap?: LabelWebhookData,
  afterSnap?: LabelWebhookData,
  notes?: string
): Promise<void> {
  const timestampMs = Date.now();

  // Process webhooks in parallel (matching Rust's spawn behavior)
  await Promise.all(
    webhooks.map(async (webhook) => {
      try {
        const payload: WebhookEventPayload = {
          event: event.toString(),
          timestamp_ms: timestampMs,
          nonce: timestampMs,
          notes: notes || undefined, // Use undefined for null if notes is not provided
          webhook_id: webhook.id,
          webhook_alt_index: webhook.alt_index,
          payload: {
            before: beforeSnap ? { type: "label", ...beforeSnap } : undefined,
            after: afterSnap ? { type: "label", ...afterSnap } : undefined,
          },
        };

        const response = await fetch(webhook.url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            signature: webhook.signature,
          },
          body: JSON.stringify(payload),
        });

        if (!response.ok) {
          console.error(
            `Webhook ${webhook.id} to ${webhook.url} failed with status ${response.status}: ${response.statusText}`
          );
        } else {
          console.log(
            `Webhook ${webhook.id} to ${webhook.url} fired successfully.`
          );
        }
      } catch (error) {
        console.error(
          `Error firing webhook ${webhook.id} to ${webhook.url}:`,
          error
        );
      }
    })
  );
}

/**
 * Get webhooks for superswap user events
 * @param event The webhook event type (should be WebhookEventLabel.ORG_SUPERSWAP_USER)
 * @param orgId The organization/drive ID
 * @returns Array of matching webhooks
 */
export async function getSuperswapUserWebhooks(
  orgId: DriveID,
  event: WebhookEventLabel
): Promise<Webhook[]> {
  // Rust: `WEBHOOKS_BY_ALT_INDEX_HASHTABLE.get(&WebhookAltIndexID::superswap_user_slug())`
  // This implies the alt_index for superswap user webhooks is a fixed slug.
  try {
    const webhooks = await db.queryDrive(
      orgId,
      `SELECT * FROM webhooks WHERE alt_index = ? AND event = ? AND active = 1`,
      [WEBHOOK_ALT_INDEX_SUPERSWAP_USER, event]
    );
    return webhooks as Webhook[];
  } catch (error) {
    console.error("Error in getSuperswapUserWebhooks:", error);
    return [];
  }
}

/**
 * Fire superswap user webhook
 * @param event The webhook event type
 * @param webhooks Array of webhooks to trigger
 * @param beforeSnap User ID before the change
 * @param afterSnap User ID after the change
 * @param notes Optional notes about the event
 */
export async function fireSuperswapUserWebhook(
  event: WebhookEventLabel,
  webhooks: Webhook[],
  beforeSnap?: UserID,
  afterSnap?: UserID,
  notes?: string
): Promise<void> {
  const timestampMs = Date.now();

  // Fire each webhook in parallel (matching Rust's spawn behavior)
  await Promise.all(
    webhooks.map(async (webhook) => {
      try {
        const payload: WebhookEventPayload = {
          event: event.toString(),
          timestamp_ms: timestampMs,
          nonce: timestampMs,
          notes,
          webhook_id: webhook.id,
          webhook_alt_index: webhook.alt_index,
          payload: {
            before: beforeSnap
              ? { type: "superswap_userid", content: beforeSnap }
              : undefined, // Corrected to use 'content' based on your types
            after: afterSnap
              ? { type: "superswap_userid", content: afterSnap }
              : undefined, // Corrected to use 'content' based on your types
          },
        };

        const response = await fetch(webhook.url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            signature: webhook.signature || "",
          },
          body: JSON.stringify(payload),
        });

        if (!response.ok) {
          console.error(
            `Webhook ${webhook.id} to ${webhook.url} failed with status ${response.status}: ${response.statusText}`
          );
        } else {
          console.log(
            `Webhook ${webhook.id} to ${webhook.url} fired successfully.`
          );
        }
      } catch (error) {
        console.error(
          `Error firing webhook ${webhook.id} to ${webhook.url}:`,
          error
        );
      }
    })
  );
}

/**
 * Get webhooks for organization inbox events
 * @param orgId The organization/drive ID
 * @param topic Optional topic filter
 * @returns Array of matching webhooks
 */
export async function getOrgInboxWebhooks(
  orgId: DriveID,
  topic?: string
): Promise<Webhook[]> {
  try {
    // Rust: `WEBHOOKS_BY_ALT_INDEX_HASHTABLE.get(&WebhookAltIndexID::superswap_user_slug())` -> This looks like a bug in Rust.
    // It should be `WebhookAltIndexID::inbox_new_notif_slug()`.
    // Assuming the intent is to filter by `INBOX_NEW_MAIL_SLUG` as `alt_index`.
    const webhooks = await db.queryDrive(
      orgId,
      `SELECT * FROM webhooks WHERE alt_index = ? AND event = ? AND active = 1`,
      [WEBHOOK_ALT_INDEX_INBOX_NEW_MAIL, WebhookEventLabel.ORG_INBOX_NEW_MAIL]
    );

    if (!webhooks || webhooks.length === 0) {
      return [];
    }

    // Filter by topic if provided
    if (topic) {
      return (webhooks as Webhook[]).filter((webhook: Webhook) => {
        // Rust's logic:
        // (None, filters) if !filters.is_empty() => false
        // (Some(_), filters) if filters.is_empty() => false
        // (Some(request_topic), filters) if !filters.is_empty() => match filter_json.get("topic") == request_topic
        // _ => true (catch-all for no topic and no filter)

        // No topic in request but webhook has filter - no match
        if (!topic && webhook.filters && webhook.filters.length > 0) {
          return false;
        }
        // Topic in request but no filter in webhook - no match
        if (topic && (!webhook.filters || webhook.filters.length === 0)) {
          return false;
        }
        // Both topic and filter exist - try to match
        if (topic && webhook.filters && webhook.filters.length > 0) {
          try {
            const filters = JSON.parse(webhook.filters);
            return filters.topic === topic;
          } catch (e) {
            console.error("Error parsing webhook filters:", e);
            return false;
          }
        }
        // No topic and no filter - catch-all case, matches (Rust's `_ => true`)
        return true;
      });
    }

    // Return all inbox webhooks if no topic filter and they are active and correct event
    return webhooks as Webhook[];
  } catch (error) {
    console.error("Error in getOrgInboxWebhooks:", error);
    return [];
  }
}

/**
 * Fire organization inbox new notification webhook
 * @param event The webhook event type
 * @param webhooks Array of webhooks to trigger
 * @param beforeSnap Request body before the change
 * @param afterSnap Request body after the change
 * @param notes Optional notes about the event
 */
export async function fireOrgInboxNewNotifWebhook(
  event: WebhookEventLabel,
  webhooks: Webhook[],
  beforeSnap?: IRequestInboxOrg,
  afterSnap?: IRequestInboxOrg,
  notes?: string
): Promise<void> {
  const timestampMs = Date.now();

  // Rust processes these sequentially due to `for webhook in webhooks { ... http_request(...).await }`
  // We will maintain sequential processing for this specific webhook due to the Rust source,
  // although typically `Promise.all` would be used for concurrent firing.
  for (const webhook of webhooks) {
    try {
      const payload: WebhookEventPayload = {
        event: event.toString(),
        timestamp_ms: timestampMs,
        nonce: timestampMs,
        notes,
        webhook_id: webhook.id,
        webhook_alt_index: webhook.alt_index,
        payload: {
          before: beforeSnap
            ? { type: "org_inbox_new_notif", ...beforeSnap }
            : undefined, // Corrected to spread the IRequestInboxOrg
          after: afterSnap
            ? { type: "org_inbox_new_notif", ...afterSnap }
            : undefined, // Corrected to spread the IRequestInboxOrg
        },
      };

      const response = await fetch(webhook.url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          signature: webhook.signature || "",
        },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        console.error(
          `Webhook ${webhook.id} to ${webhook.url} failed with status ${response.status}: ${response.statusText}`
        );
      } else {
        console.log(
          `Webhook ${webhook.id} to ${webhook.url} fired successfully.`
        );
      }
    } catch (error) {
      console.error(
        `Error firing webhook ${webhook.id} to ${webhook.url}:`,
        error
      );
    }
  }
}

/**
 * Get all active webhooks for state diff events
 * @param driveId - The drive ID to query within
 * @returns Array of active webhooks subscribed to state diff events
 */
export async function getActiveStateDiffWebhooks(
  driveId: DriveID
): Promise<Webhook[]> {
  try {
    // Rust: `WEBHOOKS_BY_ALT_INDEX_HASHTABLE.get(&WebhookAltIndexID(WebhookAltIndexID::state_diffs_slug().to_string()))`
    // This confirms the alt_index is the `STATE_DIFFS` slug.
    const webhooks = await db.queryDrive(
      driveId,
      `SELECT * FROM webhooks WHERE alt_index = ? AND event = ? AND active = 1`,
      [WEBHOOK_ALT_INDEX_STATE_DIFFS, WebhookEventLabel.DRIVE_STATE_DIFFS]
    );

    return webhooks as Webhook[];
  } catch (error) {
    console.error("Error fetching active state diff webhooks:", error);
    return [];
  }
}

/**
 * Fire state diff webhooks to all active subscribers
 * @param forwardDiff - The forward state diff string
 * @param backwardDiff - The backward state diff string
 * @param forwardChecksum - Checksum for forward diff
 * @param backwardChecksum - Checksum for backward diff
 * @param notes - Optional notes about the state diff
 * @param driveId - The drive ID these diffs belong to
 * @param endpointUrl - The endpoint URL of the drive
 */
export async function fireStateDiffWebhooks(
  forwardDiff: string,
  backwardDiff: string,
  forwardChecksum: string,
  backwardChecksum: string,
  notes: string | null,
  driveId: DriveID,
  endpointUrl: string
): Promise<void> {
  try {
    // Get current timestamp (Rust uses ic_cdk::api::time() / 1_000_000 for ms, and timestamp_ns directly for nonce)
    const timestampNs = BigInt(Date.now()) * 1_000_000n; // Convert current JS ms to ns
    const timestampMs = Number(timestampNs / 1_000_000n); // Convert to ms for the payload field

    // Generate a unique ID for this state diff
    const driveStateDiffId =
      `${IDPrefixEnum.DriveStateDiffID}${uuidv4()}` as DriveStateDiffID;

    // Get all active webhooks for state diffs
    const webhooks = await getActiveStateDiffWebhooks(driveId);

    // Prepare the state diff record
    const stateDiffRecord: StateDiffRecord = {
      id: driveStateDiffId,
      timestamp_ns: timestampNs,
      implementation: "JAVASCRIPT_RUNTIME", // As per your TS type definition
      diff_forward: forwardDiff,
      diff_backward: backwardDiff,
      notes: notes || undefined, // Use undefined if null
      drive_id: driveId,
      host_url: endpointUrl,
      checksum_forward: forwardChecksum,
      checksum_backward: backwardChecksum,
    };

    // Fire each webhook in parallel (matching Rust's spawn behavior)
    await Promise.all(
      webhooks.map(async (webhook) => {
        try {
          const payload: WebhookEventPayload = {
            event: WebhookEventLabel.DRIVE_STATE_DIFFS,
            timestamp_ms: timestampMs,
            nonce: timestampMs, // Rust uses timestamp_ns for nonce, but your TS nonce is number. Keeping it number for now.
            notes: notes || undefined,
            webhook_id: webhook.id,
            webhook_alt_index: webhook.alt_index,
            payload: {
              before: undefined, // Rust explicitly sets to None
              after: {
                type: "state_diffs",
                data: stateDiffRecord,
              },
            },
          };

          const response = await fetch(webhook.url, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              signature: webhook.signature,
            },
            body: JSON.stringify(payload),
          });

          if (!response.ok) {
            console.error(
              `Webhook ${webhook.id} to ${webhook.url} failed with status ${response.status}: ${response.statusText}`
            );
          } else {
            console.log(
              `Webhook ${webhook.id} to ${webhook.url} fired successfully.`
            );
          }
        } catch (error) {
          console.error(
            `Error firing webhook ${webhook.id} to ${webhook.url}:`,
            error
          );
        }
      })
    );
  } catch (error) {
    console.error("Error in fireStateDiffWebhooks:", error);
  }
}
