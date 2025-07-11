import { FastifyRequest } from "fastify";
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
} from "@officexapp/types";
import { v4 as uuidv4 } from "uuid";
import fetch from "node-fetch";

/**
 * Gets active webhooks for a file based on the event type
 * @param orgId - The organization/drive ID
 * @param fileId - The file ID to check webhooks for
 * @param event - The webhook event type
 * @returns Array of active webhooks
 */
export async function getActiveFileWebhooks(
  orgId: string,
  fileId: FileID,
  event: WebhookEventLabel
): Promise<Webhook[]> {
  let allWebhooks: Webhook[] = [];

  // Get webhooks for the current file
  const fileWebhooks = await db.queryDrive(
    orgId,
    `SELECT * FROM webhooks 
     WHERE alt_index = ? AND event = ? AND is_active = 1`,
    [fileId, event]
  );

  allWebhooks.push(...(fileWebhooks as Webhook[]));

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

  // Get parent folder recursion depth
  const parentRecursionDepth = 20;
  if (parentRecursionDepth <= 0) {
    return allWebhooks;
  }

  // Start with the file's parent folder
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

  // Traverse up the parent folders
  while (currentFolderId && currentDepth < parentRecursionDepth) {
    const folderResult = await db.queryDrive(
      orgId,
      `SELECT parent_folder_id, has_sovereign_permissions 
       FROM folders WHERE id = ?`,
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

    // Get webhooks for this parent folder
    const folderWebhooks = await db.queryDrive(
      orgId,
      `SELECT * FROM webhooks 
       WHERE alt_index = ? AND event = ? AND is_active = 1`,
      [currentFolderId, event]
    );

    allWebhooks.push(...(folderWebhooks as Webhook[]));

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
  orgId: string,
  folderId: FolderID,
  event: WebhookEventLabel
): Promise<Webhook[]> {
  let allWebhooks: Webhook[] = [];

  // Get webhooks for the current folder
  const folderWebhooks = await db.queryDrive(
    orgId,
    `SELECT * FROM webhooks 
     WHERE alt_index = ? AND event = ? AND is_active = 1`,
    [folderId, event]
  );

  allWebhooks.push(...(folderWebhooks as Webhook[]));

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

  // Get parent folder recursion depth
  const parentRecursionDepth = 20;
  if (parentRecursionDepth <= 0) {
    return allWebhooks;
  }

  // Start with the current folder's parent
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

  // Traverse up the parent folders
  while (currentFolderId && currentDepth < parentRecursionDepth) {
    const parentFolderResult = await db.queryDrive(
      orgId,
      `SELECT parent_folder_id, has_sovereign_permissions 
       FROM folders WHERE id = ?`,
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

    // Get webhooks for this parent folder
    const parentWebhooks = await db.queryDrive(
      orgId,
      `SELECT * FROM webhooks 
       WHERE alt_index = ? AND event = ? AND is_active = 1`,
      [currentFolderId, event]
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
 * @param orgId - The organization/drive ID
 * @param event - The webhook event type
 * @param webhooks - Array of webhooks to fire
 * @param beforeSnap - Optional snapshot of state before the event
 * @param afterSnap - Optional snapshot of state after the event
 * @param notes - Optional notes about the event
 */
export async function fireDirectoryWebhook(
  orgId: string,
  event: WebhookEventLabel,
  webhooks: Webhook[],
  beforeSnap?: DirectoryWebhookData,
  afterSnap?: DirectoryWebhookData,
  notes?: string
): Promise<void> {
  const timestampMs = Date.now();

  // Process webhooks in parallel
  await Promise.all(
    webhooks.map(async (webhook) => {
      let beforeResource: WebhookResourceData | undefined;
      let afterResource: WebhookResourceData | undefined;

      if (beforeSnap) {
        beforeResource = mapDirectoryWebhookDataToResource(beforeSnap);
      }

      if (afterSnap) {
        afterResource = mapDirectoryWebhookDataToResource(afterSnap);
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
            `Webhook ${webhook.id} failed with status ${response.status}`
          );
        }
      } catch (error) {
        console.error(
          `Failed to fire webhook ${webhook.id} to ${webhook.url}:`,
          error
        );
        // TODO: Consider retry logic or logging failed webhook deliveries
      }
    })
  );
}

// Helper to map directory webhook data to resource data
function mapDirectoryWebhookDataToResource(
  data: DirectoryWebhookData
): WebhookResourceData {
  if ("File" in data) {
    // The key is "File", but the target type in WebhookResourceData is "file".
    // We spread the nested data from data.File to flatten it.
    return { type: "file", ...data.File };
  }
  if ("Folder" in data) {
    return { type: "folder", ...data.Folder };
  }
  if ("Subfile" in data) {
    return { type: "subfile", ...data.Subfile };
  }
  if ("Subfolder" in data) {
    return { type: "subfolder", ...data.Subfolder };
  }
  if ("ShareTracking" in data) {
    return { type: "share_tracking", ...data.ShareTracking };
  }

  // This path should be unreachable for valid data, but it satisfies the compiler.
  throw new Error(
    `Unknown directory webhook data shape: ${JSON.stringify(data)}`
  );
}
