import { db, dbHelpers } from "../database";
import {
  Webhook,
  WebhookEventLabel,
  WebhookID,
  WebhookIDList,
} from "@officexapp/types";
import { InboxOrgRequestBody } from "@officexapp/types/routes";
import fetch from "node-fetch";
import { debug_log } from "../utils";

// Constants for special webhook alt indexes
const SUPERSWAP_USER_SLUG = "SUPERSWAP_USER";
const INBOX_NEW_MAIL_SLUG = "INBOX_NEW_MAIL";

/**
 * Get webhooks for superswap user events
 * @param event The webhook event type
 * @returns Array of matching webhooks
 */
export async function get_superswap_user_webhooks(
  event: WebhookEventLabel
): Promise<Webhook[]> {
  try {
    // Get webhook IDs for the superswap user slug
    const webhookIds = await db.queryDrive(
      "org_id_placeholder", // TODO: Replace with actual org ID parameter
      `SELECT webhooks FROM webhooks WHERE alt_index = ?`,
      [SUPERSWAP_USER_SLUG]
    );

    if (!webhookIds || webhookIds.length === 0) {
      return [];
    }

    // Get all webhooks that match the event and are active
    const webhooks = await db.queryDrive(
      "org_id_placeholder", // TODO: Replace with actual org ID parameter
      `SELECT * FROM webhooks WHERE id IN (?) AND event = ? AND is_active = 1`,
      [webhookIds.map((w: any) => w.id), event]
    );

    return webhooks as Webhook[];
  } catch (error) {
    debug_log("Error in get_superswap_user_webhooks:", error);
    return [];
  }
}

/**
 * Get webhooks for organization inbox events
 * @param topic Optional topic filter
 * @returns Array of matching webhooks
 */
export async function get_org_inbox_webhooks(
  topic?: string
): Promise<Webhook[]> {
  try {
    // Get all inbox webhooks
    const webhooks = await db.queryDrive(
      "org_id_placeholder", // TODO: Replace with actual org ID parameter
      `SELECT * FROM webhooks WHERE alt_index = ? AND event = ? AND is_active = 1`,
      [INBOX_NEW_MAIL_SLUG, WebhookEventLabel.OrganizationInboxNewNotif]
    );

    if (!webhooks || webhooks.length === 0) {
      return [];
    }

    // Filter by topic if provided
    if (topic) {
      return webhooks.filter((webhook: Webhook) => {
        try {
          if (!webhook.filters) return false;

          const filters = JSON.parse(webhook.filters);
          return filters.topic === topic;
        } catch (e) {
          debug_log("Error parsing webhook filters:", e);
          return false;
        }
      });
    }

    // Return all inbox webhooks if no topic filter
    return webhooks as Webhook[];
  } catch (error) {
    debug_log("Error in get_org_inbox_webhooks:", error);
    return [];
  }
}

/**
 * Fire superswap user webhook
 * @param event The webhook event type
 * @param webhooks Array of webhooks to trigger
 * @param before_snap User ID before the change
 * @param after_snap User ID after the change
 * @param notes Optional notes about the event
 */
export async function fire_superswap_user_webhook(
  event: WebhookEventLabel,
  webhooks: Webhook[],
  before_snap?: string, // UserID
  after_snap?: string, // UserID
  notes?: string
): Promise<void> {
  const timestamp_ms = Date.now();

  for (const webhook of webhooks) {
    try {
      const payload = {
        event: event.toString(),
        timestamp_ms,
        nonce: timestamp_ms,
        notes,
        webhook_id: webhook.id,
        webhook_alt_index: webhook.alt_index,
        payload: {
          before: before_snap
            ? { type: "superswap_userid", data: before_snap }
            : null,
          after: after_snap
            ? { type: "superswap_userid", data: after_snap }
            : null,
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
        debug_log(
          `Webhook ${webhook.id} failed with status ${response.status}`
        );
      }
    } catch (error) {
      debug_log(`Error firing webhook ${webhook.id}:`, error);
    }
  }
}

/**
 * Fire organization inbox new notification webhook
 * @param event The webhook event type
 * @param webhooks Array of webhooks to trigger
 * @param before_snap Request body before the change
 * @param after_snap Request body after the change
 * @param notes Optional notes about the event
 */
export async function fire_org_inbox_new_notif_webhook(
  event: WebhookEventLabel,
  webhooks: Webhook[],
  before_snap?: InboxOrgRequestBody,
  after_snap?: InboxOrgRequestBody,
  notes?: string
): Promise<void> {
  const timestamp_ms = Date.now();

  // Process webhooks sequentially to ensure they all complete
  for (const webhook of webhooks) {
    try {
      const payload = {
        event: event.toString(),
        timestamp_ms,
        nonce: timestamp_ms,
        notes,
        webhook_id: webhook.id,
        webhook_alt_index: webhook.alt_index,
        payload: {
          before: before_snap
            ? { type: "org_inbox_new_notif", data: before_snap }
            : null,
          after: after_snap
            ? { type: "org_inbox_new_notif", data: after_snap }
            : null,
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
        debug_log(
          `Webhook ${webhook.id} failed with status ${response.status}`
        );
      }
    } catch (error) {
      debug_log(`Error firing webhook ${webhook.id}:`, error);
    }
  }
}
