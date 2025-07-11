// src/services/webhooks/labels.ts
import { db } from "../database";
import {
  Webhook,
  WebhookEventLabel,
  LabelID,
  WebhookID,
} from "@officexapp/types";
import fetch from "node-fetch";

/**
 * Retrieves all active webhooks for a specific label and event
 * @param labelId The ID of the label to get webhooks for
 * @param event The webhook event type to filter by
 * @returns Array of active webhooks matching the criteria
 */
export async function get_active_label_webhooks(
  labelId: LabelID,
  event: WebhookEventLabel
): Promise<Webhook[]> {
  try {
    // Query webhooks associated with this label
    const webhookLabels = await db.queryDrive(
      labelId.split("_")[1], // Extract drive ID from label ID
      `SELECT webhook_id FROM webhook_labels WHERE label_id = ?`,
      [labelId]
    );

    if (!webhookLabels || webhookLabels.length === 0) {
      return [];
    }

    // Get all webhook IDs
    const webhookIds = webhookLabels.map((wl: any) => wl.webhook_id);

    // Query webhooks that are active and match the event
    const webhooks = await db.queryDrive(
      labelId.split("_")[1],
      `SELECT * FROM webhooks 
       WHERE id IN (${webhookIds.map(() => "?").join(",")}) 
       AND event = ? AND is_active = 1`,
      [...webhookIds, event]
    );

    return webhooks as Webhook[];
  } catch (error) {
    console.error("Error in get_active_label_webhooks:", error);
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
export async function fire_label_webhook(
  event: WebhookEventLabel,
  webhooks: Webhook[],
  beforeSnap?: any,
  afterSnap?: any,
  notes?: string
): Promise<void> {
  const timestampMs = Date.now();

  // Process webhooks in parallel
  await Promise.all(
    webhooks.map(async (webhook) => {
      const payload = {
        event: event.toString(),
        timestamp_ms: timestampMs,
        nonce: timestampMs,
        notes: notes || null,
        webhook_id: webhook.id,
        webhook_alt_index: webhook.alt_index,
        payload: {
          before: beforeSnap ? { type: "label", ...beforeSnap } : null,
          after: afterSnap ? { type: "label", ...afterSnap } : null,
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
        console.error(`Error firing webhook ${webhook.id}:`, error);
      }
    })
  );
}
