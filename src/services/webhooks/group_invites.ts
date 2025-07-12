// src/services/webhooks/group_invites.ts

import { db } from "../database";
import { Webhook, WebhookEventLabel, GroupID } from "@officexapp/types";
import fetch from "node-fetch";

/**
 * Gets all active webhooks for a group invite event
 * @param groupId The ID of the group to get webhooks for
 * @param event The webhook event type
 * @returns Array of active webhooks matching the criteria
 */
export async function get_active_group_invite_webhooks(
  groupId: GroupID,
  event: WebhookEventLabel
): Promise<Webhook[]> {
  try {
    // Get all webhooks for this group's alt_index
    const webhooks = await db.queryDrive(
      // TODO: GROUP Need to determine drive ID from group ID - this may require a service we don't have yet
      // For now, we'll assume the webhooks table is in the factory DB
      "factory",
      `SELECT * FROM webhooks 
       WHERE alt_index = ? AND event = ? AND is_active = 1`,
      [groupId, event]
    );

    return webhooks as Webhook[];
  } catch (error) {
    console.error("Error getting active group invite webhooks:", error);
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
export async function fire_group_invite_webhook(
  event: WebhookEventLabel,
  webhooks: Webhook[],
  beforeSnap?: any, // TODO: WEBHOOK Replace with proper GroupInviteWebhookData type
  afterSnap?: any, // TODO: WEBHOOK Replace with proper GroupInviteWebhookData type
  notes?: string
): Promise<void> {
  const timestampMs = Date.now();

  // Fire each webhook in parallel
  await Promise.all(
    webhooks.map(async (webhook) => {
      try {
        const payload = {
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
            `Webhook ${webhook.id} failed with status ${response.status}`
          );
        }
      } catch (error) {
        console.error(`Error firing webhook ${webhook.id}:`, error);
      }
    })
  );
}
