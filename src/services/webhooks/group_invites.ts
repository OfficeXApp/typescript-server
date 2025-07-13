// src/services/webhooks/group_invites.ts

import { db } from "../database";
import { Webhook, WebhookEventLabel, GroupID } from "@officexapp/types";
import fetch from "node-fetch";

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
