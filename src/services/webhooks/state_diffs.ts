import { db, dbHelpers } from "../database";
import {
  Webhook,
  WebhookEventLabel,
  DriveID,
  StateDiffRecord,
  DriveStateDiffID,
  IDPrefixEnum,
} from "@officexapp/types";
import { v4 as uuidv4 } from "uuid";

/**
 * Get all active webhooks for state diff events
 * @returns Array of active webhooks subscribed to state diff events
 */
export async function get_active_state_diff_webhooks(
  driveId: DriveID
): Promise<Webhook[]> {
  try {
    // Query webhooks table for active state diff webhooks
    const webhooks = await db.queryDrive(
      driveId,
      `SELECT * FROM webhooks 
             WHERE event = ? AND is_active = 1
             ORDER BY created_at DESC`,
      [WebhookEventLabel.DRIVE_STATE_DIFFS]
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
export async function fire_state_diff_webhooks(
  forwardDiff: string,
  backwardDiff: string,
  forwardChecksum: string,
  backwardChecksum: string,
  notes: string | null,
  driveId: DriveID,
  endpointUrl: string
): Promise<void> {
  try {
    // Get current timestamp
    const timestampNs = BigInt(Date.now()) * 1_000_000n; // Convert to nanoseconds
    const timestampMs = Number(timestampNs / 1_000_000n); // Convert to milliseconds

    // Generate a unique ID for this state diff
    const driveStateDiffId =
      `${IDPrefixEnum.DriveStateDiffID}${uuidv4()}` as DriveStateDiffID;

    // Get all active webhooks for state diffs
    const webhooks = await get_active_state_diff_webhooks(driveId);

    // Prepare the state diff record
    const stateDiffRecord: StateDiffRecord = {
      id: driveStateDiffId,
      timestamp_ns: timestampNs,
      implementation: "JAVASCRIPT_RUNTIME",
      diff_forward: forwardDiff,
      diff_backward: backwardDiff,
      notes: notes || undefined,
      drive_id: driveId,
      endpoint_url: endpointUrl,
      checksum_forward: forwardChecksum,
      checksum_backward: backwardChecksum,
    };

    // Fire each webhook
    for (const webhook of webhooks) {
      try {
        const payload = {
          event: WebhookEventLabel.DRIVE_STATE_DIFFS,
          timestamp_ms: timestampMs,
          nonce: timestampMs,
          notes: notes || undefined,
          webhook_id: webhook.id,
          webhook_alt_index: webhook.alt_index,
          payload: {
            before: null,
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
            `Webhook ${webhook.id} failed with status ${response.status}`
          );
        }
      } catch (error) {
        console.error(`Error firing webhook ${webhook.id}:`, error);
      }
    }
  } catch (error) {
    console.error("Error in fire_state_diff_webhooks:", error);
  }
}
