import mixpanel from "mixpanel";
import { LOCAL_DEV_MODE } from "../constants";

// The Mixpanel client instance. It will be null if no token is provided.
let mixpanelClient: mixpanel.Mixpanel | null = null;

// Initialize the client if a token is available.
if (process.env.MIXPANEL_TOKEN) {
  mixpanelClient = mixpanel.init(process.env.MIXPANEL_TOKEN);
}

/**
 * A helper function to track an event.
 * It safely handles cases where Mixpanel is not initialized.
 * @param eventName The name of the event to track.
 * @param properties An optional object of properties for the event.
 */
export const trackEvent = (eventName: string, properties?: any) => {
  if (mixpanelClient) {
    // Only track the event if the client is initialized.
    mixpanelClient.track(eventName, properties);
  } else if (LOCAL_DEV_MODE) {
    // Optionally log the event in dev mode for debugging.
    console.log(`[Analytics] Mock tracking event: ${eventName}`, properties);
  }
};
