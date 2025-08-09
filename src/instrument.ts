import * as dotenv from "dotenv";
dotenv.config();

import * as Sentry from "@sentry/node";
import { nodeProfilingIntegration } from "@sentry/profiling-node";

// Make sure to call this before requiring any other modules!
const SENTRY_DSN = process.env.SENTRY_DSN;

if (SENTRY_DSN) {
  console.log("Initializing Sentry...");

  Sentry.init({
    dsn: SENTRY_DSN,
    integrations: [nodeProfilingIntegration()],
    // Tracing
    tracesSampleRate: 1.0, //  Capture 100% of the transactions
    // Set sampling rate for profiling - this is evaluated only once per SDK.init call
    profileSessionSampleRate: 1.0,
    // Trace lifecycle automatically enables profiling during active traces
    profileLifecycle: "trace",

    // Send structured logs to Sentry
    enableLogs: true,

    // Setting this option to true will send default PII data to Sentry.
    // For example, automatic IP address collection on events
    sendDefaultPii: true,
  });

  // Example of starting a custom span. This is optional.
  Sentry.startSpan(
    {
      name: "Server startup",
    },
    () => {
      // The code executed here will be profiled
    }
  );
} else {
  console.warn("SENTRY_DSN is not set. Sentry will not be initialized.");
}
