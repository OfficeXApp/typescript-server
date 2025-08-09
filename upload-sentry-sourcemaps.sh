#!/bin/bash

# Sentry Source Maps Upload Script
# This script uploads source maps to Sentry for better error tracking.
# It's separate from the main build process to keep the repo open-source friendly.

set -e  # Exit on any error

echo "🔨 Building TypeScript project..."
npm run build

# Check if .sentryclirc exists (contains auth token)
if [ ! -f ".sentryclirc" ]; then
    echo "❌ Error: .sentryclirc file not found!"
    echo "   Please run the Sentry wizard first or create the auth config file."
    echo "   Run: npx @sentry/wizard@latest -i sourcemaps --saas --org officex --project officex-official"
    exit 1
fi

# Check if dist directory exists
if [ ! -d "./dist" ]; then
    echo "❌ Error: ./dist directory not found!"
    echo "   Please run 'npm run build' first to compile TypeScript."
    exit 1
fi

echo "📤 Injecting source map references..."
npx sentry-cli sourcemaps inject --org officex --project officex-official ./dist

echo "🚀 Uploading source maps to Sentry..."
npx sentry-cli sourcemaps upload --org officex --project officex-official ./dist

echo "✅ Source maps uploaded successfully!"
echo "   View your project at: https://sentry.io"
