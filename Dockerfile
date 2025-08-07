FROM node:20-slim AS builder

# Install build dependencies for better-sqlite3 and sqlite3
RUN apt-get update && \
    apt-get install -y python3 make g++ sqlite3 && \
    rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy package files
COPY package*.json ./
COPY tsconfig.json ./

# Install all dependencies (including dev for building)
RUN npm ci

# Copy source code
COPY src ./src

# Build TypeScript
RUN npm run build

# Production stage
FROM node:20-slim

# Install curl for healthcheck, sqlite3 for database operations, and python3 for node-gyp
RUN apt-get update && \
    apt-get install -y curl sqlite3 python3 && \ 
    rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install only production dependencies
RUN npm ci --only=production

# Copy built application from builder stage
COPY --from=builder /app/dist ./dist

# Create data directory structure
RUN mkdir -p /data/drives

# Create non-root user
RUN groupadd -g 1001 app && \
    useradd -r -u 1001 -g app app && \
    chown -R app:app /app /data

# Switch to non-root user
USER app

# Expose port
EXPOSE 8888

# Health check
# HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
#   CMD curl -f http://localhost:8888/health || exit 1

# Start the application
CMD ["sh", "-c", "mkdir -p /data/drives && node dist/server.js"]