// src/services/rate-limit.ts
import NodeCache from "node-cache";
import { FastifyRequest, FastifyReply } from "fastify";
import { ApiError, ApiResponse } from "@officexapp/types";

interface RateLimitConfig {
  ipLimit: number;
  ipWindowMs: number;
  orgLimit: number;
  orgWindowMs: number;
}

// Default configuration for rate limits
const DEFAULT_RATE_LIMIT_CONFIG: RateLimitConfig = {
  ipLimit: 100, // Max 100 requests per IP per minute
  ipWindowMs: 60 * 1000, // 1 minute
  orgLimit: 600, // Max 600 requests per organization per minute
  orgWindowMs: 60 * 1000, // 1 minute
};

class RateLimiter {
  private ipCache: NodeCache;
  private orgCache: NodeCache;
  private factoryCache: NodeCache; // For factory-level routes
  private config: RateLimitConfig;

  constructor(config?: Partial<RateLimitConfig>) {
    this.config = { ...DEFAULT_RATE_LIMIT_CONFIG, ...config };

    // IP cache: key is 'ip:<ipAddress>:<orgId>', value is current count
    this.ipCache = new NodeCache({
      stdTTL: this.config.ipWindowMs / 1000,
      checkperiod: 120,
    });

    // Organization cache: key is 'org:<orgId>', value is current count
    this.orgCache = new NodeCache({
      stdTTL: this.config.orgWindowMs / 1000,
      checkperiod: 120,
    });

    // Factory cache: key is 'factory:total', value is current count
    this.factoryCache = new NodeCache({
      stdTTL: this.config.orgWindowMs / 1000,
      checkperiod: 120,
    }); // Using orgWindowMs for factory too
  }

  /**
   * Checks and increments the rate limit counters for an IP within a specific organization.
   * @param ip The IP address of the requester.
   * @param orgId The ID of the organization.
   * @returns True if the request is allowed, false if rate limited.
   */
  public checkIpAndOrgLimit(ip: string, orgId: string): boolean {
    const ipKey = `ip:${ip}:${orgId}`;
    const orgKey = `org:${orgId}`;

    // Get current counts, default to 0 if not found
    let ipCount: number = this.ipCache.get(ipKey) || 0;
    let orgCount: number = this.orgCache.get(orgKey) || 0;

    // Check limits
    if (ipCount >= this.config.ipLimit || orgCount >= this.config.orgLimit) {
      return false; // Rate limited
    }

    // Increment counts
    this.ipCache.set(ipKey, ipCount + 1);
    this.orgCache.set(orgKey, orgCount + 1);

    return true; // Request allowed
  }

  /**
   * Checks and increments the rate limit counter for factory-level routes.
   * @param ip The IP address of the requester (optional, though not strictly used for limit breakdown here).
   * @returns True if the request is allowed, false if rate limited.
   */
  public checkFactoryLimit(): boolean {
    const factoryKey = `factory:total`;

    let factoryCount: number = this.factoryCache.get(factoryKey) || 0;

    if (factoryCount >= this.config.orgLimit) {
      // Using orgLimit for factory-wide limit
      return false; // Rate limited
    }

    this.factoryCache.set(factoryKey, factoryCount + 1);
    return true; // Request allowed
  }
}

// Export a singleton instance of the RateLimiter
export const rateLimiter = new RateLimiter();

/**
 * Fastify preHandler hook for rate limiting Drive/Organization specific routes.
 * This middleware should be applied to routes that operate within an organization context.
 */
export const driveRateLimitPreHandler = async (
  request: FastifyRequest & { params: { org_id?: string } },
  reply: FastifyReply
) => {
  const ip = request.ip;
  const orgId = request.params.org_id;

  if (!orgId) {
    request.log.warn(
      `Rate limiter: Missing org_id for drive route from IP: ${ip}`
    );
    // Consider what to do if org_id is missing, perhaps an internal server error or specific handling.
    // For now, we'll allow it to proceed, assuming other validation will catch it.
    return;
  }

  if (!rateLimiter.checkIpAndOrgLimit(ip, orgId)) {
    request.log.warn(`Rate limit hit for IP: ${ip} on Org: ${orgId}`);
    return reply.status(429).send({
      err: {
        code: 429,
        message: "Too Many Requests. Please try again later.",
      },
    } as ApiError);
  }
};

/**
 * Fastify preHandler hook for rate limiting Factory specific routes.
 * This middleware should be applied to routes that operate at the factory level.
 */
export const factoryRateLimitPreHandler = async (
  request: FastifyRequest,
  reply: FastifyReply
) => {
  const ip = request.ip; // Not strictly used for the limit itself, but good for logging

  if (!rateLimiter.checkFactoryLimit()) {
    request.log.warn(`Factory rate limit hit from IP: ${ip}`);
    return reply.status(429).send({
      err: {
        code: 429,
        message: "Too Many Requests. Please try again later.",
      },
    } as ApiError);
  }
};
