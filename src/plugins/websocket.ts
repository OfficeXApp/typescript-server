import fp from "fastify-plugin";
import websocket from "@fastify/websocket";
import { FastifyPluginAsync } from "fastify";

const websocketPlugin: FastifyPluginAsync = async (fastify) => {
  // Register @fastify/websocket once at the plugin scope
  if (!(fastify as any).hasRequestDecorator?.("ws")) {
    await fastify.register(websocket);
  } else {
    // In case another scope already registered it, do nothing
    fastify.log.debug("@fastify/websocket already registered; skipping.");
  }
};

export default fp(websocketPlugin, { name: "websocket" });
