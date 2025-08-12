export const LOCAL_DEV_MODE = process.env.NODE_ENV !== "production";
export const FREE_MODE = true;

export const frontend_endpoint = LOCAL_DEV_MODE
  ? "http://localhost:5173"
  : "https://officex.app";
