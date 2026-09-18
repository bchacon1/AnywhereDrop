import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

// In dev, Vite serves the app and proxies the backend endpoints to the Go
// process on :8080. In production the Go binary serves the built assets.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/health": "http://localhost:8080",
      "/api": "http://localhost:8080",
      "/ws": { target: "ws://localhost:8080", ws: true },
    },
  },
  build: { outDir: "dist", sourcemap: true },
  test: {
    environment: "node",
    include: ["tests/**/*.test.ts", "src/**/*.test.ts"],
  },
});
