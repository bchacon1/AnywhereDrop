import { defineConfig } from "@playwright/test";

// Experiments are not tests: they record what happened and never assert an
// outcome. Run one explicitly, e.g.
//   npx playwright test -c playwright.experiments.config.ts 4d --project=chromium
export default defineConfig({
  testDir: "./experiments",
  testMatch: /.*\.experiment\.ts/,
  timeout: 300_000,
  retries: 0,
  workers: 1,
  reporter: [["list"]],
  use: { baseURL: "http://localhost:8080", trace: "off" },
  webServer: {
    // JOIN_LIMIT_PER_MINUTE: every test joins from 127.0.0.1, so the production
    // limit (10/min/IP) would refuse the eleventh join in a full run.
    command:
      "cd ../../backend && PORT=8080 JOIN_LIMIT_PER_MINUTE=1000 ./bin/server",
    url: "http://localhost:8080/health",
    reuseExistingServer: false,
    timeout: 30_000,
  },
  projects: [
    { name: "chromium", use: { browserName: "chromium" } },
    { name: "firefox", use: { browserName: "firefox" } },
    { name: "webkit", use: { browserName: "webkit" } },
  ],
});
