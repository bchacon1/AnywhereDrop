import { defineConfig } from "@playwright/test";

// Runs the embedded Go binary (built by `make build`) and drives two browser
// contexts against it. Chromium only in CI (architecture-decisions.md A11).
export default defineConfig({
  testDir: ".",
  testMatch: /.*\.spec\.ts/,
  timeout: 60_000,
  retries: 0,
  workers: 1,
  reporter: [["list"]],
  use: {
    baseURL: "http://localhost:8080",
    trace: "retain-on-failure",
  },
  webServer: {
    // JOIN_LIMIT_PER_MINUTE: every test joins from 127.0.0.1, so the production
    // limit (10/min/IP) would refuse the eleventh join in a full run.
    command:
      "cd ../../backend && PORT=8080 JOIN_LIMIT_PER_MINUTE=1000 ./bin/server",
    url: "http://localhost:8080/health",
    reuseExistingServer: false,
    timeout: 30_000,
  },
  // chromium is the CI project (`--project=chromium`). firefox and webkit exist
  // for the one-time browser-matrix run recorded in docs/experiments/browser-matrix.md.
  projects: [
    { name: "chromium", use: { browserName: "chromium" } },
    { name: "firefox", use: { browserName: "firefox" } },
    { name: "webkit", use: { browserName: "webkit" } },
  ],
});
