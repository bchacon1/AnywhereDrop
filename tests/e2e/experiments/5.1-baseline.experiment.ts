import { writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type Page } from "@playwright/test";

// Experiment 5.1 (docs/experiments/5.1-baseline.md): first performance
// baseline with the provisional constants. Localhost, same-engine pair.
// Records all timing points per run and the hasher's standalone throughput.
// No assertions.

const SIZES = [10 * 1000 * 1000, 50 * 1000 * 1000, 200 * 1000 * 1000]; // 10/50/200 MB (decimal)
const RUNS = 3;

async function patternFile(size: number, name: string): Promise<string> {
  const b = Buffer.alloc(size);
  for (let i = 0; i < size; i++) b[i] = (i * 7 + 3) & 0xff;
  const path = join(tmpdir(), `anywheredrop-51-${process.pid}-${name}`);
  await writeFile(path, b);
  return path;
}

function parseTiming(s: string | null): Record<string, number> {
  const out: Record<string, number> = {};
  for (const part of (s ?? "").split(" ")) {
    const [k, v] = part.split("=");
    if (k && v) out[k] = Number(v);
  }
  return out;
}

async function connect(
  browser: import("@playwright/test").Browser,
): Promise<{ pa: Page; pb: Page; close: () => Promise<void> }> {
  const a = await browser.newContext();
  const b = await browser.newContext();
  const pa = await a.newPage();
  const pb = await b.newPage();
  await pa.goto("/");
  await pa.getByRole("button", { name: /Send \(create a room\)/ }).click();
  const code = (await pa.getByTestId("room-code").textContent())!;
  await pb.goto("/");
  await pb.getByLabel("room code").fill(code);
  await pb.getByRole("button", { name: /Receive \(join\)/ }).click();
  await pa
    .getByTestId("channel-state")
    .filter({ hasText: "open" })
    .waitFor({ timeout: 20_000 });
  await pb
    .getByTestId("channel-state")
    .filter({ hasText: "open" })
    .waitFor({ timeout: 20_000 });
  return {
    pa,
    pb,
    close: async () => {
      await a.close();
      await b.close();
    },
  };
}

test("hasher throughput (standalone, in-worker)", async ({
  page,
}, testInfo) => {
  await page.goto("/");
  for (const mb of [64, 256]) {
    const r = await page.evaluate(
      (bytes) =>
        (
          window as unknown as {
            __benchSha256: (n: number) => Promise<{ ms: number; mbps: number }>;
          }
        ).__benchSha256(bytes),
      mb * 1000 * 1000,
    );
    console.log(
      `[5.1-hash] ${JSON.stringify({ project: testInfo.project.name, bytes: mb * 1000 * 1000, ms: Math.round(r.ms), MBps: Number(r.mbps.toFixed(1)) })}`,
    );
  }
});

for (const size of SIZES) {
  for (let run = 1; run <= RUNS; run++) {
    test(`baseline ${size / 1e6} MB run ${run}`, async ({
      browser,
    }, testInfo) => {
      const path = await patternFile(size, `${size}-${run}.bin`);
      const { pa, pb, close } = await connect(browser);
      await pa.getByTestId("file-input").setInputFiles(path);
      await pb.getByTestId("accept").click();
      const t0 = Date.now();
      await pb
        .getByTestId("receiver-state")
        .filter({ hasText: "done" })
        .waitFor({ timeout: 300_000 });
      await pa
        .getByTestId("sender-state")
        .filter({ hasText: /done|failed/ })
        .waitFor({ timeout: 60_000 });
      const wall = Date.now() - t0;
      const st = parseTiming(
        await pa.getByTestId("sender-timing").textContent(),
      );
      const rt = parseTiming(
        await pb.getByTestId("receiver-timing").textContent(),
      );
      const stalls = await pa.getByTestId("stalls").textContent();
      const verified = await pb.getByTestId("verified").textContent();
      const mms = await pa.getByTestId("max-message-size").textContent();
      const throughputMBps =
        size / ((rt.t_verified! - rt.t_accept!) / 1000) / 1e6;
      const enqueueMBps = size / ((st.t_enqueued! - st.t_accept!) / 1000) / 1e6;
      const out = {
        project: testInfo.project.name,
        size,
        run,
        verified,
        maxMessageSize: Number(mms),
        stalls,
        wallMs: wall,
        sender: st,
        receiver: rt,
        throughput_MBps_receiver_verified: Number(throughputMBps.toFixed(2)),
        enqueue_rate_MBps: Number(enqueueMBps.toFixed(2)),
        hash_wait_ms: Math.round(st.t_complete_sent! - st.t_enqueued!),
        finalize_ms: Math.round(rt.t_verified! - rt.t_committed_all!),
      };
      console.log(`[5.1] ${JSON.stringify(out)}`);
      await close();
    });
  }
}
