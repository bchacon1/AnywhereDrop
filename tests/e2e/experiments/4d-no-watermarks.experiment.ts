import { writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type Page } from "@playwright/test";

// Experiment 4d (docs/experiments/4d-no-watermarks.md): send a 50 MiB file
// with limit 1 (bufferedAmount watermarks) disabled and RECORD what the
// browser does: peak bufferedAmount, whether send() threw, whether the
// channel closed, whether the transfer completed. No outcome is asserted.

async function createRoom(page: Page, query: string): Promise<string> {
  await page.goto("/" + query);
  await page.getByRole("button", { name: /Send \(create a room\)/ }).click();
  return (await page.getByTestId("room-code").textContent())!;
}

for (const variant of [
  { name: "limit 1 disabled (window still on)", query: "?noLimit1=1" },
  { name: "both limits disabled", query: "?noLimit1=1&noLimit2=1" },
]) {
  test(`${variant.name}: record the outcome`, async ({ browser }, testInfo) => {
    const size = 50 * 1024 * 1024;
    const path = join(tmpdir(), `anywheredrop-4d-${process.pid}.bin`);
    const buf = Buffer.alloc(size);
    for (let i = 0; i < size; i++) buf[i] = (i * 7 + 3) & 0xff;
    await writeFile(path, buf);

    const a = await browser.newContext();
    const b = await browser.newContext();
    const pa = await a.newPage();
    const pb = await b.newPage();
    const code = await createRoom(pa, variant.query);
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

    await pa.getByTestId("file-input").setInputFiles(path);
    await pb.getByTestId("accept").click();

    let peak = 0;
    let channelClosed = false;
    const t0 = Date.now();
    let state = "";
    while (Date.now() - t0 < 120_000) {
      const ba = (await pa.getByTestId("buffered-amount").textContent()) ?? "0";
      const n = parseSize(ba);
      if (n > peak) peak = n;
      const cs = (await pa.getByTestId("channel-state").textContent()) ?? "";
      if (/closed/.test(cs)) channelClosed = true;
      state = (await pa.getByTestId("sender-state").textContent()) ?? "";
      if (state === "done" || state === "failed" || state === "cancelled")
        break;
      await pa.waitForTimeout(50);
    }
    const error = (await pa.locator(".bad").allTextContents()).join(" | ");
    const stalls = await pa.getByTestId("stalls").textContent();
    const receiverState = await pb.getByTestId("receiver-state").textContent();
    const log = (await pa.getByTestId("transfer-log").allInnerTexts())
      .join(" ")
      .slice(-400);
    const result = {
      variant: variant.name,
      project: testInfo.project.name,
      size,
      senderFinalState: state,
      receiverFinalState: receiverState,
      peakBufferedAmountBytes: peak,
      sendThrew: /send_failure/.test(error),
      channelClosed,
      errorText: error,
      stalls,
      wallMs: Date.now() - t0,
      logTail: log,
    };
    console.log(`[4d] ${JSON.stringify(result)}`);
    await a.close();
    await b.close();
  });
}

function parseSize(s: string): number {
  const m = /([\d.]+)\s*(B|KiB|MiB)/.exec(s);
  if (!m) return 0;
  const v = Number(m[1]);
  return m[2] === "MiB" ? v * 1024 * 1024 : m[2] === "KiB" ? v * 1024 : v;
}
