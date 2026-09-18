import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test, type Page } from "@playwright/test";

// Phase 4 acceptance (roadmap.md 4b–4f): a file moves creator → joiner over
// the DataChannel and byte-compares equal; both sender limits are observed;
// cancel leaves both sides sane; an oversize offer is rejected.

function pattern(size: number): Buffer {
  const b = Buffer.alloc(size);
  for (let i = 0; i < size; i++) b[i] = (i * 7 + 3) & 0xff;
  return b;
}
const sha256 = (b: Buffer) => createHash("sha256").update(b).digest("hex");

/** Playwright caps inline buffers at 50 MB, so test files go through the filesystem. */
async function patternFile(
  size: number,
  name: string,
): Promise<{ path: string; original: Buffer }> {
  const original = pattern(size);
  const path = join(tmpdir(), `anywheredrop-${process.pid}-${name}`);
  await writeFile(path, original);
  return { path, original };
}

async function createRoom(page: Page, query = ""): Promise<string> {
  await page.goto("/" + query);
  await page.getByRole("button", { name: /Send \(create a room\)/ }).click();
  return (await page.getByTestId("room-code").textContent())!;
}
async function joinRoom(page: Page, code: string, query = ""): Promise<void> {
  await page.goto("/" + query);
  await page.getByLabel("room code").fill(code);
  await page.getByRole("button", { name: /Receive \(join\)/ }).click();
}
async function connect(pa: Page, pb: Page, qa = "", qb = ""): Promise<void> {
  const code = await createRoom(pa, qa);
  await joinRoom(pb, code, qb);
  await expect(pa.getByTestId("channel-state")).toHaveText(/open/, {
    timeout: 20_000,
  });
  await expect(pb.getByTestId("channel-state")).toHaveText(/open/, {
    timeout: 20_000,
  });
}

async function transfer(pa: Page, pb: Page, size: number, name: string) {
  const { path: inputPath, original } = await patternFile(size, name);
  await pa.getByTestId("file-input").setInputFiles(inputPath);
  await expect(pb.getByTestId("offer-name")).toContainText(name);
  await pb.getByTestId("accept").click();
  await expect(pb.getByTestId("receiver-state")).toHaveText("done", {
    timeout: 120_000,
  });
  await expect(pa.getByTestId("sender-state")).toHaveText("done", {
    timeout: 30_000,
  });
  const [download] = await Promise.all([
    pb.waitForEvent("download"),
    pb.getByTestId("download-link").click(),
  ]);
  const downloaded = await readFile((await download.path())!);
  return { downloaded, original };
}

for (const size of [1024 * 1024, 50 * 1024 * 1024]) {
  test(`transfers a ${size / 1024 / 1024} MiB file byte-exactly`, async ({
    browser,
  }, testInfo) => {
    const a = await browser.newContext();
    const b = await browser.newContext({ acceptDownloads: true });
    const pa = await a.newPage();
    const pb = await b.newPage();
    await connect(pa, pb);
    const t0 = Date.now();
    const { downloaded, original } = await transfer(
      pa,
      pb,
      size,
      `p4-${size}.bin`,
    );
    const wall = Date.now() - t0;
    expect(downloaded.length).toBe(original.length);
    expect(sha256(downloaded)).toBe(sha256(original));
    const stalls = await pa.getByTestId("stalls").textContent();
    const senderTiming = await pa.getByTestId("sender-timing").textContent();
    const receiverTiming = await pb
      .getByTestId("receiver-timing")
      .textContent();
    console.log(
      `[p4] size=${size} wall=${wall}ms stalls=${stalls} sender{${senderTiming}} receiver{${receiverTiming}}`,
    );
    testInfo.annotations.push({ type: "stalls", description: stalls ?? "" });
    await a.close();
    await b.close();
  });
}

test("cancel from the sender mid-transfer leaves both sides cancelled", async ({
  browser,
}) => {
  const a = await browser.newContext();
  const b = await browser.newContext();
  const pa = await a.newPage();
  const pb = await b.newPage();
  await connect(pa, pb, "", "?slowWriteMs=20"); // slow receiver worker keeps the transfer running
  await pa
    .getByTestId("file-input")
    .setInputFiles((await patternFile(8 * 1024 * 1024, "cancel.bin")).path);
  await pb.getByTestId("accept").click();
  await expect(pa.getByTestId("sender-state")).toHaveText("transferring");
  await pa.getByRole("button", { name: "Cancel" }).click();
  await expect(pa.getByTestId("sender-state")).toHaveText("cancelled");
  await expect(pb.getByTestId("receiver-state")).toHaveText("cancelled");
  await a.close();
  await b.close();
});

test("cancel from the receiver mid-transfer leaves both sides cancelled", async ({
  browser,
}) => {
  const a = await browser.newContext();
  const b = await browser.newContext();
  const pa = await a.newPage();
  const pb = await b.newPage();
  await connect(pa, pb, "", "?slowWriteMs=20");
  await pa
    .getByTestId("file-input")
    .setInputFiles((await patternFile(8 * 1024 * 1024, "cancel2.bin")).path);
  await pb.getByTestId("accept").click();
  await expect(pb.getByTestId("receiver-state")).toHaveText("transferring");
  await pb.getByRole("button", { name: "Cancel" }).click();
  await expect(pb.getByTestId("receiver-state")).toHaveText("cancelled");
  await expect(pa.getByTestId("sender-state")).toHaveText("cancelled");
  await a.close();
  await b.close();
});

test("receiver window (limit 2) engages with a slow worker and the transfer still completes", async ({
  browser,
}) => {
  const a = await browser.newContext();
  const b = await browser.newContext({ acceptDownloads: true });
  const pa = await a.newPage();
  const pb = await b.newPage();
  await connect(pa, pb, "", "?slowWriteMs=2");
  const { downloaded, original } = await transfer(
    pa,
    pb,
    16 * 1024 * 1024,
    "window.bin",
  );
  expect(sha256(downloaded)).toBe(sha256(original));
  const stalls = (await pa.getByTestId("stalls").textContent()) ?? "";
  console.log(`[p4] slow-worker stalls=${stalls}`);
  expect(Number(stalls.split("/")[1])).toBeGreaterThan(0);
  await a.close();
  await b.close();
});

test("an offer above maxFileSize is refused by the sender", async ({
  browser,
}) => {
  const a = await browser.newContext();
  const b = await browser.newContext();
  const pa = await a.newPage();
  const pb = await b.newPage();
  await connect(pa, pb);
  // The policy is checked against File.size; the content does not matter.
  const big = join(tmpdir(), `anywheredrop-${process.pid}-big.bin`);
  await writeFile(big, Buffer.alloc(200 * 1024 * 1024 + 1));
  await pa.getByTestId("file-input").setInputFiles(big);
  await expect(pa.getByTestId("sender-state")).toHaveText("failed");
  await expect(pa.locator(".bad")).toContainText("too_large");
  await a.close();
  await b.close();
});
