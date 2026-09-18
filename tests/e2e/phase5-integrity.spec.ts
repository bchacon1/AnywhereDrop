import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test, type Page } from "@playwright/test";

// Phase 5 acceptance: the receiver's SHA-256 equals the sender's and the
// file's real digest; a corrupted byte (dev flag) fails verification on both
// sides and no download is offered.

function pattern(size: number): Buffer {
  const b = Buffer.alloc(size);
  for (let i = 0; i < size; i++) b[i] = (i * 7 + 3) & 0xff;
  return b;
}
const sha256 = (b: Buffer) => createHash("sha256").update(b).digest("hex");

async function patternFile(size: number, name: string) {
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
async function connect(pa: Page, pb: Page, qb = ""): Promise<void> {
  const code = await createRoom(pa);
  await joinRoom(pb, code, qb);
  await expect(pa.getByTestId("channel-state")).toHaveText(/open/, {
    timeout: 20_000,
  });
  await expect(pb.getByTestId("channel-state")).toHaveText(/open/, {
    timeout: 20_000,
  });
}

test("verified transfer: receiver, sender and file digests agree", async ({
  browser,
}) => {
  const a = await browser.newContext();
  const b = await browser.newContext({ acceptDownloads: true });
  const pa = await a.newPage();
  const pb = await b.newPage();
  await connect(pa, pb);
  const { path, original } = await patternFile(8 * 1024 * 1024, "p5.bin");
  await pa.getByTestId("file-input").setInputFiles(path);
  await pb.getByTestId("accept").click();
  await expect(pb.getByTestId("receiver-state")).toHaveText("done", {
    timeout: 60_000,
  });
  await expect(pa.getByTestId("sender-state")).toHaveText("done", {
    timeout: 30_000,
  });
  await expect(pb.getByTestId("verified")).toHaveText("yes");
  const expected = sha256(original);
  await expect(pb.getByTestId("receiver-sha256")).toHaveText(expected);
  await expect(pa.getByTestId("sender-sha256")).toHaveText(expected);
  const [download] = await Promise.all([
    pb.waitForEvent("download"),
    pb.getByTestId("download-link").click(),
  ]);
  expect(sha256(await readFile((await download.path())!))).toBe(expected);
  await a.close();
  await b.close();
});

test("a corrupted byte fails verification on both sides and offers no download", async ({
  browser,
}) => {
  const a = await browser.newContext();
  const b = await browser.newContext();
  const pa = await a.newPage();
  const pb = await b.newPage();
  await connect(pa, pb, "?corruptByte=1");
  const { path } = await patternFile(2 * 1024 * 1024, "p5-corrupt.bin");
  await pa.getByTestId("file-input").setInputFiles(path);
  await pb.getByTestId("accept").click();
  await expect(pb.getByTestId("receiver-state")).toHaveText("failed", {
    timeout: 60_000,
  });
  await expect(pb.getByTestId("verified")).toHaveText("FAILED");
  await expect(pb.locator(".bad")).toContainText("hash_mismatch");
  await expect(pa.getByTestId("sender-state")).toHaveText("failed");
  await expect(pa.locator(".bad")).toContainText("hash_mismatch");
  await expect(pb.getByTestId("download-link")).toHaveCount(0);
  await a.close();
  await b.close();
});
