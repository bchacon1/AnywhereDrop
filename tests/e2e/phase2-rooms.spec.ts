import { expect, test, type Page } from "@playwright/test";

// Phase 2 acceptance (roadmap.md 2a–2d) through the real UI: pair by code,
// relay text, drop a socket and reattach, third join rejected.

async function createRoom(page: Page): Promise<string> {
  await page.goto("/");
  await page.getByRole("button", { name: /Send \(create a room\)/ }).click();
  const code = await page.getByTestId("room-code").textContent();
  expect(code).toMatch(/^[A-HJ-KM-NP-TV-Z2-9]{6}$/);
  return code!;
}

async function joinRoom(page: Page, code: string): Promise<void> {
  await page.goto("/");
  await page.getByLabel("room code").fill(code);
  await page.getByRole("button", { name: /Receive \(join\)/ }).click();
}

test("two browsers pair by code and relay text both ways", async ({
  browser,
}) => {
  const a = await browser.newContext();
  const b = await browser.newContext();
  const pa = await a.newPage();
  const pb = await b.newPage();

  const code = await createRoom(pa);
  await expect(pa.getByTestId("peer")).toHaveText(/waiting for a peer/);

  await joinRoom(pb, code);
  await expect(pb.getByTestId("status")).toHaveText("attached");
  await expect(pa.getByTestId("peer")).toHaveText(/connected/);
  await expect(pb.getByTestId("peer")).toHaveText(/connected/);

  await pa.getByLabel("relay message").fill("hello from A");
  await pa.getByRole("button", { name: "Send via relay" }).click();
  await expect(pb.getByTestId("signaling-log")).toContainText(
    '"text":"hello from A"',
  );

  await pb.getByLabel("relay message").fill("hello from B");
  await pb.getByRole("button", { name: "Send via relay" }).click();
  await expect(pa.getByTestId("signaling-log")).toContainText(
    '"text":"hello from B"',
  );

  await a.close();
  await b.close();
});

test("dropping the socket reattaches with a new generation and the peer is told", async ({
  browser,
}) => {
  const a = await browser.newContext();
  const b = await browser.newContext();
  const pa = await a.newPage();
  const pb = await b.newPage();

  const code = await createRoom(pa);
  await joinRoom(pb, code);
  await expect(pa.getByTestId("peer")).toHaveText(/connected/);

  await pb.getByTestId("drop-socket").click();
  await expect(pa.getByTestId("peer")).toHaveText(/disconnected/);
  await expect(pb.getByTestId("signaling-log")).toContainText(
    "reattached as joiner gen=2",
  );
  await expect(pb.getByTestId("status")).toHaveText("attached");
  await expect(pa.getByTestId("peer")).toHaveText(/connected/);
  await expect(pa.getByTestId("signaling-log")).toContainText(
    "peer reattached gen=2",
  );

  await pa.getByLabel("relay message").fill("after reattach");
  await pa.getByRole("button", { name: "Send via relay" }).click();
  await expect(pb.getByTestId("signaling-log")).toContainText(
    '"text":"after reattach"',
  );

  await a.close();
  await b.close();
});

test("a third join is rejected with room_full", async ({ browser }) => {
  const a = await browser.newContext();
  const b = await browser.newContext();
  const c = await browser.newContext();
  const pa = await a.newPage();
  const pb = await b.newPage();
  const pc = await c.newPage();

  const code = await createRoom(pa);
  await joinRoom(pb, code);
  await expect(pb.getByTestId("status")).toHaveText("attached");

  await joinRoom(pc, code);
  await expect(pc.locator(".bad")).toContainText("room_full");

  await a.close();
  await b.close();
  await c.close();
});

test("joining an unknown code shows room_not_found", async ({ page }) => {
  await joinRoom(page, "ZZZZZZ");
  await expect(page.locator(".bad")).toContainText("room_not_found");
});
