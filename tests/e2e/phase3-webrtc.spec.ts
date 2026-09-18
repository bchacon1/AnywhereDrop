import { expect, test, type Page } from "@playwright/test";

// Phase 3 acceptance (roadmap.md): a DataChannel between two contexts
// negotiated through the Phase 2 relay; text crosses it directly; ice_restart
// produces a new epoch and the channel stays usable; dropping the signaling
// socket does not affect the channel.

async function createRoom(page: Page): Promise<string> {
  await page.goto("/");
  await page.getByRole("button", { name: /Send \(create a room\)/ }).click();
  return (await page.getByTestId("room-code").textContent())!;
}

async function joinRoom(page: Page, code: string): Promise<void> {
  await page.goto("/");
  await page.getByLabel("room code").fill(code);
  await page.getByRole("button", { name: /Receive \(join\)/ }).click();
}

async function pairAndConnect(pa: Page, pb: Page): Promise<void> {
  const code = await createRoom(pa);
  await joinRoom(pb, code);
  await expect(pa.getByTestId("connection-state")).toHaveText("connected", {
    timeout: 20_000,
  });
  await expect(pb.getByTestId("connection-state")).toHaveText("connected", {
    timeout: 20_000,
  });
  await expect(pa.getByTestId("channel-state")).toHaveText(
    /open \(generation 1\)/,
  );
  await expect(pb.getByTestId("channel-state")).toHaveText(
    /open \(generation 1\)/,
  );
}

async function sendDC(from: Page, to: Page, text: string): Promise<void> {
  await from.getByLabel("datachannel message").fill(text);
  await from.getByRole("button", { name: "Send via DataChannel" }).click();
  await expect(to.getByTestId("dc-received")).toContainText(text);
}

test("two browsers connect a DataChannel and exchange text directly", async ({
  browser,
}, testInfo) => {
  const a = await browser.newContext();
  const b = await browser.newContext();
  const pa = await a.newPage();
  const pb = await b.newPage();
  await pairAndConnect(pa, pb);

  await sendDC(pa, pb, "dc hello from A");
  await sendDC(pb, pa, "dc hello from B");

  // Record what this browser pair negotiated (browser-matrix.md). Stats are
  // polled once a second, so wait for a real candidate type to appear.
  await expect(pa.getByTestId("candidate-type")).toHaveText(
    /host|srflx|prflx|relay/,
    { timeout: 10_000 },
  );
  const candidateType = await pa.getByTestId("candidate-type").textContent();
  const maxMessageSize = await pa.getByTestId("max-message-size").textContent();
  testInfo.annotations.push({
    type: "candidateType",
    description: candidateType ?? "",
  });
  testInfo.annotations.push({
    type: "maxMessageSize",
    description: maxMessageSize ?? "",
  });
  console.log(
    `[matrix] ${testInfo.project.name}: candidateType=${candidateType} maxMessageSize=${maxMessageSize}`,
  );
  expect(Number(maxMessageSize)).toBeGreaterThan(0);

  await a.close();
  await b.close();
});

test("ice_restart produces a new epoch and the channel keeps working", async ({
  browser,
}) => {
  const a = await browser.newContext();
  const b = await browser.newContext();
  const pa = await a.newPage();
  const pb = await b.newPage();
  await pairAndConnect(pa, pb);

  await pa.getByTestId("ice-restart").click();
  await expect(pa.getByTestId("link-epoch")).toHaveText("2");
  await expect(pb.getByTestId("link-epoch")).toHaveText("2");
  await expect(pa.getByTestId("connection-state")).toHaveText("connected", {
    timeout: 20_000,
  });
  await expect(pa.getByTestId("channel-state")).toHaveText(
    /open \(generation 1\)/,
  );
  await sendDC(pa, pb, "after ice restart");

  await a.close();
  await b.close();
});

test("dropping the signaling socket does not affect the DataChannel", async ({
  browser,
}) => {
  const a = await browser.newContext();
  const b = await browser.newContext();
  const pa = await a.newPage();
  const pb = await b.newPage();
  await pairAndConnect(pa, pb);

  await pb.getByTestId("drop-socket").click();
  await expect(pb.getByTestId("signaling-log")).toContainText(
    "reattached as joiner gen=2",
  );
  await expect(pb.getByTestId("channel-state")).toHaveText(
    /open \(generation 1\)/,
  );
  await sendDC(pa, pb, "channel survived ws drop");
  await sendDC(pb, pa, "and back");

  await a.close();
  await b.close();
});
