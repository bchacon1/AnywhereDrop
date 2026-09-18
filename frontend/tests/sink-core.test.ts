import { describe, expect, it } from "vitest";
import { SinkCore, type SinkReply } from "../src/workers/sink-core";

function make(opts: { slowWriteMs?: number } = {}) {
  const replies: SinkReply[] = [];
  let release: (() => void)[] = [];
  const core = new SinkCore({
    post: (r) => replies.push(r),
    slowWriteMs: opts.slowWriteMs,
    // Controllable sleep: each slow write waits until the test calls releaseOne().
    sleep: () => new Promise<void>((res) => release.push(res)),
  });
  const releaseOne = () => {
    const r = release.shift();
    r?.();
  };
  const releaseAll = () => {
    const rs = release;
    release = [];
    rs.forEach((r) => r());
  };
  return { core, replies, releaseOne, releaseAll };
}

const buf = (n: number, fill = 1) => new Uint8Array(n).fill(fill).buffer;
const tick = () => new Promise<void>((r) => setTimeout(r, 0));

describe("SinkCore serialized effects (transfer-protocol.md §4.5)", () => {
  it("commits writes in order and finalizes into a Blob", async () => {
    const { core, replies } = make();
    await core.handle({ type: "begin", totalBytes: 6 });
    await core.handle({ type: "write", index: 0, offset: 0, payload: buf(4, 1) });
    await core.handle({ type: "write", index: 1, offset: 4, payload: buf(2, 2) });
    await core.handle({ type: "finalize", expectedSha256: "" });
    expect(replies.map((r) => r.type)).toEqual(["ready", "committed", "committed", "done"]);
    const done = replies[3] as Extract<SinkReply, { type: "done" }>;
    expect(done.ok).toBe(true);
    expect(await done.result!.arrayBuffer().then((b) => [...new Uint8Array(b)])).toEqual([
      1, 1, 1, 1, 2, 2,
    ]);
    expect(core.state).toBe("finalized");
  });

  it("finalize posted after a slow write runs only after that write completed", async () => {
    const { core, replies, releaseOne } = make({ slowWriteMs: 10 });
    await core.handle({ type: "begin", totalBytes: 4 });
    void core.handle({ type: "write", index: 0, offset: 0, payload: buf(4) });
    void core.handle({ type: "finalize", expectedSha256: "" });
    await tick();
    expect(replies.map((r) => r.type)).toEqual(["ready"]); // write is pending on the slow sleep
    releaseOne();
    await tick();
    await tick();
    expect(replies.map((r) => r.type)).toEqual(["ready", "committed", "done"]);
  });

  it("sync resolves with the committed offset after all earlier writes", async () => {
    const { core, replies, releaseAll } = make({ slowWriteMs: 10 });
    await core.handle({ type: "begin", totalBytes: 8 });
    void core.handle({ type: "write", index: 0, offset: 0, payload: buf(4) });
    void core.handle({ type: "write", index: 1, offset: 4, payload: buf(4) });
    const sync = core.handle({ type: "sync" });
    await tick();
    expect(replies.filter((r) => r.type === "synced")).toHaveLength(0);
    releaseAll();
    await tick();
    releaseAll();
    await sync;
    const synced = replies.find((r) => r.type === "synced") as Extract<
      SinkReply,
      { type: "synced" }
    >;
    expect(synced.committedOffset).toBe(8);
  });

  it("reports short_write when finalize arrives before all bytes", async () => {
    const { core, replies } = make();
    await core.handle({ type: "begin", totalBytes: 8 });
    await core.handle({ type: "write", index: 0, offset: 0, payload: buf(4) });
    await core.handle({ type: "finalize", expectedSha256: "" });
    expect(replies[replies.length - 1]).toMatchObject({ type: "error", code: "short_write" });
    expect(core.state).toBe("failed");
  });

  it("after an error, later steps are ignored", async () => {
    const { core, replies } = make();
    await core.handle({ type: "begin", totalBytes: 8 });
    await core.handle({ type: "write", index: 1, offset: 4, payload: buf(4) }); // gap -> sink_failure
    expect(replies[replies.length - 1]).toMatchObject({ type: "error", code: "sink_failure" });
    await core.handle({ type: "write", index: 0, offset: 0, payload: buf(4) });
    await core.handle({ type: "finalize", expectedSha256: "" });
    expect(replies.filter((r) => r.type === "committed" || r.type === "done")).toHaveLength(0);
  });

  it("abort discards the queued tail", async () => {
    const { core, replies, releaseAll } = make({ slowWriteMs: 10 });
    await core.handle({ type: "begin", totalBytes: 8 });
    void core.handle({ type: "write", index: 0, offset: 0, payload: buf(4) });
    void core.handle({ type: "write", index: 1, offset: 4, payload: buf(4) });
    await core.handle({ type: "abort" });
    expect(core.state).toBe("aborted");
    releaseAll();
    await tick();
    releaseAll();
    await tick();
    expect(replies.filter((r) => r.type === "committed")).toHaveLength(0);
    expect(replies[replies.length - 1]).toEqual({ type: "aborted" });
  });

  it("finalize with a mismatching expected hash reports ok=false (hasher plugged in Phase 5)", async () => {
    const { core, replies } = make();
    await core.handle({ type: "begin", totalBytes: 1 });
    await core.handle({ type: "write", index: 0, offset: 0, payload: buf(1) });
    await core.handle({ type: "finalize", expectedSha256: "abc" });
    expect(replies[replies.length - 1]).toMatchObject({ type: "done", ok: false });
  });
});
