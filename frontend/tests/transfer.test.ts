import { describe, expect, it } from "vitest";
import { Receiver } from "../src/transfer/receiver";
import { Sender } from "../src/transfer/sender";
import { encodeChunk, encodeControl } from "../src/transfer/messages";
import type { ReceiveSink } from "../src/transfer/interfaces";
import { SinkCore, type SinkReply } from "../src/workers/sink-core";
import { sha256Hasher, sha256Hex } from "../src/workers/sha256";
import { FakeChannel, settle } from "./helpers/fake-channel";
import { memorySource } from "./helpers/memory-source";

/** A ReceiveSink over SinkCore in-process, optionally slow. */
function coreSink(
  opts: { slowWriteMs?: number; manual?: boolean } = {},
): ReceiveSink & { core: SinkCore; releaseAll: () => void } {
  let committed: ((i: number, o: number) => void) | null = null;
  let error: ((c: "sink_failure" | "short_write", m: string) => void) | null = null;
  let syncRes: ((r: { committedOffset: number }) => void) | null = null;
  let finRes: ((r: { ok: boolean; sha256: string; result?: Blob }) => void) | null = null;
  let finRej: ((e: Error) => void) | null = null;
  let release: (() => void)[] = [];
  const core = new SinkCore({
    post: (r: SinkReply) => {
      switch (r.type) {
        case "committed":
          committed?.(r.index, r.committedOffset);
          break;
        case "synced":
          syncRes?.({ committedOffset: r.committedOffset });
          break;
        case "done":
          finRes?.({ ok: r.ok, sha256: r.sha256, result: r.result });
          break;
        case "error":
          error?.(r.code, r.message);
          finRej?.(new Error(r.message));
          break;
      }
    },
    slowWriteMs: opts.slowWriteMs,
    sleep: opts.manual ? () => new Promise<void>((res) => release.push(res)) : undefined,
    makeHasher: sha256Hasher,
  });
  return {
    core,
    releaseAll: () => {
      const rs = release;
      release = [];
      rs.forEach((r) => r());
    },
    begin: (n) => core.handle({ type: "begin", totalBytes: n }),
    write: (i, o, p) => void core.handle({ type: "write", index: i, offset: o, payload: p }),
    onCommitted: (cb) => (committed = cb),
    sync: () =>
      new Promise((res) => {
        syncRes = res;
        void core.handle({ type: "sync" });
      }),
    finalize: (sha) =>
      new Promise((res, rej) => {
        finRes = res;
        finRej = rej;
        void core.handle({ type: "finalize", expectedSha256: sha });
      }),
    onError: (cb) => (error = cb),
    abort: () => void core.handle({ type: "abort" }),
  };
}

const limits = {
  chunkSize: 1024,
  highWater: 8 * 1024,
  lowWater: 2 * 1024,
  window: 16 * 1024,
  ackInterval: 4 * 1024,
};

/** Exchange: flush both sides a few rounds without draining bufferedAmount. */
async function exchange(a: FakeChannel, b: FakeChannel, rounds = 4) {
  for (let i = 0; i < rounds; i++) {
    a.flush();
    b.flush();
    await settle(4);
  }
}

/** Pump: flush both sides and drain the sender's buffer until nothing moves. */
async function pump(a: FakeChannel, b: FakeChannel, drainPerRound = Infinity, rounds = 200) {
  for (let i = 0; i < rounds; i++) {
    const moved = a.flush() + b.flush();
    a.drain(drainPerRound);
    await settle(3);
    if (moved === 0 && a.queue.length === 0 && b.queue.length === 0) {
      await settle(3);
      if (a.queue.length === 0 && b.queue.length === 0) break;
    }
  }
}

describe("Sender + Receiver over a fake channel (Phase 4)", () => {
  it("transfers a file byte-exactly, completes only after done, and reports timing points", async () => {
    const [a, b] = FakeChannel.pair();
    const sink = coreSink();
    const sender = new Sender({ channel: a, limits, now: () => 1 });
    const receiver = new Receiver({ channel: b, makeSink: () => sink, limits });
    let result: Blob | undefined;
    receiver.on((ev) => {
      if (ev.type === "offer") void receiver.accept();
      if (ev.type === "done") result = ev.result;
    });
    const src = memorySource(10_000);
    expect(sender.offer(src)).toBe(true);
    await pump(a, b);
    expect(sender.stats.state).toBe("done");
    expect(receiver.stats.state).toBe("done");
    expect(receiver.stats.committedOffset).toBe(10_000);
    expect(receiver.stats.expectedIndex).toBe(10);
    expect(new Uint8Array(await result!.arrayBuffer())).toEqual(src.bytes);
    // COMPLETE_ACK was sent after done, once.
    const types = b.controlSent().map((m) => m.type);
    expect(types.filter((t) => t === "COMPLETE_ACK")).toHaveLength(1);
    expect(types.indexOf("COMPLETE_ACK")).toBeGreaterThan(types.indexOf("ACK"));
    expect(sender.stats.timing.t_enqueued).toBeDefined();
    expect(sender.stats.timing.t_ack_received).toBeDefined();
    expect(receiver.stats.timing.t_verified).toBeDefined();
    expect(receiver.stats.verified).toBe(false); // no hash requested in Phase 4
  });

  it("limit 1: pauses on bufferedAmount above HIGH_WATER and resumes on bufferedamountlow", async () => {
    const [a, b] = FakeChannel.pair();
    const sink = coreSink();
    const sender = new Sender({ channel: a, limits: { ...limits, window: 1 << 30 } });
    const receiver = new Receiver({ channel: b, makeSink: () => sink, limits });
    receiver.on((ev) => {
      if (ev.type === "offer") void receiver.accept();
    });
    sender.offer(memorySource(64 * 1024));
    // Deliver the accept, but never drain: the sender must stop at HIGH_WATER.
    await exchange(a, b);
    expect(a.bufferedAmount).toBeGreaterThan(limits.highWater);
    expect(a.bufferedAmount).toBeLessThanOrEqual(limits.highWater + limits.chunkSize);
    expect(sender.stats.stallsLimit1).toBe(1);
    expect(a.bufferedAmountLowThreshold).toBe(limits.lowWater);
    // Drain below LOW_WATER: the low event fires and sending resumes.
    a.drain(limits.highWater);
    await settle(10);
    expect(sender.stats.nextOffset).toBeGreaterThan(9 * 1024);
    await pump(a, b);
    expect(sender.stats.state).toBe("done");
  });

  it("limit 2: pauses at WINDOW un-ACKed bytes until an ACK arrives; backlog stays bounded", async () => {
    const [a, b] = FakeChannel.pair();
    const sink = coreSink({ slowWriteMs: 1, manual: true });
    const sender = new Sender({ channel: a, limits: { ...limits, disableLimit1: true } });
    const receiver = new Receiver({ channel: b, makeSink: () => sink, limits });
    receiver.on((ev) => {
      if (ev.type === "offer") void receiver.accept();
    });
    sender.offer(memorySource(64 * 1024));
    await exchange(a, b);
    // Frames are delivered but the sink is stuck: no ACKs, so the sender stops at WINDOW.
    b.flush();
    await settle(10);
    expect(sender.stats.nextOffset).toBe(limits.window);
    expect(sender.stats.stallsLimit2).toBe(1);
    // Release the sink: commits -> ACKs -> the sender continues.
    for (let i = 0; i < 80; i++) {
      sink.releaseAll();
      await settle(2);
      a.flush();
      b.flush();
    }
    expect(sender.stats.nextOffset).toBeGreaterThan(limits.window);
    for (let i = 0; i < 200 && sender.stats.state !== "done"; i++) {
      sink.releaseAll();
      await settle(2);
      a.flush();
      b.flush();
    }
    expect(sender.stats.state).toBe("done");
  });

  it("rejects an offer over maxFileSize and a chunk that would exceed maxMessageSize", () => {
    const [a] = FakeChannel.pair();
    const sender = new Sender({ channel: a, limits: { ...limits, maxFileSize: 100 } });
    expect(sender.offer(memorySource(101))).toBe(false);
    expect(sender.stats.error?.code).toBe("too_large");
    const [c] = FakeChannel.pair();
    c.maxMessageSize = 1000;
    const s2 = new Sender({ channel: c, limits });
    expect(s2.offer(memorySource(10))).toBe(false);
    expect(s2.stats.error?.message).toMatch(/maxMessageSize/);
  });

  it("receiver rejects an oversize or inconsistent offer with ERROR", async () => {
    const [a, b] = FakeChannel.pair();
    const receiver = new Receiver({
      channel: b,
      makeSink: () => coreSink(),
      limits: { ...limits, maxFileSize: 100 },
    });
    a.send(
      encodeControl({
        type: "FILE_OFFER",
        transferId: 1,
        name: "x",
        size: 101,
        mime: "",
        lastModified: 0,
        chunkSize: 1024,
        totalChunks: 1,
        protocolVersion: 1,
      }),
    );
    b.flush();
    expect(b.controlSent()[0]).toMatchObject({ type: "ERROR", code: "too_large" });
    a.send(
      encodeControl({
        type: "FILE_OFFER",
        transferId: 2,
        name: "x",
        size: 50,
        mime: "",
        lastModified: 0,
        chunkSize: 1024,
        totalChunks: 5,
        protocolVersion: 1,
      }),
    );
    b.flush();
    expect(b.controlSent()[1]).toMatchObject({ type: "ERROR", code: "bad_frame" });
    a.send(
      encodeControl({
        type: "FILE_OFFER",
        transferId: 3,
        name: "x",
        size: 50,
        mime: "",
        lastModified: 0,
        chunkSize: 1024,
        totalChunks: 1,
        protocolVersion: 2,
      }),
    );
    b.flush();
    expect(b.controlSent()[2]).toMatchObject({ type: "ERROR", code: "unsupported_version" });
    expect(receiver.stats.state).toBe("idle");
  });

  it("frame validation: bad header, wrong length, out of sequence, duplicate", async () => {
    async function setup() {
      const [a, b] = FakeChannel.pair();
      const receiver = new Receiver({ channel: b, makeSink: () => coreSink(), limits });
      receiver.on((ev) => {
        if (ev.type === "offer") void receiver.accept();
      });
      a.send(
        encodeControl({
          type: "FILE_OFFER",
          transferId: 9,
          name: "x",
          size: 3000,
          mime: "",
          lastModified: 0,
          chunkSize: 1024,
          totalChunks: 3,
          protocolVersion: 1,
        }),
      );
      b.flush();
      await settle(3);
      expect(receiver.stats.state).toBe("transferring");
      return { a, b, receiver };
    }
    // bad version byte
    {
      const { a, b, receiver } = await setup();
      const f = encodeChunk(9, 0, new ArrayBuffer(1024));
      new Uint8Array(f)[0] = 2;
      a.send(f);
      b.flush();
      expect(receiver.stats.error?.code).toBe("bad_frame");
      expect(b.controlSent().at(-1)).toMatchObject({ type: "ERROR", code: "bad_frame" });
    }
    // wrong payload length
    {
      const { a, b, receiver } = await setup();
      a.send(encodeChunk(9, 0, new ArrayBuffer(1000)));
      b.flush();
      expect(receiver.stats.error?.code).toBe("bad_frame");
    }
    // last chunk must be exactly size - index*chunkSize
    {
      const { a, b, receiver } = await setup();
      a.send(encodeChunk(9, 0, new ArrayBuffer(1024)));
      a.send(encodeChunk(9, 1, new ArrayBuffer(1024)));
      a.send(encodeChunk(9, 2, new ArrayBuffer(1024))); // should be 952
      b.flush();
      expect(receiver.stats.error?.code).toBe("bad_frame");
    }
    // out of sequence
    {
      const { a, b, receiver } = await setup();
      a.send(encodeChunk(9, 1, new ArrayBuffer(1024)));
      b.flush();
      expect(receiver.stats.error?.code).toBe("out_of_sequence");
    }
    // duplicate dropped and counted
    {
      const { a, b, receiver } = await setup();
      a.send(encodeChunk(9, 0, new ArrayBuffer(1024)));
      a.send(encodeChunk(9, 0, new ArrayBuffer(1024)));
      b.flush();
      await settle(3);
      expect(receiver.stats.duplicates).toBe(1);
      expect(receiver.stats.expectedIndex).toBe(1);
      expect(receiver.stats.state).toBe("transferring");
    }
    // index beyond totalChunks
    {
      const { a, b, receiver } = await setup();
      a.send(encodeChunk(9, 3, new ArrayBuffer(1024)));
      b.flush();
      expect(receiver.stats.error?.code).toBe("bad_frame");
    }
  });

  it("a throw from send() fails the transfer with send_failure", async () => {
    const [a, b] = FakeChannel.pair();
    const sender = new Sender({ channel: a, limits });
    const receiver = new Receiver({ channel: b, makeSink: () => coreSink(), limits });
    receiver.on((ev) => {
      if (ev.type === "offer") void receiver.accept();
    });
    sender.offer(memorySource(4096)); // FILE_OFFER goes out fine
    a.throwOnSend = new Error("OperationError: not enough buffer space"); // the first chunk will throw
    await exchange(a, b);
    expect(sender.stats.state).toBe("failed");
    expect(sender.stats.error?.code).toBe("send_failure");
  });

  it("cancel from the sender while waiting on each limit, and from the receiver mid-transfer", async () => {
    // waiting on limit 1
    {
      const [a, b] = FakeChannel.pair();
      const sender = new Sender({ channel: a, limits: { ...limits, window: 1 << 30 } });
      const receiver = new Receiver({ channel: b, makeSink: () => coreSink(), limits });
      receiver.on((ev) => {
        if (ev.type === "offer") void receiver.accept();
      });
      sender.offer(memorySource(64 * 1024));
      await exchange(a, b);
      expect(sender.stats.stallsLimit1).toBe(1);
      sender.cancel("test");
      b.flush();
      await settle(5);
      expect(sender.stats.state).toBe("cancelled");
      expect(receiver.stats.state).toBe("cancelled");
      expect(a.controlSent().at(-1)).toMatchObject({ type: "CANCEL" });
    }
    // waiting on limit 2
    {
      const [a, b] = FakeChannel.pair();
      const sink = coreSink({ slowWriteMs: 1, manual: true });
      const sender = new Sender({ channel: a, limits: { ...limits, disableLimit1: true } });
      const receiver = new Receiver({ channel: b, makeSink: () => sink, limits });
      receiver.on((ev) => {
        if (ev.type === "offer") void receiver.accept();
      });
      sender.offer(memorySource(64 * 1024));
      await exchange(a, b);
      expect(sender.stats.stallsLimit2).toBe(1);
      sender.cancel();
      b.flush();
      await settle(5);
      expect(sender.stats.state).toBe("cancelled");
      expect(receiver.stats.state).toBe("cancelled");
      expect(sink.core.state).toBe("aborted");
    }
    // receiver cancels with a worker backlog
    {
      const [a, b] = FakeChannel.pair();
      const sink = coreSink({ slowWriteMs: 1, manual: true });
      const sender = new Sender({ channel: a, limits });
      const receiver = new Receiver({ channel: b, makeSink: () => sink, limits });
      receiver.on((ev) => {
        if (ev.type === "offer") void receiver.accept();
      });
      sender.offer(memorySource(8 * 1024));
      await exchange(a, b);
      receiver.cancel();
      a.flush();
      await settle(5);
      expect(receiver.stats.state).toBe("cancelled");
      expect(sender.stats.state).toBe("cancelled");
      expect(sink.core.state).toBe("aborted");
    }
  });

  it("channel loss during transfer fails both sides cleanly (Phase 4; Phase 7 makes this interrupted)", async () => {
    const [a, b] = FakeChannel.pair();
    const sink = coreSink({ slowWriteMs: 1, manual: true });
    const sender = new Sender({ channel: a, limits: { ...limits, disableLimit1: true } });
    const receiver = new Receiver({ channel: b, makeSink: () => sink, limits });
    receiver.on((ev) => {
      if (ev.type === "offer") void receiver.accept();
    });
    sender.offer(memorySource(64 * 1024));
    await exchange(a, b);
    b.flush();
    await settle(5);
    a.close();
    await settle(5);
    expect(sender.stats.state).toBe("failed");
    expect(sender.stats.error?.code).toBe("channel_lost");
    expect(receiver.stats.state).toBe("failed");
    expect(sink.core.state).toBe("aborted");
  });

  it("COMPLETE_ACK is never sent before finalization and a repeated COMPLETE is answered from the record", async () => {
    const [a, b] = FakeChannel.pair();
    const sink = coreSink({ slowWriteMs: 1, manual: true });
    const sender = new Sender({ channel: a, limits });
    const receiver = new Receiver({ channel: b, makeSink: () => sink, limits });
    receiver.on((ev) => {
      if (ev.type === "offer") void receiver.accept();
    });
    sender.offer(memorySource(2048));
    await exchange(a, b);
    b.flush();
    await settle(5); // frames + COMPLETE delivered; writes stuck
    expect(receiver.stats.state).toBe("completing");
    expect(b.controlSent().some((m) => m.type === "COMPLETE_ACK")).toBe(false);
    for (let i = 0; i < 10; i++) {
      sink.releaseAll();
      await settle(3);
    }
    expect(receiver.stats.state).toBe("done");
    expect(b.controlSent().filter((m) => m.type === "COMPLETE_ACK")).toHaveLength(1);
    // A duplicate COMPLETE after done is answered again without re-finalizing.
    a.send(encodeControl({ type: "COMPLETE", transferId: sender.stats.transferId, sha256: "" }));
    b.flush();
    await settle(3);
    expect(b.controlSent().filter((m) => m.type === "COMPLETE_ACK")).toHaveLength(2);
    expect(sink.core.state).toBe("finalized");
  });

  // ---- Phase 5: integrity -------------------------------------------------

  it("verifies the SHA-256 end to end and reports verified=true", async () => {
    const [a, b] = FakeChannel.pair();
    const src = memorySource(10_000);
    const sender = new Sender({
      channel: a,
      limits,
      makeHash: () => ({ start() {}, result: Promise.resolve(sha256Hex(src.bytes)), cancel() {} }),
    });
    const receiver = new Receiver({ channel: b, makeSink: () => coreSink(), limits });
    receiver.on((ev) => {
      if (ev.type === "offer") void receiver.accept();
    });
    sender.offer(src);
    await pump(a, b);
    expect(sender.stats.state).toBe("done");
    expect(receiver.stats.state).toBe("done");
    expect(receiver.stats.verified).toBe(true);
    expect(receiver.stats.sha256).toBe(sha256Hex(src.bytes));
    expect(sender.stats.sha256).toBe(receiver.stats.sha256);
  });

  it("a corrupted frame produces hash_mismatch on both sides and no result", async () => {
    const [a, b] = FakeChannel.pair();
    const src = memorySource(10_000);
    const sender = new Sender({
      channel: a,
      limits,
      makeHash: () => ({ start() {}, result: Promise.resolve(sha256Hex(src.bytes)), cancel() {} }),
    });
    const receiver = new Receiver({ channel: b, makeSink: () => coreSink(), limits });
    let doneEvent: { ok: boolean; result: Blob | undefined } | null = null;
    receiver.on((ev) => {
      if (ev.type === "offer") void receiver.accept();
      if (ev.type === "done") doneEvent = { ok: ev.ok, result: ev.result };
    });
    sender.offer(src);
    a.corruptNext = true; // the first binary frame is flipped in transit
    await pump(a, b);
    expect(receiver.stats.state).toBe("failed");
    expect(receiver.stats.error?.code).toBe("hash_mismatch");
    expect(receiver.stats.verified).toBe(false);
    expect(doneEvent).toMatchObject({ ok: false, result: undefined });
    expect(sender.stats.state).toBe("failed");
    expect(sender.stats.error?.code).toBe("hash_mismatch");
    const ack = b.controlSent().find((m) => m.type === "COMPLETE_ACK") as
      { ok: boolean } | undefined;
    expect(ack?.ok).toBe(false);
  });

  it("devCorruptFirstChunk flips a byte on receive and the check fires", async () => {
    const [a, b] = FakeChannel.pair();
    const src = memorySource(3_000);
    const sender = new Sender({
      channel: a,
      limits,
      makeHash: () => ({ start() {}, result: Promise.resolve(sha256Hex(src.bytes)), cancel() {} }),
    });
    const receiver = new Receiver({
      channel: b,
      makeSink: () => coreSink(),
      limits,
      devCorruptFirstChunk: true,
    });
    receiver.on((ev) => {
      if (ev.type === "offer") void receiver.accept();
    });
    sender.offer(src);
    await pump(a, b);
    expect(receiver.stats.error?.code).toBe("hash_mismatch");
  });
});
