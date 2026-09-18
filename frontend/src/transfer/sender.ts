// Sender: offers a file, streams chunks under two limits (transfer-protocol.md
// §4.2), sends COMPLETE after the hash pass, and finishes on COMPLETE_ACK.
// Every await is bound to the channel generation (§5.3).

import {
  encodeChunk,
  encodeControl,
  decodeControl,
  HEADER_SIZE,
  PROTOCOL_VERSION,
  type ControlMessage,
  type ErrorCode,
} from "./messages";
import {
  defaultLimits,
  noHash,
  type Channel,
  type FileSource,
  type SenderHash,
  type SenderState,
  type TimingPoints,
  type TransferLimits,
} from "./interfaces";

export interface SenderStats {
  state: SenderState;
  transferId: number;
  size: number;
  nextOffset: number;
  ackedOffset: number;
  chunksSent: number;
  stallsLimit1: number;
  stallsLimit2: number;
  bufferedAmount: number;
  timing: TimingPoints;
  error: { code: ErrorCode; message: string } | null;
  sha256: string;
}

export type SenderEvent = { type: "state"; stats: SenderStats } | { type: "log"; line: string };

export interface SenderOptions {
  channel: Channel;
  limits?: Partial<TransferLimits>;
  makeHash?: (source: FileSource) => SenderHash;
  now?: () => number;
  randomId?: () => number;
  setTimer?: (fn: () => void, ms: number) => unknown;
  clearTimer?: (h: unknown) => void;
}

type Waiter = { kind: "low" | "ack" | "complete_ack"; resolve: (r: "ok" | "cancelled") => void };

export class Sender {
  private readonly channel: Channel;
  private readonly limits: TransferLimits;
  private readonly makeHash: (source: FileSource) => SenderHash;
  private readonly now: () => number;
  private readonly randomId: () => number;
  private readonly setTimer: (fn: () => void, ms: number) => unknown;
  private readonly clearTimer: (h: unknown) => void;

  private listeners = new Set<(ev: SenderEvent) => void>();
  private waiters: Waiter[] = [];
  private source: FileSource | null = null;
  private hash: SenderHash | null = null;
  private ackTimer: unknown = null;
  private channelGen: number;
  private terminal: { transferId: number; sha256: string } | null = null;

  stats: SenderStats = {
    state: "idle",
    transferId: 0,
    size: 0,
    nextOffset: 0,
    ackedOffset: 0,
    chunksSent: 0,
    stallsLimit1: 0,
    stallsLimit2: 0,
    bufferedAmount: 0,
    timing: {},
    error: null,
    sha256: "",
  };

  constructor(opts: SenderOptions) {
    this.channel = opts.channel;
    this.limits = { ...defaultLimits(), ...(opts.limits ?? {}) };
    this.makeHash = opts.makeHash ?? (() => noHash());
    this.now = opts.now ?? (() => performance.now());
    this.randomId = opts.randomId ?? (() => Math.floor(Math.random() * 0xffffffff));
    this.setTimer = opts.setTimer ?? ((fn, ms) => setTimeout(fn, ms));
    this.clearTimer = opts.clearTimer ?? ((h) => clearTimeout(h as ReturnType<typeof setTimeout>));
    this.channelGen = this.channel.generation;
    this.channel.onMessage((data) => this.onMessage(data));
    this.channel.onBufferedAmountLow(() => this.wake("low"));
    this.channel.onClose(() => this.onChannelClosed());
  }

  on(l: (ev: SenderEvent) => void): () => void {
    this.listeners.add(l);
    return () => this.listeners.delete(l);
  }

  /** Validate and send FILE_OFFER. Returns false (with stats.error set) if the file cannot be offered. */
  offer(source: FileSource): boolean {
    if (this.stats.state !== "idle") return false;
    const { chunkSize, maxFileSize } = this.limits;
    if (source.size > maxFileSize) {
      this.fail("too_large", `file is ${source.size} bytes; limit is ${maxFileSize}`);
      return false;
    }
    const mms = this.channel.maxMessageSize;
    if (mms > 0 && HEADER_SIZE + chunkSize > mms) {
      this.fail("too_large", `chunk ${HEADER_SIZE + chunkSize} exceeds maxMessageSize ${mms}`);
      return false;
    }
    this.source = source;
    this.stats.transferId = this.randomId() >>> 0;
    this.stats.size = source.size;
    const totalChunks = Math.max(1, Math.ceil(source.size / chunkSize));
    this.send({
      type: "FILE_OFFER",
      transferId: this.stats.transferId,
      name: source.name,
      size: source.size,
      mime: source.type,
      lastModified: source.lastModified,
      chunkSize,
      totalChunks,
      protocolVersion: PROTOCOL_VERSION,
    });
    this.setState("offered");
    return true;
  }

  cancel(reason = "user"): void {
    if (this.isTerminal()) return;
    this.send({ type: "CANCEL", transferId: this.stats.transferId, reason });
    this.hash?.cancel();
    this.setState("cancelled");
    this.wakeAll("cancelled");
  }

  // ---- incoming -----------------------------------------------------------

  private onMessage(data: string | ArrayBuffer): void {
    if (typeof data !== "string") return; // sender receives no binary
    const msg = decodeControl(data);
    if (!msg) return;
    if (
      "transferId" in msg &&
      msg.transferId !== undefined &&
      msg.transferId !== this.stats.transferId
    ) {
      if (this.terminal && msg.transferId === this.terminal.transferId) return; // late duplicate for a finished transfer
      return;
    }
    switch (msg.type) {
      case "FILE_ACCEPT":
        if (this.stats.state !== "offered") return;
        this.stats.timing.t_accept = this.now();
        this.setState("accepted");
        void this.run();
        return;
      case "FILE_REJECT":
        if (this.stats.state !== "offered") return;
        this.log(`rejected: ${msg.reason}`);
        this.setState("rejected");
        return;
      case "ACK":
        if (
          msg.committedOffset > this.stats.ackedOffset &&
          msg.committedOffset <= this.stats.nextOffset
        ) {
          this.stats.ackedOffset = msg.committedOffset;
          this.wake("ack");
          this.emitState();
        }
        return;
      case "COMPLETE_ACK":
        if (this.stats.state !== "completing" && this.stats.state !== "interrupted") return;
        this.clearAckTimer();
        this.stats.timing.t_ack_received = this.now();
        this.terminal = { transferId: this.stats.transferId, sha256: this.stats.sha256 };
        if (msg.ok) this.setState("done");
        else this.fail("hash_mismatch", "receiver reported a mismatch");
        this.wakeAll("cancelled");
        return;
      case "CANCEL":
        if (this.isTerminal()) return;
        this.log(`peer cancelled: ${msg.reason}`);
        this.hash?.cancel();
        this.setState("cancelled");
        this.wakeAll("cancelled");
        return;
      case "ERROR":
        if (this.isTerminal()) return;
        this.fail(msg.code, `peer error: ${msg.message}`);
        this.wakeAll("cancelled");
        return;
      default:
        return;
    }
  }

  // ---- the loop (transfer-protocol.md §4.2) ---------------------------------

  private async run(): Promise<void> {
    const source = this.source!;
    const { chunkSize, highWater, lowWater, window, disableLimit1, disableLimit2 } = this.limits;
    const gen = this.channelGen;
    this.channel.bufferedAmountLowThreshold = lowWater;
    this.hash = this.makeHash(source);
    this.hash.start();
    this.setState("transferring");

    while (this.stats.nextOffset < source.size) {
      if (this.stats.state !== "transferring" || gen !== this.channelGen) return;

      // Limit 1: local SCTP send buffer.
      if (!disableLimit1 && this.channel.bufferedAmount > highWater) {
        this.stats.stallsLimit1 += 1;
        if ((await this.waitFor("low", gen)) === "cancelled") return;
        continue;
      }
      // Limit 2: receiver processing window (un-ACKed bytes).
      if (!disableLimit2 && this.stats.nextOffset - this.stats.ackedOffset >= window) {
        this.stats.stallsLimit2 += 1;
        if ((await this.waitFor("ack", gen)) === "cancelled") return;
        continue;
      }

      const len = Math.min(chunkSize, source.size - this.stats.nextOffset);
      let buf: ArrayBuffer;
      try {
        buf = await source.read(this.stats.nextOffset, len);
      } catch (e) {
        this.fail("read_failure", (e as Error).message);
        return;
      }
      if (this.stats.state !== "transferring" || gen !== this.channelGen) return;
      const index = this.stats.nextOffset / chunkSize;
      try {
        this.channel.send(encodeChunk(this.stats.transferId, index, buf));
      } catch (e) {
        this.fail("send_failure", (e as Error).message);
        return;
      }
      this.stats.nextOffset += len;
      this.stats.chunksSent += 1;
      this.stats.bufferedAmount = this.channel.bufferedAmount;
      if (this.stats.chunksSent % 16 === 0) this.emitState();
    }
    this.stats.timing.t_enqueued = this.now();
    this.emitState();

    let sha256: string;
    try {
      sha256 = await this.hash.result;
    } catch (e) {
      this.fail("read_failure", `hash pass failed: ${(e as Error).message}`);
      return;
    }
    if (this.stats.state !== "transferring" || gen !== this.channelGen) return;
    this.stats.sha256 = sha256;
    this.send({ type: "COMPLETE", transferId: this.stats.transferId, sha256 });
    this.stats.timing.t_complete_sent = this.now();
    this.setState("completing");
    this.armAckTimer();
  }

  // ---- waiters bound to channel generation (§5.3) ----------------------------

  private waitFor(kind: Waiter["kind"], gen: number): Promise<"ok" | "cancelled"> {
    return new Promise((resolve) => {
      this.waiters.push({
        kind,
        resolve: (r) => resolve(gen === this.channelGen ? r : "cancelled"),
      });
    });
  }

  private wake(kind: Waiter["kind"]): void {
    const keep: Waiter[] = [];
    for (const w of this.waiters) {
      if (w.kind === kind) w.resolve("ok");
      else keep.push(w);
    }
    this.waiters = keep;
  }

  private wakeAll(r: "ok" | "cancelled"): void {
    const ws = this.waiters;
    this.waiters = [];
    for (const w of ws) w.resolve(r);
  }

  private onChannelClosed(): void {
    this.clearAckTimer();
    this.wakeAll("cancelled");
    if (this.isTerminal() || this.stats.state === "idle") return;
    // Phase 4: a lost channel ends the transfer. Phase 7 turns this into `interrupted` + RESUME.
    this.fail("channel_lost", "data channel closed during transfer");
  }

  private armAckTimer(): void {
    this.clearAckTimer();
    this.ackTimer = this.setTimer(() => {
      this.ackTimer = null;
      if (this.stats.state === "completing")
        this.fail("internal", "no COMPLETE_ACK within timeout");
    }, this.limits.completeAckTimeoutMs);
  }

  private clearAckTimer(): void {
    if (this.ackTimer !== null) {
      this.clearTimer(this.ackTimer);
      this.ackTimer = null;
    }
  }

  // ---- plumbing --------------------------------------------------------------

  private send(msg: ControlMessage): void {
    try {
      this.channel.send(encodeControl(msg));
    } catch (e) {
      this.log(`control send failed: ${(e as Error).message}`);
    }
  }

  private fail(code: ErrorCode, message: string): void {
    if (this.isTerminal()) return;
    this.hash?.cancel();
    this.clearAckTimer();
    this.stats.error = { code, message };
    if (this.stats.state !== "idle" && code !== "channel_lost") {
      this.send({ type: "ERROR", transferId: this.stats.transferId, code, message });
    }
    this.setState("failed");
  }

  private isTerminal(): boolean {
    const s = this.stats.state;
    return s === "done" || s === "failed" || s === "cancelled" || s === "rejected";
  }

  private setState(state: SenderState): void {
    this.stats.state = state;
    this.log(`state=${state}`);
    this.emitState();
  }

  private emitState(): void {
    this.stats.bufferedAmount = this.channel.bufferedAmount;
    const snapshot: SenderStats = { ...this.stats, timing: { ...this.stats.timing } };
    for (const l of this.listeners) l({ type: "state", stats: snapshot });
  }

  private log(line: string): void {
    for (const l of this.listeners) l({ type: "log", line });
  }
}
