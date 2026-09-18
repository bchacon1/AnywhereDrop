// Receiver: validates FILE_OFFER, accepts frames through the six validation
// steps (transfer-protocol.md §4.5), hands payloads to the ReceiveSink, ACKs
// committed progress, and completes only from a terminal record (§5.2).

import {
  chunkPayload,
  decodeChunkHeader,
  decodeControl,
  encodeControl,
  expectedPayloadLength,
  HEADER_SIZE,
  PROTOCOL_VERSION,
  type ControlMessage,
  type ErrorCode,
} from "./messages";
import {
  defaultLimits,
  type Channel,
  type ReceiveSink,
  type ReceiverState,
  type TimingPoints,
  type TransferLimits,
} from "./interfaces";

export interface OfferInfo {
  transferId: number;
  name: string;
  size: number;
  mime: string;
  lastModified: number;
  chunkSize: number;
  totalChunks: number;
}

export interface ReceiverStats {
  state: ReceiverState;
  offer: OfferInfo | null;
  expectedIndex: number;
  committedOffset: number;
  duplicates: number;
  acksSent: number;
  timing: TimingPoints;
  error: { code: ErrorCode; message: string } | null;
  verified: boolean | null; // null until finalize; false when no hash was requested
  sha256: string;
}

export type ReceiverEvent =
  | { type: "state"; stats: ReceiverStats }
  | { type: "offer"; offer: OfferInfo }
  | { type: "done"; ok: boolean; result: Blob | undefined; sha256: string }
  | { type: "log"; line: string };

export interface ReceiverOptions {
  channel: Channel;
  makeSink: () => ReceiveSink;
  limits?: Partial<TransferLimits>;
  now?: () => number;
  /** Dev flag (Phase 5 acceptance): flip one byte of the first chunk before it is committed, to prove the hash check fires. */
  devCorruptFirstChunk?: boolean;
}

interface TerminalRecord {
  transferId: number;
  ok: boolean;
  sha256: string;
  committedOffset: number;
}

export class Receiver {
  private readonly channel: Channel;
  private readonly makeSink: () => ReceiveSink;
  private readonly limits: TransferLimits;
  private readonly now: () => number;
  private readonly devCorruptFirstChunk: boolean;
  private listeners = new Set<(ev: ReceiverEvent) => void>();
  private sink: ReceiveSink | null = null;
  private finalizePending = false;
  private nextAckAt = 0;
  private terminal: TerminalRecord | null = null;

  stats: ReceiverStats = {
    state: "idle",
    offer: null,
    expectedIndex: 0,
    committedOffset: 0,
    duplicates: 0,
    acksSent: 0,
    timing: {},
    error: null,
    verified: null,
    sha256: "",
  };

  constructor(opts: ReceiverOptions) {
    this.channel = opts.channel;
    this.makeSink = opts.makeSink;
    this.limits = { ...defaultLimits(), ...(opts.limits ?? {}) };
    this.now = opts.now ?? (() => performance.now());
    this.devCorruptFirstChunk = opts.devCorruptFirstChunk ?? false;
    this.channel.onMessage((data) => this.onMessage(data));
    this.channel.onClose(() => this.onChannelClosed());
  }

  on(l: (ev: ReceiverEvent) => void): () => void {
    this.listeners.add(l);
    return () => this.listeners.delete(l);
  }

  /** User consent. Allocates the sink and replies FILE_ACCEPT. */
  async accept(): Promise<void> {
    const offer = this.stats.offer;
    if (this.stats.state !== "offered" || !offer) return;
    const sink = this.makeSink();
    this.sink = sink;
    sink.onCommitted((_index, committedOffset) => this.onCommitted(committedOffset));
    sink.onError((code, message) => this.fail(code, message));
    try {
      await sink.begin(offer.size);
    } catch (e) {
      this.fail("sink_failure", (e as Error).message);
      return;
    }
    if (this.stats.state !== "offered") return;
    this.nextAckAt = this.limits.ackInterval;
    this.stats.timing.t_accept = this.now();
    this.send({ type: "FILE_ACCEPT", transferId: offer.transferId });
    this.setState("transferring");
  }

  reject(reason = "declined"): void {
    const offer = this.stats.offer;
    if (this.stats.state !== "offered" || !offer) return;
    this.send({ type: "FILE_REJECT", transferId: offer.transferId, reason });
    this.setState("rejected");
  }

  cancel(reason = "user"): void {
    if (this.isTerminal() || this.stats.state === "idle") return;
    const id = this.stats.offer?.transferId ?? 0;
    this.send({ type: "CANCEL", transferId: id, reason });
    this.sink?.abort();
    this.setState("cancelled");
  }

  // ---- incoming ----------------------------------------------------------

  private onMessage(data: string | ArrayBuffer): void {
    if (typeof data === "string") {
      const msg = decodeControl(data);
      if (msg) this.onControl(msg);
    } else {
      this.onFrame(data);
    }
  }

  private onControl(msg: ControlMessage): void {
    switch (msg.type) {
      case "FILE_OFFER":
        this.onOffer(msg);
        return;
      case "COMPLETE":
        this.onComplete(msg);
        return;
      case "CANCEL":
        if (this.isTerminal() || this.stats.state === "idle") return;
        this.log(`peer cancelled: ${msg.reason}`);
        this.sink?.abort();
        this.setState("cancelled");
        return;
      case "ERROR":
        if (this.isTerminal() || this.stats.state === "idle") return;
        this.sink?.abort();
        this.stats.error = { code: msg.code, message: `peer error: ${msg.message}` };
        this.setState("failed");
        return;
      default:
        return;
    }
  }

  private onOffer(msg: Extract<ControlMessage, { type: "FILE_OFFER" }>): void {
    if (this.stats.state !== "idle" && !this.isTerminal()) return;
    if (msg.protocolVersion !== PROTOCOL_VERSION) {
      this.send({
        type: "ERROR",
        transferId: msg.transferId,
        code: "unsupported_version",
        message: `version ${msg.protocolVersion}`,
      });
      return;
    }
    if (msg.size > this.limits.maxFileSize) {
      this.send({
        type: "ERROR",
        transferId: msg.transferId,
        code: "too_large",
        message: `size ${msg.size} exceeds ${this.limits.maxFileSize}`,
      });
      return;
    }
    const mms = this.channel.maxMessageSize;
    if (mms > 0 && HEADER_SIZE + msg.chunkSize > mms) {
      this.send({
        type: "ERROR",
        transferId: msg.transferId,
        code: "too_large",
        message: `chunk ${HEADER_SIZE + msg.chunkSize} exceeds maxMessageSize ${mms}`,
      });
      return;
    }
    const expectedTotal = Math.max(1, Math.ceil(msg.size / msg.chunkSize));
    if (msg.chunkSize <= 0 || msg.totalChunks !== expectedTotal || msg.size < 0) {
      this.send({
        type: "ERROR",
        transferId: msg.transferId,
        code: "bad_frame",
        message: "inconsistent offer",
      });
      return;
    }
    // Fresh transfer: reset per-transfer state (a previous terminal record is kept).
    this.stats = {
      ...this.stats,
      state: "idle",
      expectedIndex: 0,
      committedOffset: 0,
      duplicates: 0,
      acksSent: 0,
      timing: {},
      error: null,
      verified: null,
      sha256: "",
    };
    this.finalizePending = false;
    this.stats.offer = {
      transferId: msg.transferId,
      name: msg.name,
      size: msg.size,
      mime: msg.mime,
      lastModified: msg.lastModified,
      chunkSize: msg.chunkSize,
      totalChunks: msg.totalChunks,
    };
    this.setState("offered");
    for (const l of this.listeners) l({ type: "offer", offer: this.stats.offer });
  }

  /** Validation steps 1–6 of transfer-protocol.md §4.5. */
  private onFrame(buf: ArrayBuffer): void {
    const offer = this.stats.offer;
    const h = decodeChunkHeader(buf);
    if (!h || h.version !== PROTOCOL_VERSION || h.flags !== 0 || h.reserved !== 0) {
      this.protocolError("bad_frame", "bad header");
      return;
    }
    if (!offer || h.transferId !== offer.transferId || this.stats.state !== "transferring") {
      if (this.terminal && h.transferId === this.terminal.transferId) {
        this.stats.duplicates += 1; // chunk for a finished transfer
        return;
      }
      if (this.stats.state !== "transferring") return; // not accepting frames right now
      this.protocolError("unknown_transfer", `transferId ${h.transferId}`);
      return;
    }
    if (h.chunkIndex >= offer.totalChunks) {
      this.protocolError("bad_frame", `chunkIndex ${h.chunkIndex} >= ${offer.totalChunks}`);
      return;
    }
    const want = expectedPayloadLength(
      offer.size,
      offer.chunkSize,
      h.chunkIndex,
      offer.totalChunks,
    );
    if (buf.byteLength - HEADER_SIZE !== want) {
      this.protocolError("bad_frame", `payload ${buf.byteLength - HEADER_SIZE} != ${want}`);
      return;
    }
    if (h.chunkIndex < this.stats.expectedIndex) {
      this.stats.duplicates += 1;
      return;
    }
    if (h.chunkIndex > this.stats.expectedIndex) {
      this.protocolError(
        "out_of_sequence",
        `got ${h.chunkIndex}, expected ${this.stats.expectedIndex}`,
      );
      return;
    }
    this.stats.expectedIndex += 1;
    const payload = chunkPayload(buf);
    if (this.devCorruptFirstChunk && h.chunkIndex === 0 && payload.byteLength > 0) {
      new Uint8Array(payload)[0]! ^= 0xff;
      this.log("dev: corrupted one byte of chunk 0");
    }
    this.sink!.write(h.chunkIndex, h.chunkIndex * offer.chunkSize, payload);
  }

  private onCommitted(committedOffset: number): void {
    this.stats.committedOffset = committedOffset;
    const offer = this.stats.offer;
    if (offer && committedOffset >= offer.size) this.stats.timing.t_committed_all = this.now();
    if (committedOffset >= this.nextAckAt || (offer && committedOffset >= offer.size)) {
      this.sendAck();
      while (this.nextAckAt <= committedOffset) this.nextAckAt += this.limits.ackInterval;
    }
    if (this.stats.expectedIndex % 16 === 0 || (offer && committedOffset >= offer.size))
      this.emitState();
  }

  private sendAck(): void {
    const offer = this.stats.offer;
    if (!offer || this.channel.readyState !== "open") return;
    this.send({
      type: "ACK",
      transferId: offer.transferId,
      committedOffset: this.stats.committedOffset,
    });
    this.stats.acksSent += 1;
  }

  private onComplete(msg: Extract<ControlMessage, { type: "COMPLETE" }>): void {
    if (this.terminal && msg.transferId === this.terminal.transferId) {
      this.sendCompleteAck(this.terminal); // idempotent resend from the record (§5.2)
      return;
    }
    const offer = this.stats.offer;
    if (!offer || msg.transferId !== offer.transferId || this.stats.state !== "transferring")
      return;
    if (this.finalizePending) return;
    this.finalizePending = true;
    this.setState("completing");
    const sink = this.sink!;
    void sink.finalize(msg.sha256).then(
      (res) => {
        if (this.stats.state !== "completing") return;
        this.stats.timing.t_verified = this.now();
        this.stats.sha256 = res.sha256;
        this.stats.verified = msg.sha256 === "" ? false : res.ok;
        this.terminal = {
          transferId: offer.transferId,
          ok: res.ok,
          sha256: res.sha256,
          committedOffset: this.stats.committedOffset,
        };
        this.sink = null;
        this.sendCompleteAck(this.terminal);
        if (res.ok) {
          this.setState("done");
          this.stats.timing.t_visible = this.now();
        } else {
          this.stats.error = {
            code: "hash_mismatch",
            message: `expected ${msg.sha256}, got ${res.sha256}`,
          };
          this.setState("failed");
        }
        for (const l of this.listeners)
          l({
            type: "done",
            ok: res.ok,
            result: res.ok ? res.result : undefined,
            sha256: res.sha256,
          });
      },
      (e: Error) => this.fail("sink_failure", e.message),
    );
  }

  private sendCompleteAck(t: TerminalRecord): void {
    this.send({
      type: "COMPLETE_ACK",
      transferId: t.transferId,
      ok: t.ok,
      committedOffset: t.committedOffset,
      sha256: t.sha256,
    });
  }

  private onChannelClosed(): void {
    if (this.isTerminal() || this.stats.state === "idle" || this.stats.state === "offered") return;
    if (this.stats.state === "completing") return; // finalize continues; Phase 7 sends the ACK on the next channel
    // Phase 4: a lost channel ends the transfer. Phase 7 turns this into `interrupted` + RESUME.
    this.sink?.abort();
    this.stats.error = { code: "channel_lost", message: "data channel closed during transfer" };
    this.setState("failed");
  }

  // ---- plumbing ----------------------------------------------------------

  private protocolError(code: ErrorCode, message: string): void {
    this.send({ type: "ERROR", transferId: this.stats.offer?.transferId, code, message });
    this.sink?.abort();
    this.stats.error = { code, message };
    this.setState("failed");
  }

  private fail(code: ErrorCode, message: string): void {
    if (this.isTerminal()) return;
    this.send({ type: "ERROR", transferId: this.stats.offer?.transferId, code, message });
    this.sink?.abort();
    this.stats.error = { code, message };
    this.setState("failed");
  }

  private send(msg: ControlMessage): void {
    try {
      this.channel.send(encodeControl(msg));
    } catch (e) {
      this.log(`control send failed: ${(e as Error).message}`);
    }
  }

  private isTerminal(): boolean {
    const s = this.stats.state;
    return s === "done" || s === "failed" || s === "cancelled" || s === "rejected";
  }

  private setState(state: ReceiverState): void {
    this.stats.state = state;
    this.log(`state=${state}`);
    this.emitState();
  }

  private emitState(): void {
    const snapshot: ReceiverStats = { ...this.stats, timing: { ...this.stats.timing } };
    for (const l of this.listeners) l({ type: "state", stats: snapshot });
  }

  private log(line: string): void {
    for (const l of this.listeners) l({ type: "log", line });
  }
}
