import type { Channel } from "../../src/webrtc/types";

/**
 * An in-memory Channel pair. `a.send` queues for `b` (and vice versa); the
 * test delivers with `flush()`, so interleaving is controlled. bufferedAmount
 * is simulated: each binary send adds to it; `drain(n)` reduces it and fires
 * bufferedamountlow when crossing the threshold, as the spec describes.
 */
export class FakeChannel implements Channel {
  peer: FakeChannel | null = null;
  queue: (string | ArrayBuffer)[] = [];
  sent: (string | ArrayBuffer)[] = [];
  bufferedAmount = 0;
  bufferedAmountLowThreshold = 0;
  maxMessageSize = 262144;
  readonly generation: number;
  readyState: Channel["readyState"] = "open";
  throwOnSend: Error | null = null;
  corruptNext = false;
  private msgCb: ((d: string | ArrayBuffer) => void) | null = null;
  private lowCb: (() => void) | null = null;
  private closeCbs: (() => void)[] = [];

  constructor(generation = 1) {
    this.generation = generation;
  }

  static pair(): [FakeChannel, FakeChannel] {
    const a = new FakeChannel();
    const b = new FakeChannel();
    a.peer = b;
    b.peer = a;
    return [a, b];
  }

  send(data: string | ArrayBuffer): void {
    if (this.readyState !== "open") throw new Error("InvalidStateError: channel not open");
    if (this.throwOnSend) throw this.throwOnSend;
    this.sent.push(data);
    if (typeof data !== "string") this.bufferedAmount += data.byteLength;
    if (this.corruptNext && typeof data !== "string") {
      this.corruptNext = false;
      const copy = data.slice(0);
      new Uint8Array(copy)[20]! ^= 0xff;
      data = copy;
    }
    this.peer?.queue.push(data);
  }
  onMessage(cb: (d: string | ArrayBuffer) => void): void {
    this.msgCb = cb;
  }
  onBufferedAmountLow(cb: () => void): void {
    this.lowCb = cb;
  }
  onClose(cb: () => void): void {
    this.closeCbs.push(cb);
  }
  close(): void {
    if (this.readyState === "closed") return;
    this.readyState = "closed";
    for (const cb of this.closeCbs) cb();
    this.peer?.close();
  }

  /** Deliver queued messages to this side's listener. */
  flush(max = Infinity): number {
    let n = 0;
    while (this.queue.length && n < max) {
      const d = this.queue.shift()!;
      this.msgCb?.(d);
      n++;
    }
    return n;
  }
  /** Simulate the SCTP stack sending bytes: reduce bufferedAmount, fire the low event when crossing. */
  drain(bytes: number): void {
    const before = this.bufferedAmount;
    this.bufferedAmount = Math.max(0, this.bufferedAmount - bytes);
    if (
      before > this.bufferedAmountLowThreshold &&
      this.bufferedAmount <= this.bufferedAmountLowThreshold
    )
      this.lowCb?.();
  }
  controlSent(): { type: string }[] {
    return this.sent
      .filter((m): m is string => typeof m === "string")
      .map((m) => JSON.parse(m) as { type: string });
  }
  binarySent(): ArrayBuffer[] {
    return this.sent.filter((m): m is ArrayBuffer => typeof m !== "string");
  }
}

export const tick = () => new Promise<void>((r) => setTimeout(r, 0));
export async function settle(rounds = 5) {
  for (let i = 0; i < rounds; i++) await tick();
}
