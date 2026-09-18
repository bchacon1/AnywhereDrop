// DataChannelAdapter: the Channel implementation over an RTCDataChannel-like
// object. Pure adaptation; no protocol logic.

import type { Channel, DataChannelLike } from "./types";

export class DataChannelAdapter implements Channel {
  private messageCbs: ((data: string | ArrayBuffer) => void)[] = [];
  private lowCb: (() => void) | null = null;
  private closeCbs: (() => void)[] = [];
  private closedOnce = false;

  constructor(
    private readonly dc: DataChannelLike,
    readonly generation: number,
    private readonly maxMessageSizeFn: () => number,
  ) {
    dc.binaryType = "arraybuffer";
    dc.onmessage = (ev) => {
      const d = ev.data;
      if (typeof d === "string" || d instanceof ArrayBuffer)
        for (const cb of this.messageCbs) cb(d);
    };
    dc.onbufferedamountlow = () => this.lowCb?.();
    dc.onclose = () => this.fireClose();
    dc.onerror = () => this.fireClose();
  }

  send(data: string | ArrayBuffer): void {
    this.dc.send(data);
  }
  /** Multiple listeners are allowed (the transfer layer and a diagnostics panel may both listen). */
  onMessage(cb: (data: string | ArrayBuffer) => void): void {
    this.messageCbs.push(cb);
  }
  get bufferedAmount(): number {
    return this.dc.bufferedAmount;
  }
  get bufferedAmountLowThreshold(): number {
    return this.dc.bufferedAmountLowThreshold;
  }
  set bufferedAmountLowThreshold(v: number) {
    this.dc.bufferedAmountLowThreshold = v;
  }
  onBufferedAmountLow(cb: () => void): void {
    this.lowCb = cb;
  }
  get maxMessageSize(): number {
    return this.maxMessageSizeFn();
  }
  get readyState() {
    return this.dc.readyState;
  }
  onClose(cb: () => void): void {
    this.closeCbs.push(cb);
  }
  close(): void {
    this.dc.close();
  }
  private fireClose(): void {
    if (this.closedOnce) return;
    this.closedOnce = true;
    for (const cb of this.closeCbs) cb();
  }
}
