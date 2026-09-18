// The four interfaces transfer/ is written against (system-design.md §5) and
// the provisional constants (architecture-decisions.md Part B). Type-only
// references to Blob/File are allowed; no browser API is called here.

import type { Channel } from "../webrtc/types";
export type { Channel };

export interface FileSource {
  readonly size: number;
  readonly name: string;
  readonly type: string;
  readonly lastModified: number;
  read(offset: number, length: number): Promise<ArrayBuffer>;
}

export type SinkErrorCode = "sink_failure" | "short_write";

export interface ReceiveSink {
  begin(totalBytes: number): Promise<void>;
  /** Ownership of payload transfers to the sink. Effects are serialized in order. */
  write(index: number, offset: number, payload: ArrayBuffer): void;
  onCommitted(cb: (index: number, committedOffset: number) => void): void;
  /** Barrier: resolves after every earlier write has committed. */
  sync(): Promise<{ committedOffset: number }>;
  finalize(expectedSha256: string): Promise<{ ok: boolean; sha256: string; result?: Blob }>;
  onError(cb: (code: SinkErrorCode, message: string) => void): void;
  abort(): void;
}

/** Sender-side independent hash pass (Phase 5). Phase 4 uses noHash(). */
export interface SenderHash {
  start(): void;
  readonly result: Promise<string>;
  cancel(): void;
}

export const noHash = (): SenderHash => ({
  start() {},
  result: Promise.resolve(""),
  cancel() {},
});

/** Provisional defaults. Each cites the experiment that owns it. */
export const DEFAULTS = {
  chunkSize: 16 * 1024, // RFC 8831 §6.6; experiment 8.2
  highWater: 1024 * 1024, // experiment 8.3
  lowWater: 256 * 1024, // experiment 8.3
  window: 8 * 1024 * 1024, // experiment 8.3
  ackInterval: 1024 * 1024, // experiment 8.3
  maxFileSize: 200 * 1024 * 1024, // unvalidated policy; experiment 8.7
  completeAckTimeoutMs: 60_000, // Phase 4 provisional
} as const;

export interface TransferLimits {
  chunkSize: number;
  highWater: number;
  lowWater: number;
  window: number;
  ackInterval: number;
  maxFileSize: number;
  completeAckTimeoutMs: number;
  /** Dev flags for experiments 4d/4e: disable a limit to observe what happens. */
  disableLimit1?: boolean;
  disableLimit2?: boolean;
}

export const defaultLimits = (): TransferLimits => ({ ...DEFAULTS });

/** Timing points (transfer-protocol.md §4.7), milliseconds from an arbitrary origin. */
export interface TimingPoints {
  t_accept?: number;
  t_enqueued?: number;
  t_complete_sent?: number;
  t_committed_all?: number;
  t_verified?: number;
  t_visible?: number;
  t_ack_received?: number;
}

export type SenderState =
  | "idle"
  | "offered"
  | "accepted"
  | "transferring"
  | "completing"
  | "done"
  | "rejected"
  | "cancelled"
  | "failed"
  | "interrupted";

export type ReceiverState =
  | "idle"
  | "offered"
  | "accepted"
  | "transferring"
  | "completing"
  | "done"
  | "rejected"
  | "cancelled"
  | "failed"
  | "interrupted";
