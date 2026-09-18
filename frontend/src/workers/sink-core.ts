// SinkCore: the receive sink's logic, independent of the Worker API so it
// runs under Vitest in Node. Effects are serialized on one promise chain
// (transfer-protocol.md §4.5): a step starts only after the previous one
// settled, so `sync` and `finalize` run after every earlier write completed.

export type SinkRequest =
  | { type: "begin"; totalBytes: number; slowWriteMs?: number }
  | { type: "write"; index: number; offset: number; payload: ArrayBuffer }
  | { type: "sync" }
  | { type: "finalize"; expectedSha256: string }
  | { type: "abort" };

export type SinkReply =
  | { type: "ready" }
  | { type: "committed"; index: number; committedOffset: number }
  | { type: "synced"; committedOffset: number }
  | { type: "done"; ok: boolean; sha256: string; result?: Blob }
  | { type: "error"; code: "sink_failure" | "short_write"; message: string }
  | { type: "aborted" };

export type SinkState = "idle" | "active" | "finalized" | "failed" | "aborted";

/** Incremental hasher; Phase 5 supplies SHA-256, Phase 4 uses noopHasher. */
export interface Hasher {
  update(chunk: ArrayBuffer): void;
  digestHex(): Promise<string>;
}

export const noopHasher = (): Hasher => ({
  update() {},
  digestHex: async () => "",
});

export interface SinkCoreOptions {
  post: (reply: SinkReply) => void;
  makeHasher?: () => Hasher;
  /** Dev flag (experiment 4e): delay each write to show the receiver window at work. */
  slowWriteMs?: number;
  sleep?: (ms: number) => Promise<void>;
}

export class SinkCore {
  state: SinkState = "idle";
  private chain: Promise<void> = Promise.resolve();
  private chunks: ArrayBuffer[] = [];
  private committedOffset = 0;
  private totalBytes = 0;
  private hasher: Hasher;
  private readonly post: (reply: SinkReply) => void;
  private readonly makeHasher: () => Hasher;
  private slowWriteMs: number;
  private readonly sleep: (ms: number) => Promise<void>;

  constructor(opts: SinkCoreOptions) {
    this.post = opts.post;
    this.makeHasher = opts.makeHasher ?? noopHasher;
    this.hasher = this.makeHasher();
    this.slowWriteMs = opts.slowWriteMs ?? 0;
    this.sleep = opts.sleep ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
  }

  /** Enqueue a request. Returns a promise that settles when this step has been processed. */
  handle(req: SinkRequest): Promise<void> {
    if (req.type === "abort") {
      // Abort jumps the queue: later steps see the aborted state and skip.
      this.state = "aborted";
      this.chunks = [];
      this.post({ type: "aborted" });
      return Promise.resolve();
    }
    this.chain = this.chain
      .then(() => this.step(req))
      .catch((e: Error) => this.failWith("sink_failure", e.message));
    return this.chain;
  }

  private async step(req: SinkRequest): Promise<void> {
    if (this.state === "aborted" || this.state === "failed") return;
    switch (req.type) {
      case "begin":
        this.totalBytes = req.totalBytes;
        if (req.slowWriteMs !== undefined) this.slowWriteMs = req.slowWriteMs;
        this.chunks = [];
        this.committedOffset = 0;
        this.hasher = this.makeHasher();
        this.state = "active";
        this.post({ type: "ready" });
        return;
      case "write": {
        if (this.state !== "active") return;
        if (req.offset !== this.committedOffset) {
          this.failWith("sink_failure", `write at ${req.offset}, expected ${this.committedOffset}`);
          return;
        }
        if (this.slowWriteMs > 0) await this.sleep(this.slowWriteMs);
        this.chunks.push(req.payload);
        this.hasher.update(req.payload);
        this.committedOffset += req.payload.byteLength;
        this.post({ type: "committed", index: req.index, committedOffset: this.committedOffset });
        return;
      }
      case "sync":
        this.post({ type: "synced", committedOffset: this.committedOffset });
        return;
      case "finalize": {
        if (this.state !== "active") return;
        if (this.committedOffset !== this.totalBytes) {
          this.failWith("short_write", `committed ${this.committedOffset} of ${this.totalBytes}`);
          return;
        }
        const sha256 = await this.hasher.digestHex();
        const ok = req.expectedSha256 === "" ? true : sha256 === req.expectedSha256;
        const result = new Blob(this.chunks);
        this.chunks = [];
        this.state = "finalized";
        this.post({ type: "done", ok, sha256, result });
        return;
      }
      default:
        return;
    }
  }

  private failWith(code: "sink_failure" | "short_write", message: string): void {
    if (this.state === "failed" || this.state === "aborted") return;
    this.state = "failed";
    this.chunks = [];
    this.post({ type: "error", code, message });
  }
}
