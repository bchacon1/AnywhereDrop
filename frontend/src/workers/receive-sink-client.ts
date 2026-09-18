// Main-thread ReceiveSink over the worker. Transfers each payload buffer
// (zero-copy, detaching it) and resolves sync/finalize from worker replies.

import type { ReceiveSink, SinkErrorCode } from "../transfer/interfaces";
import type { SinkReply, SinkRequest } from "./sink-core";

export interface WorkerSinkOptions {
  slowWriteMs?: number;
}

export function createWorkerSink(opts: WorkerSinkOptions = {}): ReceiveSink {
  // Vite bundles the worker only when it sees this exact literal form.
  const worker = new Worker(new URL("./receive-sink.worker.ts", import.meta.url), {
    type: "module",
  });

  let committedCb: ((index: number, committedOffset: number) => void) | null = null;
  let errorCb: ((code: SinkErrorCode, message: string) => void) | null = null;
  let readyResolve: (() => void) | null = null;
  let syncResolve: ((r: { committedOffset: number }) => void) | null = null;
  let finalizeResolve: ((r: { ok: boolean; sha256: string; result?: Blob }) => void) | null = null;
  let finalizeReject: ((e: Error) => void) | null = null;
  let terminated = false;

  const post = (req: SinkRequest, transfer?: Transferable[]) => {
    if (terminated) return;
    if (transfer) worker.postMessage(req, transfer);
    else worker.postMessage(req);
  };

  worker.onmessage = (ev: MessageEvent<SinkReply>) => {
    const r = ev.data;
    switch (r.type) {
      case "ready":
        readyResolve?.();
        readyResolve = null;
        return;
      case "committed":
        committedCb?.(r.index, r.committedOffset);
        return;
      case "synced":
        syncResolve?.({ committedOffset: r.committedOffset });
        syncResolve = null;
        return;
      case "done":
        finalizeResolve?.({ ok: r.ok, sha256: r.sha256, result: r.result });
        finalizeResolve = finalizeReject = null;
        terminate();
        return;
      case "error":
        errorCb?.(r.code, r.message);
        finalizeReject?.(new Error(r.message));
        finalizeResolve = finalizeReject = null;
        terminate();
        return;
      case "aborted":
        terminate();
        return;
    }
  };
  worker.onerror = (ev) => {
    errorCb?.("sink_failure", ev.message);
    finalizeReject?.(new Error(ev.message));
    terminate();
  };

  const terminate = () => {
    if (terminated) return;
    terminated = true;
    worker.terminate();
  };

  return {
    begin(totalBytes) {
      return new Promise<void>((resolve) => {
        readyResolve = resolve;
        post({ type: "begin", totalBytes, slowWriteMs: opts.slowWriteMs });
      });
    },
    write(index, offset, payload) {
      post({ type: "write", index, offset, payload }, [payload]);
    },
    onCommitted(cb) {
      committedCb = cb;
    },
    sync() {
      return new Promise((resolve) => {
        syncResolve = resolve;
        post({ type: "sync" });
      });
    },
    finalize(expectedSha256) {
      return new Promise((resolve, reject) => {
        finalizeResolve = resolve;
        finalizeReject = reject;
        post({ type: "finalize", expectedSha256 });
      });
    },
    onError(cb) {
      errorCb = cb;
    },
    abort() {
      post({ type: "abort" });
      terminate();
    },
  };
}
