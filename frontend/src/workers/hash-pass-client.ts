// Main-thread SenderHash over the hash-pass worker.

import type { SenderHash } from "../transfer/interfaces";
import type { HashPassReply, HashPassRequest } from "./hash-pass.worker";

export interface HashPassResult {
  sha256: string;
  bytes: number;
  ms: number;
}

/** Hash a Blob in a worker; resolves with digest and elapsed time. */
export function hashBlob(
  file: Blob,
  onProgress?: (bytes: number) => void,
): { result: Promise<HashPassResult>; cancel: () => void } {
  const worker = new Worker(new URL("./hash-pass.worker.ts", import.meta.url), { type: "module" });
  let done = false;
  const result = new Promise<HashPassResult>((resolve, reject) => {
    worker.onmessage = (ev: MessageEvent<HashPassReply>) => {
      const r = ev.data;
      if (r.type === "progress") onProgress?.(r.bytes);
      else if (r.type === "result") {
        done = true;
        resolve({ sha256: r.sha256, bytes: r.bytes, ms: r.ms });
        worker.terminate();
      } else {
        done = true;
        reject(new Error(r.message));
        worker.terminate();
      }
    };
    worker.onerror = (ev) => {
      done = true;
      reject(new Error(ev.message));
      worker.terminate();
    };
    worker.postMessage({ type: "hash", file } satisfies HashPassRequest);
  });
  return {
    result,
    cancel: () => {
      if (!done) {
        done = true;
        worker.terminate();
      }
    },
  };
}

/** SenderHash for a File: starts on demand, cancellable. */
export function createSenderHash(file: File): SenderHash {
  let handle: ReturnType<typeof hashBlob> | null = null;
  let resolveResult: (s: string) => void = () => {};
  let rejectResult: (e: Error) => void = () => {};
  const result = new Promise<string>((res, rej) => {
    resolveResult = res;
    rejectResult = rej;
  });
  return {
    start() {
      if (handle) return;
      handle = hashBlob(file);
      handle.result.then((r) => resolveResult(r.sha256), rejectResult);
    },
    result,
    cancel() {
      handle?.cancel();
      rejectResult(new Error("cancelled"));
    },
  };
}
