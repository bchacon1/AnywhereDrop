// Sender-side independent hash pass (transfer-protocol.md §4.4): reads the
// File sequentially in large slices and hashes it, decoupled from what the
// sender has enqueued. Also serves the in-page benchmark hook.

import { Sha256 } from "./sha256";

export type HashPassRequest = { type: "hash"; file: Blob; sliceBytes?: number };
export type HashPassReply =
  | { type: "progress"; bytes: number }
  | { type: "result"; sha256: string; bytes: number; ms: number }
  | { type: "error"; message: string };

const post = (m: HashPassReply) =>
  (self as unknown as { postMessage: (m: HashPassReply) => void }).postMessage(m);

self.onmessage = async (ev: MessageEvent<HashPassRequest>) => {
  const { file } = ev.data;
  const slice = ev.data.sliceBytes ?? 4 * 1024 * 1024;
  const t0 = performance.now();
  try {
    const h = new Sha256();
    let off = 0;
    while (off < file.size) {
      const end = Math.min(off + slice, file.size);
      h.update(await file.slice(off, end).arrayBuffer());
      off = end;
      post({ type: "progress", bytes: off });
    }
    post({ type: "result", sha256: h.digestHex(), bytes: file.size, ms: performance.now() - t0 });
  } catch (e) {
    post({ type: "error", message: (e as Error).message });
  }
};
