import React from "react";
import { createRoot } from "react-dom/client";
import { App } from "./ui/App";
import "./ui/styles.css";
import { hashBlob } from "./workers/hash-pass-client";

// Dev/experiment hook (experiment 5.1): hash `bytes` of pseudo-random data in
// the hash-pass worker and report MB/s, so hasher throughput can be compared
// with transfer throughput in the same browser.
(
  window as unknown as {
    __benchSha256: (bytes: number) => Promise<{ ms: number; mbps: number; sha256: string }>;
  }
).__benchSha256 = async (bytes: number) => {
  const chunk = new Uint8Array(1 << 20);
  for (let i = 0; i < chunk.length; i++) chunk[i] = (i * 7 + 3) & 0xff;
  const parts: Uint8Array[] = [];
  for (let n = 0; n < bytes; n += chunk.length)
    parts.push(chunk.subarray(0, Math.min(chunk.length, bytes - n)));
  const blob = new Blob(parts as BlobPart[]);
  const r = await hashBlob(blob).result;
  return { ms: r.ms, mbps: r.bytes / (r.ms / 1000) / 1e6, sha256: r.sha256 };
};

const root = document.getElementById("root");
if (!root) throw new Error("missing #root");
createRoot(root).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
