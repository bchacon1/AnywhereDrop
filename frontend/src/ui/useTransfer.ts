// React hook that owns a Sender (creator) or Receiver (joiner) for the
// current channel. Dev flags come from the URL so experiments 4d/4e and the
// Phase 5 corruption check run without code changes:
//   ?noLimit1=1 ?noLimit2=1 ?chunkSize=65536 ?slowWriteMs=50 ?corruptByte=1

import { useEffect, useMemo, useRef, useState } from "react";
import type { Role } from "../signaling/messages";
import { Receiver, type OfferInfo, type ReceiverStats } from "../transfer/receiver";
import { Sender, type SenderStats } from "../transfer/sender";
import { defaultLimits, type TransferLimits } from "../transfer/interfaces";
import type { Channel } from "../webrtc/types";
import { createSenderHash } from "../workers/hash-pass-client";
import { createWorkerSink } from "../workers/receive-sink-client";
import { fileSource } from "./fileSource";

export interface TransferView {
  role: Role;
  sender: SenderStats | null;
  receiver: ReceiverStats | null;
  offer: OfferInfo | null;
  result: { url: string; name: string; sha256: string } | null;
  log: string[];
  limits: TransferLimits;
  sendFile: (file: File) => void;
  accept: () => void;
  reject: () => void;
  cancel: () => void;
}

export function devLimits(search: string): TransferLimits {
  const p = new URLSearchParams(search);
  const l = defaultLimits();
  if (p.get("noLimit1")) l.disableLimit1 = true;
  if (p.get("noLimit2")) l.disableLimit2 = true;
  const cs = Number(p.get("chunkSize"));
  if (Number.isFinite(cs) && cs > 0) l.chunkSize = cs;
  return l;
}

export function devSlowWriteMs(search: string): number {
  const v = Number(new URLSearchParams(search).get("slowWriteMs") ?? "0");
  return Number.isFinite(v) && v > 0 ? v : 0;
}

export function devCorrupt(search: string): boolean {
  return new URLSearchParams(search).get("corruptByte") !== null;
}

export function useTransfer(role: Role, channel: Channel | null): TransferView {
  const limits = useMemo(() => devLimits(window.location.search), []);
  const slowWriteMs = useMemo(() => devSlowWriteMs(window.location.search), []);
  const corrupt = useMemo(() => devCorrupt(window.location.search), []);
  const fileRef = useRef<File | null>(null);
  const senderRef = useRef<Sender | null>(null);
  const receiverRef = useRef<Receiver | null>(null);
  const [sender, setSender] = useState<SenderStats | null>(null);
  const [receiver, setReceiver] = useState<ReceiverStats | null>(null);
  const [offer, setOffer] = useState<OfferInfo | null>(null);
  const [result, setResult] = useState<TransferView["result"]>(null);
  const [log, setLog] = useState<string[]>([]);

  useEffect(() => {
    senderRef.current = null;
    receiverRef.current = null;
    if (!channel) return;
    const append = (line: string) => setLog((l) => [...l.slice(-99), line]);
    if (role === "creator") {
      const s = new Sender({
        channel,
        limits,
        randomId: () => crypto.getRandomValues(new Uint32Array(1))[0]!,
        // Independent hash pass over the File the user picked (transfer-protocol.md §4.4).
        makeHash: () => createSenderHash(fileRef.current!),
      });
      s.on((ev) => (ev.type === "state" ? setSender(ev.stats) : append(ev.line)));
      senderRef.current = s;
      setSender(s.stats);
    } else {
      const r = new Receiver({
        channel,
        limits,
        makeSink: () => createWorkerSink({ slowWriteMs }),
        devCorruptFirstChunk: corrupt,
      });
      r.on((ev) => {
        switch (ev.type) {
          case "state":
            setReceiver(ev.stats);
            return;
          case "offer":
            setOffer(ev.offer);
            return;
          case "done":
            if (ev.ok && ev.result) {
              setResult({
                url: URL.createObjectURL(ev.result),
                name: r.stats.offer?.name ?? "file",
                sha256: ev.sha256,
              });
            }
            return;
          case "log":
            append(ev.line);
            return;
        }
      });
      receiverRef.current = r;
      setReceiver(r.stats);
    }
  }, [channel, role, limits, slowWriteMs, corrupt]);

  return {
    role,
    sender,
    receiver,
    offer,
    result,
    log,
    limits,
    sendFile: (file) => {
      fileRef.current = file;
      senderRef.current?.offer(fileSource(file));
    },
    accept: () => void receiverRef.current?.accept(),
    reject: () => receiverRef.current?.reject(),
    cancel: () => {
      senderRef.current?.cancel();
      receiverRef.current?.cancel();
    },
  };
}
