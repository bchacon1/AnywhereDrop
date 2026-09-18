// React hook that owns one PeerLink per attachment. Creates the link when
// signaling attaches, tears it down on leave. Exposes the open Channel and
// link state for the screens.

import { useEffect, useRef, useState } from "react";
import type { SignalingClient } from "../signaling/client";
import { PeerLink, type LinkState } from "../webrtc/link";
import { browserPeerConnectionFactory, type Channel } from "../webrtc/types";
import type { SignalingState } from "./useSignaling";

export interface PeerLinkView {
  state: LinkState | null;
  channel: Channel | null;
  log: string[];
  restartIce: () => void;
}

export function usePeerLink(client: SignalingClient, sig: SignalingState): PeerLinkView {
  const linkRef = useRef<PeerLink | null>(null);
  const [state, setState] = useState<LinkState | null>(null);
  const [channel, setChannel] = useState<Channel | null>(null);
  const [log, setLog] = useState<string[]>([]);

  const attachment = sig.attachment;
  const roomState = sig.roomState;
  const paired = roomState?.state === "paired";

  useEffect(() => {
    if (!attachment) {
      linkRef.current?.close();
      linkRef.current = null;
      setState(null);
      setChannel(null);
      return;
    }
    if (linkRef.current) return;
    const link = new PeerLink({
      role: attachment.role,
      signaling: client,
      pcFactory: browserPeerConnectionFactory,
    });
    linkRef.current = link;
    link.on((ev) => {
      switch (ev.type) {
        case "state":
          setState(ev.state);
          return;
        case "channel":
          setChannel(ev.channel);
          ev.channel.onClose(() => setChannel((c) => (c === ev.channel ? null : c)));
          return;
        case "log":
          setLog((l) => [...l.slice(-99), ev.line]);
          return;
      }
    });
    link.start(paired);
    return undefined;
    // `paired` is intentionally not a dependency: it is read once when the link is
    // created; later pairing arrives as a peer_joined event through the client.
  }, [attachment, client]);

  return { state, channel, log, restartIce: () => linkRef.current?.restartIce() };
}
