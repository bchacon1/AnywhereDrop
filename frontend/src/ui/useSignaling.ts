// React hook that owns one SignalingClient for the page and mirrors its
// events into state the screens can render. The client itself is
// framework-free (src/signaling/client.ts); this file is the only bridge.

import { useEffect, useMemo, useRef, useState } from "react";
import {
  SignalingClient,
  type Attachment,
  type ConnectionStatus,
  type SignalingEvent,
} from "../signaling/client";
import type { RoomState } from "../signaling/messages";

export interface PeerInfo {
  peerId: string;
  connected: boolean;
}

export interface SignalingState {
  status: ConnectionStatus;
  attachment: Attachment | null;
  roomState: RoomState | null;
  peer: PeerInfo | null;
  closedReason: string | null;
  lastError: { code: string; message: string } | null;
  log: string[];
}

const initial: SignalingState = {
  status: "idle",
  attachment: null,
  roomState: null,
  peer: null,
  closedReason: null,
  lastError: null,
  log: [],
};

export function wsUrl(): string {
  const proto = window.location.protocol === "https:" ? "wss" : "ws";
  return `${proto}://${window.location.host}/ws`;
}

/** Derive the partner from a room snapshot and our own id. */
function partnerFrom(roomState: RoomState | null, selfId: string | undefined): PeerInfo | null {
  if (!roomState || !selfId) return null;
  const other = roomState.peers.find((p) => p.peerId !== selfId);
  return other ? { peerId: other.peerId, connected: other.connected } : null;
}

export function useSignaling(url: string = wsUrl()) {
  const clientRef = useRef<SignalingClient | null>(null);
  if (clientRef.current === null) clientRef.current = new SignalingClient({ url });
  const client = clientRef.current;

  const [state, setState] = useState<SignalingState>(initial);

  useEffect(() => {
    const append = (s: SignalingState, line: string) => ({
      ...s,
      log: [...s.log.slice(-99), line],
    });
    const off = client.on((ev: SignalingEvent) => {
      setState((s) => {
        switch (ev.type) {
          case "status":
            return append({ ...s, status: ev.status }, `status: ${ev.status}`);
          case "attached":
            return append(
              {
                ...s,
                attachment: ev.attachment,
                roomState: ev.roomState,
                peer: partnerFrom(ev.roomState, ev.attachment.peerId),
                lastError: null,
              },
              `${ev.reattached ? "reattached" : "attached"} as ${ev.attachment.role} gen=${ev.attachment.gen}`,
            );
          case "peer_joined":
            return append({ ...s, peer: { peerId: ev.peerId, connected: true } }, "peer joined");
          case "peer_disconnected":
            return append(
              { ...s, peer: s.peer ? { ...s.peer, connected: false } : null },
              "peer disconnected (grace period running)",
            );
          case "peer_reattached":
            return append(
              { ...s, peer: { peerId: ev.peerId, connected: true } },
              `peer reattached gen=${ev.gen}`,
            );
          case "room_state":
            return append(
              {
                ...s,
                roomState: ev.roomState,
                peer: partnerFrom(ev.roomState, s.attachment?.peerId),
              },
              `room_state: ${ev.roomState.state}`,
            );
          case "room_closed":
            return append({ ...s, closedReason: ev.reason }, `room closed: ${ev.reason}`);
          case "relay":
            return append(s, `relay from ${ev.from.slice(0, 6)}: ${JSON.stringify(ev.payload)}`);
          case "resync":
            return append(s, "resync (reattached; negotiation may need to restart)");
          case "error":
            return append(
              { ...s, lastError: { code: ev.code, message: ev.message } },
              `error ${ev.code}: ${ev.message}`,
            );
        }
      });
    });
    return () => {
      off();
    };
  }, [client]);

  useEffect(() => () => client.close(), [client]);

  const actions = useMemo(
    () => ({
      createRoom: () => client.createRoom(),
      joinRoom: (code: string) => client.joinRoom(code),
      relay: (payload: unknown) => client.relay(payload),
      syncRequest: () => client.syncRequest(),
      dropSocket: () => client.dropSocket(),
      leave: () => {
        client.close();
        setState(initial);
        clientRef.current = new SignalingClient({ url });
      },
    }),
    [client, url],
  );

  return { state, actions, client };
}
