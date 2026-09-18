// Wire types for the /ws signaling envelope. Mirrors backend/internal/signaling/messages.go.
// system-design.md §2, §6; transfer-protocol.md §2.

export const ENVELOPE_VERSION = 1;

export type Role = "creator" | "joiner";

export interface PeerState {
  peerId: string;
  role: Role;
  connected: boolean;
  gen: number;
}

export interface RoomState {
  state: "waiting" | "paired" | "closed";
  peers: PeerState[];
}

/** Every message in both directions. Unused fields are absent. */
export interface Envelope {
  v: number;
  type: string;
  code?: string;
  peerId?: string;
  peerToken?: string;
  gen?: number;
  role?: Role;
  roomState?: RoomState;
  from?: string;
  payload?: unknown;
  reason?: string;
  message?: string;
}

export const ClientType = {
  createRoom: "create_room",
  joinRoom: "join_room",
  reattach: "reattach",
  relay: "relay",
  ping: "ping",
  syncRequest: "sync_request",
} as const;

export const ServerType = {
  roomCreated: "room_created",
  joined: "joined",
  reattached: "reattached",
  peerJoined: "peer_joined",
  peerDisconnected: "peer_disconnected",
  peerReattached: "peer_reattached",
  roomClosed: "room_closed",
  relay: "relay",
  pong: "pong",
  roomState: "room_state",
  error: "error",
} as const;

/** WebSocket close codes the server uses (application range). */
export const CloseCode = {
  superseded: 4001,
  roomClosed: 4002,
  protocol: 4003,
} as const;
