// SignalingClient: one WebSocket session attached to one room slot.
// Responsibilities (system-design.md §5): create/join/reattach, ping,
// relay, and reconnect-with-reattach. It knows nothing about WebRTC or files.

import {
  ClientType,
  CloseCode,
  ENVELOPE_VERSION,
  ServerType,
  type Envelope,
  type Role,
  type RoomState,
} from "./messages";

export type ConnectionStatus = "idle" | "connecting" | "attached" | "reconnecting" | "closed";

/** Identity handed out by the server; peerToken never leaves this object except in `reattach`. */
export interface Attachment {
  code: string;
  peerId: string;
  peerToken: string;
  gen: number;
  role: Role;
}

export type SignalingEvent =
  | { type: "status"; status: ConnectionStatus }
  | { type: "attached"; attachment: Attachment; roomState: RoomState; reattached: boolean }
  | { type: "peer_joined"; peerId: string }
  | { type: "peer_disconnected"; peerId: string }
  | { type: "peer_reattached"; peerId: string; gen: number }
  | { type: "room_state"; roomState: RoomState }
  | { type: "room_closed"; reason: string }
  | { type: "relay"; from: string; payload: unknown }
  | { type: "resync" } // signaling_resync from the server (Phase 9) or a reattach: negotiation may need to restart
  | { type: "error"; code: string; message: string; fatal: boolean };

export type Listener = (event: SignalingEvent) => void;

/** The subset of WebSocket this client uses; tests supply a fake. */
export interface SocketLike {
  readonly readyState: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  onopen: ((ev: unknown) => void) | null;
  onmessage: ((ev: { data: unknown }) => void) | null;
  onclose: ((ev: { code: number; reason: string }) => void) | null;
  onerror: ((ev: unknown) => void) | null;
}

export interface ClientOptions {
  url: string;
  /** Factory so tests can inject a fake socket. */
  createSocket?: (url: string) => SocketLike;
  /** Provisional 20 s (architecture-decisions.md Part B). */
  pingIntervalMs?: number;
  /** Reattach backoff schedule, capped at the last value. */
  backoffMs?: number[];
  setTimer?: (fn: () => void, ms: number) => unknown;
  clearTimer?: (handle: unknown) => void;
}

const SOCKET_OPEN = 1;

export class SignalingClient {
  private readonly url: string;
  private readonly createSocket: (url: string) => SocketLike;
  private readonly pingIntervalMs: number;
  private readonly backoffMs: number[];
  private readonly setTimer: (fn: () => void, ms: number) => unknown;
  private readonly clearTimer: (handle: unknown) => void;

  private socket: SocketLike | null = null;
  private listeners = new Set<Listener>();
  private pendingFirst: Envelope | null = null;
  private pingHandle: unknown = null;
  private reconnectHandle: unknown = null;
  private reconnectAttempt = 0;
  private stopped = false;

  status: ConnectionStatus = "idle";
  attachment: Attachment | null = null;
  roomState: RoomState | null = null;

  constructor(opts: ClientOptions) {
    this.url = opts.url;
    this.createSocket = opts.createSocket ?? ((u) => new WebSocket(u) as unknown as SocketLike);
    this.pingIntervalMs = opts.pingIntervalMs ?? 20_000;
    this.backoffMs = opts.backoffMs ?? [1000, 2000, 4000, 8000];
    this.setTimer = opts.setTimer ?? ((fn, ms) => setTimeout(fn, ms));
    this.clearTimer = opts.clearTimer ?? ((h) => clearTimeout(h as ReturnType<typeof setTimeout>));
  }

  on(listener: Listener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  /** Open a socket and create a room. */
  createRoom(): void {
    this.open({ v: ENVELOPE_VERSION, type: ClientType.createRoom });
  }

  /** Open a socket and join an existing room. */
  joinRoom(code: string): void {
    this.open({ v: ENVELOPE_VERSION, type: ClientType.joinRoom, code: code.trim().toUpperCase() });
  }

  /** Send an opaque payload to the other peer. Silently dropped if not attached. */
  relay(payload: unknown): boolean {
    return this.sendEnvelope({ v: ENVELOPE_VERSION, type: ClientType.relay, payload });
  }

  /** Ask the server for the room snapshot and, if paired, a re-announce of peer_joined. */
  syncRequest(): boolean {
    return this.sendEnvelope({ v: ENVELOPE_VERSION, type: ClientType.syncRequest });
  }

  /** Dev hook: drop the socket without leaving the room (exercises reattach). */
  dropSocket(): void {
    this.socket?.close(4999, "dev drop");
  }

  /** Leave for good: no reattach, listeners stop receiving events after "closed". */
  close(): void {
    this.stopped = true;
    this.clearTimers();
    this.socket?.close(1000, "bye");
    this.socket = null;
    this.setStatus("closed");
  }

  // ---- internals ---------------------------------------------------------

  private open(first: Envelope): void {
    this.stopped = false;
    this.pendingFirst = first;
    this.setStatus(this.attachment ? "reconnecting" : "connecting");
    const socket = this.createSocket(this.url);
    this.socket = socket;
    socket.onopen = () => {
      if (this.pendingFirst) socket.send(JSON.stringify(this.pendingFirst));
    };
    socket.onmessage = (ev) => this.handleMessage(ev.data);
    socket.onerror = () => {
      /* onclose follows; nothing to do here */
    };
    socket.onclose = (ev) => this.handleClose(ev.code, ev.reason);
  }

  private handleMessage(data: unknown): void {
    if (typeof data !== "string") return;
    let msg: Envelope;
    try {
      msg = JSON.parse(data) as Envelope;
    } catch {
      return;
    }
    if (msg.v !== ENVELOPE_VERSION) return;
    switch (msg.type) {
      case ServerType.roomCreated:
      case ServerType.joined:
      case ServerType.reattached: {
        const reattached = msg.type === ServerType.reattached;
        const prev = this.attachment;
        this.attachment = {
          code: msg.code ?? prev?.code ?? "",
          peerId: msg.peerId ?? prev?.peerId ?? "",
          peerToken: reattached ? (prev?.peerToken ?? "") : (msg.peerToken ?? ""),
          gen: msg.gen ?? 1,
          role: msg.role ?? prev?.role ?? "joiner",
        };
        this.roomState = msg.roomState ?? null;
        this.reconnectAttempt = 0;
        this.pendingFirst = null;
        this.setStatus("attached");
        this.startPing();
        this.emit({
          type: "attached",
          attachment: this.attachment,
          roomState: this.roomState ?? { state: "waiting", peers: [] },
          reattached,
        });
        if (reattached) this.emit({ type: "resync" });
        return;
      }
      case ServerType.peerJoined:
        this.emit({ type: "peer_joined", peerId: msg.peerId ?? "" });
        return;
      case ServerType.peerDisconnected:
        this.emit({ type: "peer_disconnected", peerId: msg.peerId ?? "" });
        return;
      case ServerType.peerReattached:
        this.emit({ type: "peer_reattached", peerId: msg.peerId ?? "", gen: msg.gen ?? 0 });
        return;
      case ServerType.roomState:
        this.roomState = msg.roomState ?? this.roomState;
        if (msg.roomState) this.emit({ type: "room_state", roomState: msg.roomState });
        return;
      case ServerType.roomClosed:
        this.stopped = true; // the room is over; never reattach
        this.clearTimers();
        this.emit({ type: "room_closed", reason: msg.reason ?? "closed" });
        return;
      case ServerType.relay:
        this.emit({ type: "relay", from: msg.from ?? "", payload: msg.payload });
        return;
      case ServerType.pong:
        return;
      case ServerType.error: {
        const code = msg.code ?? "unknown";
        // Errors during attach are fatal for that attempt; the server closes the socket.
        const fatal = this.status !== "attached";
        if (fatal) this.stopped = true;
        this.emit({ type: "error", code, message: msg.message ?? "", fatal });
        return;
      }
      default:
        return;
    }
  }

  private handleClose(code: number, _reason: string): void {
    this.clearPing();
    this.socket = null;
    if (this.stopped) {
      this.setStatus("closed");
      return;
    }
    if (
      code === CloseCode.superseded ||
      code === CloseCode.roomClosed ||
      code === CloseCode.protocol
    ) {
      // Deliberate server-side ends; never reconnect.
      this.stopped = true;
      this.setStatus("closed");
      return;
    }
    if (!this.attachment) {
      this.setStatus("closed");
      return;
    }
    this.scheduleReattach();
  }

  private scheduleReattach(): void {
    const idx = Math.min(this.reconnectAttempt, this.backoffMs.length - 1);
    const delay = this.backoffMs[idx] ?? 1000;
    this.reconnectAttempt += 1;
    this.setStatus("reconnecting");
    this.reconnectHandle = this.setTimer(() => {
      this.reconnectHandle = null;
      const a = this.attachment;
      if (!a || this.stopped) return;
      this.open({
        v: ENVELOPE_VERSION,
        type: ClientType.reattach,
        code: a.code,
        peerId: a.peerId,
        peerToken: a.peerToken,
      });
    }, delay);
  }

  private sendEnvelope(env: Envelope): boolean {
    const s = this.socket;
    if (!s || s.readyState !== SOCKET_OPEN || this.status !== "attached") return false;
    s.send(JSON.stringify(env));
    return true;
  }

  private startPing(): void {
    this.clearPing();
    this.pingHandle = this.setTimer(() => {
      this.pingHandle = null;
      if (this.sendEnvelope({ v: ENVELOPE_VERSION, type: ClientType.ping })) this.startPing();
    }, this.pingIntervalMs);
  }

  private clearPing(): void {
    if (this.pingHandle !== null) {
      this.clearTimer(this.pingHandle);
      this.pingHandle = null;
    }
  }

  private clearTimers(): void {
    this.clearPing();
    if (this.reconnectHandle !== null) {
      this.clearTimer(this.reconnectHandle);
      this.reconnectHandle = null;
    }
  }

  private setStatus(status: ConnectionStatus): void {
    if (this.status === status) return;
    this.status = status;
    this.emit({ type: "status", status });
  }

  private emit(event: SignalingEvent): void {
    for (const l of this.listeners) l(event);
  }
}
