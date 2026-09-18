// Interfaces the WebRTC layer exposes upward and the browser shapes it
// depends on downward. transfer/ sees only Channel (system-design.md §5);
// tests supply fakes for PeerConnectionLike.

/** What the transfer protocol is written against. Implemented by DataChannelAdapter. */
export interface Channel {
  send(data: string | ArrayBuffer): void;
  onMessage(cb: (data: string | ArrayBuffer) => void): void;
  readonly bufferedAmount: number;
  bufferedAmountLowThreshold: number;
  onBufferedAmountLow(cb: () => void): void;
  /** From pc.sctp.maxMessageSize; 0 if the SCTP transport is not negotiated yet. */
  readonly maxMessageSize: number;
  /** 1 for the first channel of a link, +1 for each replacement (transfer-protocol.md §5.3). */
  readonly generation: number;
  readonly readyState: "connecting" | "open" | "closing" | "closed";
  onClose(cb: () => void): void;
  close(): void;
}

/** Negotiation messages carried inside signaling `relay` payloads. */
export type NegotiationMessage =
  | { kind: "offer"; epoch: number; newConnection: boolean; sdp: string }
  | { kind: "answer"; epoch: number; sdp: string }
  | { kind: "ice"; epoch: number; candidate: RTCIceCandidateInit | null };

export function isNegotiationMessage(p: unknown): p is NegotiationMessage {
  if (typeof p !== "object" || p === null) return false;
  const k = (p as { kind?: unknown }).kind;
  return k === "offer" || k === "answer" || k === "ice";
}

/** The subset of RTCDataChannel this layer uses. */
export interface DataChannelLike {
  readonly label: string;
  readonly readyState: "connecting" | "open" | "closing" | "closed";
  readonly bufferedAmount: number;
  bufferedAmountLowThreshold: number;
  binaryType: string;
  send(data: string | ArrayBuffer): void;
  close(): void;
  onopen: ((ev: unknown) => void) | null;
  onmessage: ((ev: { data: unknown }) => void) | null;
  onclose: ((ev: unknown) => void) | null;
  onerror: ((ev: unknown) => void) | null;
  onbufferedamountlow: ((ev: unknown) => void) | null;
}

export type ConnectionState =
  "new" | "connecting" | "connected" | "disconnected" | "failed" | "closed";
export type IceConnectionState =
  "new" | "checking" | "connected" | "completed" | "disconnected" | "failed" | "closed";

/** The subset of RTCPeerConnection this layer uses. */
export interface PeerConnectionLike {
  readonly connectionState: ConnectionState;
  readonly iceConnectionState: IceConnectionState;
  readonly sctp: { readonly maxMessageSize: number } | null;
  createOffer(): Promise<{ type?: string; sdp?: string }>;
  createAnswer(): Promise<{ type?: string; sdp?: string }>;
  setLocalDescription(desc?: { type: string; sdp?: string }): Promise<void>;
  setRemoteDescription(desc: { type: string; sdp?: string }): Promise<void>;
  addIceCandidate(candidate: RTCIceCandidateInit | null | undefined): Promise<void>;
  restartIce(): void;
  createDataChannel(label: string, init?: { ordered?: boolean }): DataChannelLike;
  getStats(): Promise<Iterable<[string, Record<string, unknown>]>>;
  close(): void;
  onicecandidate: ((ev: { candidate: RTCIceCandidateInit | null }) => void) | null;
  onconnectionstatechange: ((ev: unknown) => void) | null;
  oniceconnectionstatechange: ((ev: unknown) => void) | null;
  ondatachannel: ((ev: { channel: DataChannelLike }) => void) | null;
}

export interface PeerConnectionFactory {
  create(
    iceServers: { urls: string | string[]; username?: string; credential?: string }[],
  ): PeerConnectionLike;
}

/** Production adapter over the real RTCPeerConnection. */
class BrowserPeerConnection implements PeerConnectionLike {
  private readonly pc: RTCPeerConnection;
  constructor(iceServers: RTCIceServer[]) {
    this.pc = new RTCPeerConnection({ iceServers });
  }
  get connectionState(): ConnectionState {
    return this.pc.connectionState;
  }
  get iceConnectionState(): IceConnectionState {
    return this.pc.iceConnectionState;
  }
  get sctp(): { readonly maxMessageSize: number } | null {
    return this.pc.sctp;
  }
  createOffer() {
    return this.pc.createOffer();
  }
  createAnswer() {
    return this.pc.createAnswer();
  }
  setLocalDescription(desc?: { type: string; sdp?: string }) {
    return this.pc.setLocalDescription(desc as RTCLocalSessionDescriptionInit | undefined);
  }
  setRemoteDescription(desc: { type: string; sdp?: string }) {
    return this.pc.setRemoteDescription(desc as RTCSessionDescriptionInit);
  }
  addIceCandidate(candidate: RTCIceCandidateInit | null | undefined) {
    return this.pc.addIceCandidate(candidate ?? undefined);
  }
  restartIce() {
    this.pc.restartIce();
  }
  createDataChannel(label: string, init?: { ordered?: boolean }): DataChannelLike {
    return this.pc.createDataChannel(label, init) as unknown as DataChannelLike;
  }
  async getStats(): Promise<Iterable<[string, Record<string, unknown>]>> {
    const report = await this.pc.getStats();
    const out: [string, Record<string, unknown>][] = [];
    report.forEach((v: unknown, k: string) => out.push([k, v as Record<string, unknown>]));
    return out;
  }
  close() {
    this.pc.close();
  }
  set onicecandidate(h: PeerConnectionLike["onicecandidate"]) {
    this.pc.onicecandidate = h
      ? (ev) => h({ candidate: ev.candidate ? ev.candidate.toJSON() : null })
      : null;
  }
  set onconnectionstatechange(h: PeerConnectionLike["onconnectionstatechange"]) {
    this.pc.onconnectionstatechange = h;
  }
  set oniceconnectionstatechange(h: PeerConnectionLike["oniceconnectionstatechange"]) {
    this.pc.oniceconnectionstatechange = h;
  }
  set ondatachannel(h: PeerConnectionLike["ondatachannel"]) {
    this.pc.ondatachannel = h
      ? (ev) => h({ channel: ev.channel as unknown as DataChannelLike })
      : null;
  }
}

export const browserPeerConnectionFactory: PeerConnectionFactory = {
  create(iceServers) {
    return new BrowserPeerConnection(iceServers as RTCIceServer[]);
  },
};
