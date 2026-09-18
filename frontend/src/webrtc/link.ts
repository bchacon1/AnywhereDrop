// PeerLink: one WebRTC connection between the two peers of a room, driven by
// signaling relay messages. Implements the fixed rules of
// transfer-protocol.md §6: creator is the only offerer; every negotiation
// message carries an epoch; stale epochs are dropped; ICE candidates are
// queued until the remote description for their epoch is applied; the
// initiator retries with a new epoch on negotiation timeout; the joiner
// asks the server to re-announce peer_joined if no offer arrives.
//
// Phase 3 scope: initial negotiation, ICE-restart re-offer (dev hook and
// T_neg retry), stats. Failure classes F1–F5 recovery is Phase 7.

import type { SignalingEvent } from "../signaling/client";
import type { Role } from "../signaling/messages";
import { DataChannelAdapter } from "./channel";
import {
  isNegotiationMessage,
  type Channel,
  type ConnectionState,
  type DataChannelLike,
  type IceConnectionState,
  type NegotiationMessage,
  type PeerConnectionFactory,
  type PeerConnectionLike,
} from "./types";

/** The two things PeerLink needs from signaling. */
export interface SignalingLike {
  relay(payload: unknown): boolean;
  syncRequest(): boolean;
  on(listener: (ev: SignalingEvent) => void): () => void;
}

export interface LinkStats {
  connectionState: ConnectionState;
  iceConnectionState: IceConnectionState;
  candidateType: string; // host | srflx | prflx | relay | unknown
  rttMs: number | null;
  maxMessageSize: number;
}

export interface LinkState {
  epoch: number;
  attempts: number;
  phase: "idle" | "negotiating" | "connected" | "failed";
  channelGeneration: number;
  stats: LinkStats;
  lastError: string | null;
}

export type LinkEvent =
  | { type: "state"; state: LinkState }
  | { type: "channel"; channel: Channel }
  | { type: "log"; line: string };

export interface LinkOptions {
  role: Role;
  signaling: SignalingLike;
  pcFactory: PeerConnectionFactory;
  iceServers?: { urls: string | string[]; username?: string; credential?: string }[];
  /** Provisional: T_neg 20 s, N_neg 3 (architecture-decisions.md Part B). */
  negotiationTimeoutMs?: number;
  maxNegotiationAttempts?: number;
  statsIntervalMs?: number;
  setTimer?: (fn: () => void, ms: number) => unknown;
  clearTimer?: (h: unknown) => void;
}

const DEFAULT_ICE = [{ urls: "stun:stun.l.google.com:19302" }];

export class PeerLink {
  readonly role: Role;
  private readonly signaling: SignalingLike;
  private readonly pcFactory: PeerConnectionFactory;
  private readonly iceServers: NonNullable<LinkOptions["iceServers"]>;
  private readonly tNeg: number;
  private readonly nNeg: number;
  private readonly statsIntervalMs: number;
  private readonly setTimer: (fn: () => void, ms: number) => unknown;
  private readonly clearTimer: (h: unknown) => void;

  private pc: PeerConnectionLike | null = null;
  private channel: DataChannelAdapter | null = null;
  private listeners = new Set<(ev: LinkEvent) => void>();
  private offSignaling: (() => void) | null = null;

  // Negotiation bookkeeping (transfer-protocol.md §6.2, §6.3).
  private epoch = 0; // creator: epoch of the offer in flight; joiner: highest offer epoch applied
  private remoteApplied = new Set<number>();
  private pending = new Map<number, (RTCIceCandidateInit | null)[]>();
  private negTimer: unknown = null;
  private statsTimer: unknown = null;
  private attempts = 0;
  private offerWaits = 0; // joiner: sync_requests sent while waiting for an offer
  private started = false;
  private closed = false;

  state: LinkState = {
    epoch: 0,
    attempts: 0,
    phase: "idle",
    channelGeneration: 0,
    stats: {
      connectionState: "new",
      iceConnectionState: "new",
      candidateType: "unknown",
      rttMs: null,
      maxMessageSize: 0,
    },
    lastError: null,
  };

  constructor(opts: LinkOptions) {
    this.role = opts.role;
    this.signaling = opts.signaling;
    this.pcFactory = opts.pcFactory;
    this.iceServers = opts.iceServers ?? DEFAULT_ICE;
    this.tNeg = opts.negotiationTimeoutMs ?? 20_000;
    this.nNeg = opts.maxNegotiationAttempts ?? 3;
    this.statsIntervalMs = opts.statsIntervalMs ?? 1000;
    this.setTimer = opts.setTimer ?? ((fn, ms) => setTimeout(fn, ms));
    this.clearTimer = opts.clearTimer ?? ((h) => clearTimeout(h as ReturnType<typeof setTimeout>));
  }

  on(listener: (ev: LinkEvent) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  /**
   * Begin listening to signaling. `paired` says whether the room already has
   * both peers (from the attach reply's room_state), so a creator that missed
   * peer_joined can still start (trace T3), and a joiner can start T_offer.
   */
  start(paired: boolean): void {
    if (this.started) return;
    this.started = true;
    this.offSignaling = this.signaling.on((ev) => this.onSignaling(ev));
    if (this.role === "creator") {
      if (paired) this.startNegotiation(true);
    } else if (paired) {
      this.armOfferWait();
    }
  }

  /** Dev hook / Phase 7 F2: re-offer with new ICE credentials on the same connection. */
  restartIce(): void {
    if (this.role !== "creator" || !this.pc) return;
    this.log("ice_restart requested");
    this.startNegotiation(false);
  }

  close(): void {
    this.closed = true;
    this.clearNegTimer();
    if (this.statsTimer !== null) this.clearTimer(this.statsTimer);
    this.offSignaling?.();
    this.pc?.close();
    this.pc = null;
  }

  // ---- signaling in --------------------------------------------------------

  private onSignaling(ev: SignalingEvent): void {
    if (this.closed) return;
    switch (ev.type) {
      case "peer_joined":
        // Idempotent: a re-announced peer_joined after negotiation started is ignored.
        if (this.role === "creator" && this.state.phase === "idle") this.startNegotiation(true);
        return;
      case "attached":
        // Reconciliation after reattach: if the room is paired and we never
        // started, behave as if peer_joined arrived (transfer-protocol.md §6.4).
        if (ev.roomState.state === "paired" && this.state.phase === "idle") {
          if (this.role === "creator") this.startNegotiation(true);
          else this.armOfferWait();
        }
        return;
      case "room_state":
        if (
          ev.roomState.state === "paired" &&
          this.state.phase === "idle" &&
          this.role === "creator"
        ) {
          this.startNegotiation(true);
        }
        return;
      case "resync":
        // Mid-negotiation: treat as T_neg expiry (initiator retries, responder waits).
        if (this.state.phase === "negotiating" && this.role === "creator")
          this.onNegotiationTimeout();
        return;
      case "relay":
        if (isNegotiationMessage(ev.payload)) void this.onNegotiation(ev.payload);
        return;
      default:
        return;
    }
  }

  private async onNegotiation(msg: NegotiationMessage): Promise<void> {
    try {
      switch (msg.kind) {
        case "offer":
          if (this.role !== "joiner") return;
          if (msg.epoch <= this.epoch) {
            this.log(`drop stale offer epoch=${msg.epoch} (current ${this.epoch})`);
            return;
          }
          await this.acceptOffer(msg);
          return;
        case "answer":
          if (this.role !== "creator" || !this.pc) return;
          if (msg.epoch !== this.epoch) {
            this.log(`drop answer epoch=${msg.epoch} (current ${this.epoch})`);
            return;
          }
          await this.pc.setRemoteDescription({ type: "answer", sdp: msg.sdp });
          this.markRemoteApplied(msg.epoch);
          return;
        case "ice":
          await this.onIce(msg);
          return;
      }
    } catch (e) {
      this.fail(`negotiation error: ${(e as Error).message}`);
    }
  }

  // ---- creator -------------------------------------------------------------

  private startNegotiation(newConnection: boolean): void {
    if (this.attempts >= this.nNeg && this.state.phase !== "connected") {
      this.fail("could not connect: negotiation attempts exhausted");
      return;
    }
    this.attempts += 1;
    this.epoch += 1;
    const epoch = this.epoch;
    this.setPhase("negotiating");
    this.clearOfferWait();

    if (newConnection || !this.pc) {
      this.replaceConnection();
      const dc = this.pc!.createDataChannel("main", { ordered: true });
      this.adoptChannel(dc);
    } else {
      this.pc.restartIce();
    }
    const pc = this.pc!;
    this.log(`offer epoch=${epoch} newConnection=${newConnection}`);
    void (async () => {
      try {
        const offer = await pc.createOffer();
        await pc.setLocalDescription(offer as { type: string; sdp?: string });
        if (this.epoch !== epoch || this.pc !== pc) return; // superseded meanwhile
        this.signaling.relay({
          kind: "offer",
          epoch,
          newConnection,
          sdp: offer.sdp ?? "",
        } satisfies NegotiationMessage);
        this.armNegTimer();
      } catch (e) {
        this.fail(`offer failed: ${(e as Error).message}`);
      }
    })();
  }

  private armNegTimer(): void {
    this.clearNegTimer();
    this.negTimer = this.setTimer(() => {
      this.negTimer = null;
      this.onNegotiationTimeout();
    }, this.tNeg);
  }

  private clearNegTimer(): void {
    if (this.negTimer !== null) {
      this.clearTimer(this.negTimer);
      this.negTimer = null;
    }
  }

  private onNegotiationTimeout(): void {
    if (this.state.phase !== "negotiating") return;
    this.log(`negotiation timeout epoch=${this.epoch} attempt=${this.attempts}`);
    // Retry with an ICE restart on the same connection (Phase 7 refines to F4 after N_restart).
    this.startNegotiation(false);
  }

  // ---- joiner --------------------------------------------------------------

  private async acceptOffer(msg: Extract<NegotiationMessage, { kind: "offer" }>): Promise<void> {
    this.epoch = msg.epoch;
    this.setPhase("negotiating");
    this.clearOfferWait();
    if (msg.newConnection || !this.pc) {
      this.replaceConnection();
      this.pc!.ondatachannel = (ev) => this.adoptChannel(ev.channel);
    }
    const pc = this.pc!;
    this.log(`offer received epoch=${msg.epoch} newConnection=${msg.newConnection}`);
    await pc.setRemoteDescription({ type: "offer", sdp: msg.sdp });
    if (this.pc !== pc || this.epoch !== msg.epoch) return;
    this.markRemoteApplied(msg.epoch);
    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer as { type: string; sdp?: string });
    if (this.pc !== pc || this.epoch !== msg.epoch) return;
    this.signaling.relay({
      kind: "answer",
      epoch: msg.epoch,
      sdp: answer.sdp ?? "",
    } satisfies NegotiationMessage);
  }

  private armOfferWait(): void {
    // T_offer = T_neg: if no offer arrives, ask the server to re-announce us.
    this.clearNegTimer();
    this.negTimer = this.setTimer(() => {
      this.negTimer = null;
      if (this.state.phase !== "idle") return;
      if (this.offerWaits >= this.nNeg) {
        this.fail("peer_unresponsive: no offer received");
        return;
      }
      this.offerWaits += 1;
      this.log(`no offer yet; sync_request #${this.offerWaits}`);
      this.signaling.syncRequest();
      this.armOfferWait();
    }, this.tNeg);
  }

  private clearOfferWait(): void {
    if (this.role === "joiner") this.clearNegTimer();
  }

  // ---- ICE candidates -----------------------------------------------------

  private async onIce(msg: Extract<NegotiationMessage, { kind: "ice" }>): Promise<void> {
    if (msg.epoch < this.epoch) return; // stale generation
    if (msg.epoch > this.epoch + 1) return; // too far ahead; bounded queue
    if (this.remoteApplied.has(msg.epoch) && this.pc) {
      await this.pc.addIceCandidate(msg.candidate);
      return;
    }
    const q = this.pending.get(msg.epoch) ?? [];
    q.push(msg.candidate);
    this.pending.set(msg.epoch, q);
  }

  private markRemoteApplied(epoch: number): void {
    this.remoteApplied.add(epoch);
    for (const e of [...this.pending.keys()]) if (e < epoch) this.pending.delete(e);
    const q = this.pending.get(epoch);
    if (q && this.pc) {
      this.pending.delete(epoch);
      const pc = this.pc;
      void (async () => {
        for (const c of q) {
          try {
            await pc.addIceCandidate(c);
          } catch (e) {
            this.log(`addIceCandidate failed: ${(e as Error).message}`);
          }
        }
      })();
    }
  }

  // ---- connection and channel ---------------------------------------------

  private replaceConnection(): void {
    this.pc?.close();
    this.remoteApplied.clear();
    this.pending.clear();
    const pc = this.pcFactory.create(this.iceServers);
    this.pc = pc;
    pc.onicecandidate = (ev) => {
      if (this.pc !== pc) return;
      this.signaling.relay({
        kind: "ice",
        epoch: this.epoch,
        candidate: ev.candidate,
      } satisfies NegotiationMessage);
    };
    pc.onconnectionstatechange = () => {
      if (this.pc !== pc) return;
      this.state.stats.connectionState = pc.connectionState;
      this.log(`connectionState=${pc.connectionState}`);
      if (pc.connectionState === "connected") {
        this.clearNegTimer();
        this.attempts = 0;
        this.setPhase("connected");
      }
      this.emitState();
    };
    pc.oniceconnectionstatechange = () => {
      if (this.pc !== pc) return;
      this.state.stats.iceConnectionState = pc.iceConnectionState;
      this.log(`iceConnectionState=${pc.iceConnectionState}`);
      this.emitState();
    };
    if (this.statsTimer === null) this.scheduleStats();
  }

  private adoptChannel(dc: DataChannelLike): void {
    const generation = this.state.channelGeneration + 1;
    this.state.channelGeneration = generation;
    const ch = new DataChannelAdapter(dc, generation, () => this.pc?.sctp?.maxMessageSize ?? 0);
    this.channel = ch;
    const announce = () => {
      this.log(`channel open generation=${generation} maxMessageSize=${ch.maxMessageSize}`);
      this.state.stats.maxMessageSize = ch.maxMessageSize;
      this.emit({ type: "channel", channel: ch });
      this.emitState();
    };
    if (dc.readyState === "open") announce();
    else dc.onopen = announce;
    ch.onClose(() => this.log(`channel closed generation=${generation}`));
  }

  private scheduleStats(): void {
    this.statsTimer = this.setTimer(() => {
      this.statsTimer = null;
      void this.pollStats().finally(() => {
        if (!this.closed) this.scheduleStats();
      });
    }, this.statsIntervalMs);
  }

  private async pollStats(): Promise<void> {
    const pc = this.pc;
    if (!pc) return;
    let candidateType = "unknown";
    let rttMs: number | null = null;
    try {
      const entries = [...(await pc.getStats())];
      const byId = new Map(entries);
      let pair: Record<string, unknown> | undefined;
      for (const [, s] of entries) {
        if (s.type === "transport" && typeof s.selectedCandidatePairId === "string") {
          pair = byId.get(s.selectedCandidatePairId);
          break;
        }
      }
      if (!pair) {
        for (const [, s] of entries) {
          if (
            s.type === "candidate-pair" &&
            (s.selected === true || (s.state === "succeeded" && s.nominated === true))
          ) {
            pair = s;
            break;
          }
        }
      }
      if (pair) {
        const local = byId.get(String(pair.localCandidateId));
        if (local && typeof local.candidateType === "string") candidateType = local.candidateType;
        if (typeof pair.currentRoundTripTime === "number")
          rttMs = Math.round(pair.currentRoundTripTime * 1000);
      }
    } catch {
      /* stats are best-effort */
    }
    const s = this.state.stats;
    if (s.candidateType !== candidateType || s.rttMs !== rttMs) {
      s.candidateType = candidateType;
      s.rttMs = rttMs;
      this.emitState();
    }
  }

  // ---- state and events ----------------------------------------------------

  private setPhase(phase: LinkState["phase"]): void {
    this.state.phase = phase;
    this.state.epoch = this.epoch;
    this.state.attempts = this.attempts;
    this.emitState();
  }

  private fail(reason: string): void {
    this.clearNegTimer();
    this.state.lastError = reason;
    this.log(reason);
    this.setPhase("failed");
  }

  private emitState(): void {
    this.state.epoch = this.epoch;
    this.state.attempts = this.attempts;
    this.emit({ type: "state", state: { ...this.state, stats: { ...this.state.stats } } });
  }

  private log(line: string): void {
    this.emit({ type: "log", line });
  }

  private emit(ev: LinkEvent): void {
    for (const l of this.listeners) l(ev);
  }
}
