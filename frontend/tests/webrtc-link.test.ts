import { describe, expect, it } from "vitest";
import type { SignalingEvent } from "../src/signaling/client";
import { PeerLink, type LinkEvent, type SignalingLike } from "../src/webrtc/link";
import type {
  DataChannelLike,
  PeerConnectionFactory,
  PeerConnectionLike,
} from "../src/webrtc/types";

// ---- fakes ---------------------------------------------------------------

class FakeTimers {
  private q: { at: number; fn: () => void; id: number }[] = [];
  private now = 0;
  private id = 1;
  set = (fn: () => void, ms: number) => {
    const id = this.id++;
    this.q.push({ at: this.now + ms, fn, id });
    return id;
  };
  clear = (h: unknown) => {
    this.q = this.q.filter((t) => t.id !== h);
  };
  advance(ms: number) {
    this.now += ms;
    const due = this.q.filter((t) => t.at <= this.now).sort((a, b) => a.at - b.at);
    this.q = this.q.filter((t) => t.at > this.now);
    for (const t of due) t.fn();
  }
}

class FakeSignaling implements SignalingLike {
  listeners = new Set<(ev: SignalingEvent) => void>();
  relayed: unknown[] = [];
  syncRequests = 0;
  relay(p: unknown) {
    this.relayed.push(p);
    return true;
  }
  syncRequest() {
    this.syncRequests += 1;
    return true;
  }
  on(l: (ev: SignalingEvent) => void) {
    this.listeners.add(l);
    return () => this.listeners.delete(l);
  }
  emit(ev: SignalingEvent) {
    for (const l of this.listeners) l(ev);
  }
  lastRelay<T>(): T {
    return this.relayed[this.relayed.length - 1] as T;
  }
}

class FakeDC implements DataChannelLike {
  readyState: DataChannelLike["readyState"] = "connecting";
  bufferedAmount = 0;
  bufferedAmountLowThreshold = 0;
  binaryType = "blob";
  sent: (string | ArrayBuffer)[] = [];
  onopen: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onclose: ((ev: unknown) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  onbufferedamountlow: ((ev: unknown) => void) | null = null;
  constructor(public label: string) {}
  send(d: string | ArrayBuffer) {
    this.sent.push(d);
  }
  close() {
    this.readyState = "closed";
    this.onclose?.(undefined);
  }
  open() {
    this.readyState = "open";
    this.onopen?.(undefined);
  }
}

/** A peer connection whose async steps resolve only when the test says so. */
class FakePC implements PeerConnectionLike {
  connectionState: PeerConnectionLike["connectionState"] = "new";
  iceConnectionState: PeerConnectionLike["iceConnectionState"] = "new";
  sctp: { maxMessageSize: number } | null = { maxMessageSize: 262144 };
  added: (RTCIceCandidateInit | null | undefined)[] = [];
  restarts = 0;
  closed = false;
  channels: FakeDC[] = [];
  srdResolvers: (() => void)[] = [];
  holdSRD = false;
  onicecandidate: PeerConnectionLike["onicecandidate"] = null;
  onconnectionstatechange: PeerConnectionLike["onconnectionstatechange"] = null;
  oniceconnectionstatechange: PeerConnectionLike["oniceconnectionstatechange"] = null;
  ondatachannel: PeerConnectionLike["ondatachannel"] = null;

  async createOffer() {
    return { type: "offer", sdp: "v=0 offer" };
  }
  async createAnswer() {
    return { type: "answer", sdp: "v=0 answer" };
  }
  async setLocalDescription() {}
  setRemoteDescription(): Promise<void> {
    if (!this.holdSRD) return Promise.resolve();
    return new Promise((res) => this.srdResolvers.push(res));
  }
  releaseSRD() {
    const r = this.srdResolvers.shift();
    r?.();
  }
  async addIceCandidate(c: RTCIceCandidateInit | null | undefined) {
    this.added.push(c);
  }
  restartIce() {
    this.restarts += 1;
  }
  createDataChannel(label: string) {
    const dc = new FakeDC(label);
    this.channels.push(dc);
    return dc;
  }
  async getStats() {
    return [] as [string, Record<string, unknown>][];
  }
  close() {
    this.closed = true;
  }
  setConnected() {
    this.connectionState = "connected";
    this.onconnectionstatechange?.(undefined);
  }
}

class FakeFactory implements PeerConnectionFactory {
  created: FakePC[] = [];
  create() {
    const pc = new FakePC();
    this.created.push(pc);
    return pc;
  }
}

const paired = { state: "paired" as const, peers: [] };

function make(role: "creator" | "joiner") {
  const timers = new FakeTimers();
  const sig = new FakeSignaling();
  const factory = new FakeFactory();
  const events: LinkEvent[] = [];
  const link = new PeerLink({
    role,
    signaling: sig,
    pcFactory: factory,
    negotiationTimeoutMs: 20_000,
    maxNegotiationAttempts: 3,
    statsIntervalMs: 1000,
    setTimer: timers.set,
    clearTimer: timers.clear,
  });
  link.on((e) => events.push(e));
  return {
    link,
    timers,
    sig,
    factory,
    events,
    logs: () => events.filter((e) => e.type === "log").map((e) => (e as { line: string }).line),
  };
}

const tick = () => new Promise((r) => setTimeout(r, 0));

// ---- tests -----------------------------------------------------------------

describe("PeerLink creator", () => {
  it("offers with epoch 1 on peer_joined and ignores a duplicate peer_joined", async () => {
    const { link, sig, factory } = make("creator");
    link.start(false);
    sig.emit({ type: "peer_joined", peerId: "j" });
    await tick();
    expect(factory.created.length).toBe(1);
    expect(factory.created[0]!.channels[0]!.label).toBe("main");
    expect(sig.lastRelay()).toEqual({
      kind: "offer",
      epoch: 1,
      newConnection: true,
      sdp: "v=0 offer",
    });

    sig.emit({ type: "peer_joined", peerId: "j" }); // re-announced (trace T3 tail)
    await tick();
    expect(sig.relayed.filter((r) => (r as { kind: string }).kind === "offer").length).toBe(1);
  });

  it("starts from a paired room_state after reattach when it never saw peer_joined (trace T3)", async () => {
    const { link, sig } = make("creator");
    link.start(false);
    sig.emit({
      type: "attached",
      attachment: { code: "X", peerId: "c", peerToken: "t", gen: 2, role: "creator" },
      roomState: paired,
      reattached: true,
    });
    await tick();
    expect(sig.lastRelay<{ kind: string; epoch: number }>()).toMatchObject({
      kind: "offer",
      epoch: 1,
    });
  });

  it("applies the answer only for the current epoch and drops stale ones", async () => {
    const { link, sig, factory, logs } = make("creator");
    link.start(true);
    await tick();
    sig.emit({ type: "relay", from: "j", payload: { kind: "answer", epoch: 7, sdp: "x" } });
    await tick();
    expect(logs().some((l) => l.includes("drop answer epoch=7"))).toBe(true);
    sig.emit({ type: "relay", from: "j", payload: { kind: "answer", epoch: 1, sdp: "x" } });
    await tick();
    factory.created[0]!.setConnected();
    expect(link.state.phase).toBe("connected");
  });

  it("retries with an ICE restart and a new epoch on negotiation timeout, then fails after N_neg", async () => {
    const { link, sig, factory, timers } = make("creator");
    link.start(true);
    await tick();
    expect(link.state.epoch).toBe(1);
    timers.advance(20_000);
    await tick();
    expect(link.state.epoch).toBe(2);
    expect(factory.created[0]!.restarts).toBe(1);
    expect(sig.lastRelay()).toMatchObject({ kind: "offer", epoch: 2, newConnection: false });
    timers.advance(20_000);
    await tick();
    expect(link.state.epoch).toBe(3);
    timers.advance(20_000);
    await tick();
    expect(link.state.phase).toBe("failed");
    expect(link.state.lastError).toMatch(/attempts exhausted/);
  });

  it("emits the channel when it opens, with generation 1 and maxMessageSize", async () => {
    const { link, factory, events } = make("creator");
    link.start(true);
    await tick();
    factory.created[0]!.channels[0]!.open();
    const ch = events.find((e) => e.type === "channel") as {
      channel: { generation: number; maxMessageSize: number };
    };
    expect(ch.channel.generation).toBe(1);
    expect(ch.channel.maxMessageSize).toBe(262144);
  });

  it("resync during negotiation retries like a timeout", async () => {
    const { link, sig } = make("creator");
    link.start(true);
    await tick();
    sig.emit({ type: "resync" });
    await tick();
    expect(sig.lastRelay()).toMatchObject({ kind: "offer", epoch: 2 });
  });
});

describe("PeerLink joiner", () => {
  it("answers an offer and queues candidates until setRemoteDescription resolves (trace T5)", async () => {
    const { link, sig, factory } = make("joiner");
    link.start(true);
    sig.emit({
      type: "relay",
      from: "c",
      payload: { kind: "offer", epoch: 1, newConnection: true, sdp: "o" },
    });
    await tick();
    const pc = factory.created[0]!;
    // Hold the next SRD: candidates arriving now must be queued, not applied.
    // (acceptOffer already called SRD once and it resolved; simulate the pending window
    // by re-sending an epoch-2 offer with SRD held.)
    pc.holdSRD = true;
    sig.emit({
      type: "relay",
      from: "c",
      payload: { kind: "offer", epoch: 2, newConnection: false, sdp: "o2" },
    });
    await tick();
    sig.emit({
      type: "relay",
      from: "c",
      payload: { kind: "ice", epoch: 2, candidate: { candidate: "a" } },
    });
    sig.emit({
      type: "relay",
      from: "c",
      payload: { kind: "ice", epoch: 2, candidate: { candidate: "b" } },
    });
    await tick();
    expect(pc.added.length).toBe(0);
    pc.releaseSRD();
    await tick();
    await tick();
    expect(pc.added.map((c) => (c as { candidate: string }).candidate)).toEqual(["a", "b"]);
    expect(sig.lastRelay()).toMatchObject({ kind: "answer", epoch: 2 });
  });

  it("drops stale-epoch offers and candidates, queues one epoch ahead", async () => {
    const { link, sig, factory, logs } = make("joiner");
    link.start(true);
    sig.emit({
      type: "relay",
      from: "c",
      payload: { kind: "offer", epoch: 3, newConnection: true, sdp: "o" },
    });
    await tick();
    const pc = factory.created[0]!;
    sig.emit({
      type: "relay",
      from: "c",
      payload: { kind: "offer", epoch: 2, newConnection: true, sdp: "old" },
    });
    await tick();
    expect(logs().some((l) => l.includes("drop stale offer epoch=2"))).toBe(true);
    sig.emit({
      type: "relay",
      from: "c",
      payload: { kind: "ice", epoch: 2, candidate: { candidate: "stale" } },
    });
    sig.emit({
      type: "relay",
      from: "c",
      payload: { kind: "ice", epoch: 4, candidate: { candidate: "ahead" } },
    });
    sig.emit({
      type: "relay",
      from: "c",
      payload: { kind: "ice", epoch: 9, candidate: { candidate: "far" } },
    });
    await tick();
    expect(pc.added.length).toBe(0);
    // The epoch-4 offer arrives: its queued candidate is applied after SRD.
    sig.emit({
      type: "relay",
      from: "c",
      payload: { kind: "offer", epoch: 4, newConnection: false, sdp: "o4" },
    });
    await tick();
    await tick();
    expect(pc.added.map((c) => (c as { candidate: string }).candidate)).toEqual(["ahead"]);
  });

  it("sends sync_request when no offer arrives within T_offer, up to N_neg, then fails", () => {
    const { link, sig, timers } = make("joiner");
    link.start(true);
    timers.advance(20_000);
    expect(sig.syncRequests).toBe(1);
    timers.advance(20_000);
    timers.advance(20_000);
    expect(sig.syncRequests).toBe(3);
    timers.advance(20_000);
    expect(link.state.phase).toBe("failed");
    expect(link.state.lastError).toMatch(/peer_unresponsive/);
  });

  it("adopts the incoming data channel", async () => {
    const { link, sig, factory, events } = make("joiner");
    link.start(true);
    sig.emit({
      type: "relay",
      from: "c",
      payload: { kind: "offer", epoch: 1, newConnection: true, sdp: "o" },
    });
    await tick();
    const pc = factory.created[0]!;
    const dc = new FakeDC("main");
    pc.ondatachannel?.({ channel: dc });
    dc.open();
    expect(events.some((e) => e.type === "channel")).toBe(true);
  });
});
