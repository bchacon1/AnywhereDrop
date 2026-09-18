import { describe, expect, it } from "vitest";
import { SignalingClient, type SignalingEvent, type SocketLike } from "../src/signaling/client";

/** A scripted fake socket. Tests drive it: open(), receive(json), serverClose(code). */
class FakeSocket implements SocketLike {
  static instances: FakeSocket[] = [];
  readyState = 0;
  sent: string[] = [];
  onopen: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onclose: ((ev: { code: number; reason: string }) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  constructor(public url: string) {
    FakeSocket.instances.push(this);
  }
  send(data: string) {
    this.sent.push(data);
  }
  close(code = 1000, reason = "") {
    this.readyState = 3;
    this.onclose?.({ code, reason });
  }
  open() {
    this.readyState = 1;
    this.onopen?.(undefined);
  }
  receive(msg: object) {
    this.onmessage?.({ data: JSON.stringify({ v: 1, ...msg }) });
  }
  serverClose(code: number) {
    this.readyState = 3;
    this.onclose?.({ code, reason: "" });
  }
  lastSent(): Record<string, unknown> {
    return JSON.parse(this.sent[this.sent.length - 1] ?? "{}") as Record<string, unknown>;
  }
}

/** Manual timers so backoff and ping are deterministic. */
class FakeTimers {
  private queue: { at: number; fn: () => void; id: number }[] = [];
  private now = 0;
  private nextId = 1;
  set = (fn: () => void, ms: number) => {
    const id = this.nextId++;
    this.queue.push({ at: this.now + ms, fn, id });
    return id;
  };
  clear = (h: unknown) => {
    this.queue = this.queue.filter((t) => t.id !== h);
  };
  advance(ms: number) {
    this.now += ms;
    const due = this.queue.filter((t) => t.at <= this.now).sort((a, b) => a.at - b.at);
    this.queue = this.queue.filter((t) => t.at > this.now);
    for (const t of due) t.fn();
  }
  pending() {
    return this.queue.length;
  }
}

function setup() {
  FakeSocket.instances = [];
  const timers = new FakeTimers();
  const events: SignalingEvent[] = [];
  const client = new SignalingClient({
    url: "ws://test/ws",
    createSocket: (u) => new FakeSocket(u),
    pingIntervalMs: 20_000,
    backoffMs: [1000, 2000, 4000, 8000],
    setTimer: timers.set,
    clearTimer: timers.clear,
  });
  client.on((e) => events.push(e));
  return { client, timers, events, sockets: FakeSocket.instances };
}

const attachedCreator = {
  type: "room_created",
  code: "ABC234",
  peerId: "p1",
  peerToken: "tok1",
  gen: 1,
  role: "creator",
  roomState: {
    state: "waiting",
    peers: [{ peerId: "p1", role: "creator", connected: true, gen: 1 }],
  },
};

describe("SignalingClient", () => {
  it("creates a room: sends create_room on open and becomes attached", () => {
    const { client, sockets, events } = setup();
    client.createRoom();
    const s = sockets[0]!;
    expect(client.status).toBe("connecting");
    s.open();
    expect(s.lastSent()).toEqual({ v: 1, type: "create_room" });
    s.receive(attachedCreator);
    expect(client.status).toBe("attached");
    expect(client.attachment).toMatchObject({
      code: "ABC234",
      peerId: "p1",
      peerToken: "tok1",
      gen: 1,
    });
    expect(events.find((e) => e.type === "attached")).toBeTruthy();
  });

  it("joins with a normalised code", () => {
    const { client, sockets } = setup();
    client.joinRoom(" abc234 ");
    sockets[0]!.open();
    expect(sockets[0]!.lastSent()).toEqual({ v: 1, type: "join_room", code: "ABC234" });
  });

  it("pings every interval while attached and stops when closed", () => {
    const { client, sockets, timers } = setup();
    client.createRoom();
    sockets[0]!.open();
    sockets[0]!.receive(attachedCreator);
    timers.advance(20_000);
    expect(sockets[0]!.lastSent()).toEqual({ v: 1, type: "ping" });
    timers.advance(20_000);
    expect(sockets[0]!.sent.filter((m) => m.includes('"ping"')).length).toBe(2);
    client.close();
    expect(timers.pending()).toBe(0);
  });

  it("reattaches with backoff after an unexpected close, keeping the token", () => {
    const { client, sockets, timers, events } = setup();
    client.createRoom();
    sockets[0]!.open();
    sockets[0]!.receive(attachedCreator);

    sockets[0]!.serverClose(1006); // abnormal
    expect(client.status).toBe("reconnecting");
    expect(sockets.length).toBe(1);
    timers.advance(999);
    expect(sockets.length).toBe(1);
    timers.advance(1);
    expect(sockets.length).toBe(2);
    const s2 = sockets[1]!;
    s2.open();
    expect(s2.lastSent()).toEqual({
      v: 1,
      type: "reattach",
      code: "ABC234",
      peerId: "p1",
      peerToken: "tok1",
    });
    s2.receive({
      type: "reattached",
      code: "ABC234",
      peerId: "p1",
      gen: 2,
      role: "creator",
      roomState: { state: "paired", peers: [] },
    });
    expect(client.status).toBe("attached");
    expect(client.attachment?.gen).toBe(2);
    expect(client.attachment?.peerToken).toBe("tok1"); // token retained, server never resends it
    expect(events.some((e) => e.type === "resync")).toBe(true);

    // A successful reattach resets the backoff: the next drop retries after 1 s again.
    s2.serverClose(1006);
    timers.advance(1000);
    expect(sockets.length).toBe(3);
    // If that attempt fails before attaching, the following one waits 2 s, then 4 s.
    sockets[2]!.serverClose(1006);
    timers.advance(1999);
    expect(sockets.length).toBe(3);
    timers.advance(1);
    expect(sockets.length).toBe(4);
    sockets[3]!.serverClose(1006);
    timers.advance(3999);
    expect(sockets.length).toBe(4);
    timers.advance(1);
    expect(sockets.length).toBe(5);
  });

  it("does not reattach after superseded or room_closed close codes", () => {
    for (const code of [4001, 4002]) {
      const { client, sockets, timers } = setup();
      client.createRoom();
      sockets[0]!.open();
      sockets[0]!.receive(attachedCreator);
      sockets[0]!.serverClose(code);
      timers.advance(60_000);
      expect(sockets.length).toBe(1);
      expect(client.status).toBe("closed");
    }
  });

  it("surfaces attach errors as fatal and does not reconnect", () => {
    const { client, sockets, events, timers } = setup();
    client.joinRoom("NOPE00");
    sockets[0]!.open();
    sockets[0]!.receive({ type: "error", code: "room_not_found", message: "room not found" });
    sockets[0]!.serverClose(1008);
    timers.advance(60_000);
    const err = events.find((e) => e.type === "error");
    expect(err).toMatchObject({ code: "room_not_found", fatal: true });
    expect(sockets.length).toBe(1);
    expect(client.status).toBe("closed");
  });

  it("forwards relay, peer events and room_closed; room_closed stops reconnects", () => {
    const { client, sockets, events, timers } = setup();
    client.createRoom();
    sockets[0]!.open();
    sockets[0]!.receive(attachedCreator);
    sockets[0]!.receive({ type: "peer_joined", peerId: "p2" });
    sockets[0]!.receive({ type: "relay", from: "p2", payload: { hello: 1 } });
    expect(client.relay({ hi: 2 })).toBe(true);
    expect(sockets[0]!.lastSent()).toEqual({ v: 1, type: "relay", payload: { hi: 2 } });
    sockets[0]!.receive({ type: "room_closed", reason: "peer_gone" });
    sockets[0]!.serverClose(4002);
    timers.advance(60_000);
    expect(events.map((e) => e.type)).toEqual(
      expect.arrayContaining(["peer_joined", "relay", "room_closed"]),
    );
    expect(sockets.length).toBe(1);
  });

  it("refuses to relay when not attached", () => {
    const { client } = setup();
    expect(client.relay({ x: 1 })).toBe(false);
  });
});
