import { useEffect, useState } from "react";
import { usePeerLink } from "./usePeerLink";
import { useTransfer } from "./useTransfer";
import { useSignaling } from "./useSignaling";
import type { Channel } from "../webrtc/types";

/**
 * Phase 5 UI: create or join a room, connect a DataChannel, transfer a file
 * from the creator to the joiner with bounded processing, and verify its
 * SHA-256 before offering the download.
 */
export function App() {
  const { state, actions, client } = useSignaling();
  const link = usePeerLink(client, state);
  const attached = state.attachment !== null;

  return (
    <main className="app">
      <h1>AnywhereDrop</h1>
      <p className="muted">
        Browser-to-browser file transfer. Phase 5: verified transfer over the DataChannel.
      </p>
      {!attached ? (
        <Home onCreate={actions.createRoom} onJoin={actions.joinRoom} state={state} />
      ) : (
        <RoomView
          state={state}
          onRelay={actions.relay}
          onDrop={actions.dropSocket}
          onSync={actions.syncRequest}
          onLeave={actions.leave}
        />
      )}
      {attached && <TransferView role={state.attachment!.role} channel={link.channel} />}
      {attached && <LinkView link={link} />}
      <section className="card">
        <h2>Signaling log</h2>
        <div className="log mono" data-testid="signaling-log">
          {state.log.map((line, i) => (
            <div key={i}>{line}</div>
          ))}
        </div>
      </section>
      {attached && (
        <section className="card">
          <h2>WebRTC log</h2>
          <div className="log mono" data-testid="webrtc-log">
            {link.log.map((line, i) => (
              <div key={i}>{line}</div>
            ))}
          </div>
        </section>
      )}
    </main>
  );
}

function Home(props: {
  onCreate: () => void;
  onJoin: (code: string) => void;
  state: ReturnType<typeof useSignaling>["state"];
}) {
  const [code, setCode] = useState("");
  const busy = props.state.status === "connecting";
  return (
    <section className="card">
      <div className="row">
        <button onClick={props.onCreate} disabled={busy}>
          Send (create a room)
        </button>
      </div>
      <p className="muted">or</p>
      <div className="row">
        <input
          className="mono"
          placeholder="ROOM CODE"
          value={code}
          onChange={(e) => setCode(e.target.value.toUpperCase())}
          maxLength={6}
          aria-label="room code"
        />
        <button
          className="secondary"
          onClick={() => props.onJoin(code)}
          disabled={busy || code.length < 6}
        >
          Receive (join)
        </button>
      </div>
      {props.state.lastError && (
        <p className="bad">
          {props.state.lastError.code}: {props.state.lastError.message}
        </p>
      )}
      {props.state.closedReason && <p className="bad">room closed: {props.state.closedReason}</p>}
    </section>
  );
}

function RoomView(props: {
  state: ReturnType<typeof useSignaling>["state"];
  onRelay: (payload: unknown) => boolean;
  onDrop: () => void;
  onSync: () => void;
  onLeave: () => void;
}) {
  const { state } = props;
  const [text, setText] = useState("");
  const a = state.attachment!;
  const peerLine = state.peer
    ? state.peer.connected
      ? `peer ${state.peer.peerId.slice(0, 6)} connected`
      : `peer ${state.peer.peerId.slice(0, 6)} disconnected (may reattach)`
    : "waiting for a peer to join";

  return (
    <>
      <section className="card">
        <h2>Room</h2>
        {a.role === "creator" && (
          <p>
            Share this code:{" "}
            <span className="code-display" data-testid="room-code">
              {a.code}
            </span>
          </p>
        )}
        <p>
          You are the <b>{a.role}</b> · attachment gen {a.gen} · socket{" "}
          <span className={state.status === "attached" ? "good" : "bad"} data-testid="status">
            {state.status}
          </span>
        </p>
        <p data-testid="peer">{peerLine}</p>
        {state.closedReason && <p className="bad">room closed: {state.closedReason}</p>}
      </section>
      <section className="card">
        <h2>Text relay (through the backend)</h2>
        <div className="row">
          <input
            value={text}
            onChange={(e) => setText(e.target.value)}
            placeholder="message"
            aria-label="relay message"
          />
          <button
            onClick={() => {
              if (props.onRelay({ text })) setText("");
            }}
            disabled={!state.peer?.connected}
          >
            Send via relay
          </button>
        </div>
      </section>
      <section className="card">
        <h2>Dev hooks</h2>
        <div className="row">
          <button className="secondary" onClick={props.onDrop} data-testid="drop-socket">
            Drop socket (ws_close)
          </button>
          <button className="secondary" onClick={props.onSync}>
            sync_request
          </button>
          <button className="secondary" onClick={props.onLeave}>
            Leave
          </button>
        </div>
      </section>
    </>
  );
}

function LinkView(props: { link: ReturnType<typeof usePeerLink> }) {
  const { state, channel, restartIce } = props.link;
  const [text, setText] = useState("");
  const [received, setReceived] = useState<string[]>([]);

  useEffect(() => {
    if (!channel) return;
    channel.onMessage((data) => {
      // Protocol control messages are JSON objects; the text panel shows only plain text.
      if (typeof data === "string" && !data.startsWith("{"))
        setReceived((r) => [...r.slice(-49), data]);
    });
  }, [channel]);

  const stats = state?.stats;
  const open = channel !== null && channel.readyState === "open";
  return (
    <>
      <section className="card">
        <h2>WebRTC connection</h2>
        <table className="stats">
          <tbody>
            <tr>
              <td>phase</td>
              <td data-testid="link-phase">{state?.phase ?? "idle"}</td>
            </tr>
            <tr>
              <td>epoch</td>
              <td data-testid="link-epoch">{state?.epoch ?? 0}</td>
            </tr>
            <tr>
              <td>connectionState</td>
              <td data-testid="connection-state">{stats?.connectionState ?? "new"}</td>
            </tr>
            <tr>
              <td>iceConnectionState</td>
              <td>{stats?.iceConnectionState ?? "new"}</td>
            </tr>
            <tr>
              <td>candidate type</td>
              <td data-testid="candidate-type">{stats?.candidateType ?? "unknown"}</td>
            </tr>
            <tr>
              <td>RTT</td>
              <td>
                {stats?.rttMs === null || stats?.rttMs === undefined ? "–" : `${stats.rttMs} ms`}
              </td>
            </tr>
            <tr>
              <td>maxMessageSize</td>
              <td data-testid="max-message-size">{stats?.maxMessageSize ?? 0}</td>
            </tr>
            <tr>
              <td>channel</td>
              <td data-testid="channel-state">
                {channel ? `${channel.readyState} (generation ${channel.generation})` : "none"}
              </td>
            </tr>
          </tbody>
        </table>
        {state?.lastError && <p className="bad">{state.lastError}</p>}
      </section>
      <section className="card">
        <h2>Text over the DataChannel (direct, not through the backend)</h2>
        <div className="row">
          <input
            value={text}
            onChange={(e) => setText(e.target.value)}
            placeholder="message"
            aria-label="datachannel message"
          />
          <button
            onClick={() => {
              if (channel && open) {
                channel.send(text);
                setText("");
              }
            }}
            disabled={!open}
          >
            Send via DataChannel
          </button>
          <button className="secondary" onClick={restartIce} data-testid="ice-restart">
            ice_restart
          </button>
        </div>
        <div className="log mono" data-testid="dc-received">
          {received.map((r, i) => (
            <div key={i}>{r}</div>
          ))}
        </div>
      </section>
    </>
  );
}

// Keep the Channel type referenced for readers of this file: the UI never calls
// anything on it except send/onMessage/readyState/generation.
export type { Channel };

function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MiB`;
}

function TransferView(props: { role: "creator" | "joiner"; channel: Channel | null }) {
  const tv = useTransfer(props.role, props.channel);
  const open = props.channel !== null && props.channel.readyState === "open";
  const [now, setNow] = useState(0);
  useEffect(() => {
    const h = setInterval(() => setNow(performance.now()), 250);
    return () => clearInterval(h);
  }, []);

  if (props.role === "creator") {
    const s = tv.sender;
    const elapsed = s?.timing.t_accept !== undefined ? (now - s.timing.t_accept) / 1000 : 0;
    const rate = s && elapsed > 0 ? s.nextOffset / elapsed : 0;
    return (
      <section className="card">
        <h2>Send a file</h2>
        <div className="row">
          <input
            type="file"
            aria-label="file to send"
            data-testid="file-input"
            disabled={!open || (s !== null && s.state !== "idle" && !isTerminal(s.state))}
            onChange={(e) => {
              const f = e.target.files?.[0];
              if (f) tv.sendFile(f);
            }}
          />
          <button
            className="secondary"
            onClick={tv.cancel}
            disabled={!s || isTerminal(s.state) || s.state === "idle"}
          >
            Cancel
          </button>
        </div>
        {s && (
          <>
            <p>
              state <b data-testid="sender-state">{s.state}</b>
              {s.error && (
                <span className="bad">
                  {" "}
                  · {s.error.code}: {s.error.message}
                </span>
              )}
            </p>
            <progress value={s.nextOffset} max={s.size || 1} />
            <table className="stats">
              <tbody>
                <tr>
                  <td>enqueued</td>
                  <td data-testid="sender-enqueued">{fmtBytes(s.nextOffset)}</td>
                  <td>of {fmtBytes(s.size)}</td>
                </tr>
                <tr>
                  <td>acked</td>
                  <td>{fmtBytes(s.ackedOffset)}</td>
                </tr>
                <tr>
                  <td>enqueue rate</td>
                  <td>{s.state === "transferring" ? `${fmtBytes(rate)}/s` : "–"}</td>
                </tr>
                <tr>
                  <td>bufferedAmount</td>
                  <td data-testid="buffered-amount">{fmtBytes(s.bufferedAmount)}</td>
                </tr>
                <tr>
                  <td>stalls (limit 1 / limit 2)</td>
                  <td data-testid="stalls">
                    {s.stallsLimit1} / {s.stallsLimit2}
                  </td>
                </tr>
                <tr>
                  <td>chunks</td>
                  <td>{s.chunksSent}</td>
                </tr>
                <tr>
                  <td>sha256</td>
                  <td className="mono" data-testid="sender-sha256">
                    {s.sha256 || "–"}
                  </td>
                </tr>
                <tr>
                  <td>timing</td>
                  <td className="mono" data-testid="sender-timing">
                    {Object.entries(s.timing)
                      .map(([k, v]) => `${k}=${Math.round(v as number)}`)
                      .join(" ")}
                  </td>
                </tr>
              </tbody>
            </table>
          </>
        )}
        <p className="muted">
          limits: chunk {fmtBytes(tv.limits.chunkSize)} · high {fmtBytes(tv.limits.highWater)} · low{" "}
          {fmtBytes(tv.limits.lowWater)} · window {fmtBytes(tv.limits.window)} · ack every{" "}
          {fmtBytes(tv.limits.ackInterval)} · max file {fmtBytes(tv.limits.maxFileSize)}
          {tv.limits.disableLimit1 && " · LIMIT 1 DISABLED"}
          {tv.limits.disableLimit2 && " · LIMIT 2 DISABLED"}
        </p>
        <div className="log mono" data-testid="transfer-log">
          {tv.log.map((l, i) => (
            <div key={i}>{l}</div>
          ))}
        </div>
      </section>
    );
  }

  const r = tv.receiver;
  const elapsed = r?.timing.t_accept !== undefined ? (now - r.timing.t_accept) / 1000 : 0;
  const rate = r && elapsed > 0 ? r.committedOffset / elapsed : 0;
  return (
    <section className="card">
      <h2>Receive a file</h2>
      {tv.offer && r?.state === "offered" && (
        <div className="row">
          <span data-testid="offer-name">
            {tv.offer.name} ({fmtBytes(tv.offer.size)})
          </span>
          <button onClick={tv.accept} data-testid="accept">
            Accept
          </button>
          <button className="secondary" onClick={tv.reject}>
            Reject
          </button>
        </div>
      )}
      {r && (
        <>
          <p>
            state <b data-testid="receiver-state">{r.state}</b>
            {r.error && (
              <span className="bad">
                {" "}
                · {r.error.code}: {r.error.message}
              </span>
            )}
          </p>
          <progress value={r.committedOffset} max={r.offer?.size || 1} />
          <table className="stats">
            <tbody>
              <tr>
                <td>committed</td>
                <td data-testid="receiver-committed">{fmtBytes(r.committedOffset)}</td>
                <td>of {fmtBytes(r.offer?.size ?? 0)}</td>
              </tr>
              <tr>
                <td>rate (committed)</td>
                <td>{r.state === "transferring" ? `${fmtBytes(rate)}/s` : "–"}</td>
              </tr>
              <tr>
                <td>accepted chunks / duplicates</td>
                <td>
                  {r.expectedIndex} / {r.duplicates}
                </td>
              </tr>
              <tr>
                <td>worker backlog (chunks)</td>
                <td data-testid="backlog">
                  {r.offer
                    ? Math.max(
                        0,
                        r.expectedIndex - Math.ceil(r.committedOffset / r.offer.chunkSize),
                      )
                    : 0}
                </td>
              </tr>
              <tr>
                <td>ACKs sent</td>
                <td>{r.acksSent}</td>
              </tr>
              <tr>
                <td>verified</td>
                <td data-testid="verified">
                  {r.verified === null
                    ? "–"
                    : r.verified
                      ? "yes"
                      : r.error
                        ? "FAILED"
                        : "no (no hash)"}
                </td>
              </tr>
              <tr>
                <td>sha256</td>
                <td className="mono" data-testid="receiver-sha256">
                  {r.sha256 || "–"}
                </td>
              </tr>
              <tr>
                <td>timing</td>
                <td className="mono" data-testid="receiver-timing">
                  {Object.entries(r.timing)
                    .map(([k, v]) => `${k}=${Math.round(v as number)}`)
                    .join(" ")}
                </td>
              </tr>
            </tbody>
          </table>
          {tv.result && r.state === "done" && (
            <p>
              <a href={tv.result.url} download={tv.result.name} data-testid="download-link">
                Save {tv.result.name}
              </a>
            </p>
          )}
          <div className="row">
            <button
              className="secondary"
              onClick={tv.cancel}
              disabled={isTerminal(r.state) || r.state === "idle"}
            >
              Cancel
            </button>
          </div>
        </>
      )}
      <div className="log mono" data-testid="transfer-log">
        {tv.log.map((l, i) => (
          <div key={i}>{l}</div>
        ))}
      </div>
    </section>
  );
}

function isTerminal(s: string): boolean {
  return s === "done" || s === "failed" || s === "cancelled" || s === "rejected";
}
