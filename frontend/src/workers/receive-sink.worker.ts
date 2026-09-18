// Worker entry: wires postMessage to SinkCore. Kept to a few lines so the
// logic is testable in Node (sink-core.ts). The hasher is the incremental
// SHA-256 from sha256.ts, fed in commit order (transfer-protocol.md §4.4).

import { sha256Hasher } from "./sha256";
import { SinkCore, type SinkReply, type SinkRequest } from "./sink-core";

const core = new SinkCore({
  post: (reply: SinkReply) => {
    // A Blob result is structured-cloned; nothing here needs a transfer list.
    (self as unknown as { postMessage: (m: SinkReply) => void }).postMessage(reply);
  },
  makeHasher: sha256Hasher,
});

self.onmessage = (ev: MessageEvent<SinkRequest>) => {
  void core.handle(ev.data);
};
