// Transfer protocol wire format: control messages (JSON text frames) and the
// binary chunk frame. transfer-protocol.md §3. Pure: no browser globals.

export const PROTOCOL_VERSION = 1;
export const HEADER_SIZE = 12;

export type ControlMessage =
  | {
      type: "FILE_OFFER";
      transferId: number;
      name: string;
      size: number;
      mime: string;
      lastModified: number;
      chunkSize: number;
      totalChunks: number;
      protocolVersion: number;
    }
  | { type: "FILE_ACCEPT"; transferId: number }
  | { type: "FILE_REJECT"; transferId: number; reason: string }
  | { type: "ACK"; transferId: number; committedOffset: number }
  | { type: "COMPLETE"; transferId: number; sha256: string }
  | {
      type: "COMPLETE_ACK";
      transferId: number;
      ok: boolean;
      committedOffset: number;
      sha256: string;
    }
  | { type: "RESUME"; transferId: number; committedOffset: number }
  | { type: "RESUME_OK"; transferId: number; from: number }
  | { type: "RESUME_REJECT"; transferId: number; reason: string }
  | { type: "CANCEL"; transferId: number; reason: string }
  | { type: "ERROR"; transferId?: number; code: ErrorCode; message: string };

export type ErrorCode =
  | "unsupported_version"
  | "too_large"
  | "bad_frame"
  | "out_of_sequence"
  | "sink_failure"
  | "short_write"
  | "read_failure"
  | "send_failure"
  | "hash_mismatch"
  | "invalid_offset"
  | "unknown_transfer"
  | "channel_lost"
  | "internal";

export function encodeControl(msg: ControlMessage): string {
  return JSON.stringify(msg);
}

export function decodeControl(text: string): ControlMessage | null {
  try {
    const v = JSON.parse(text) as { type?: unknown };
    if (typeof v !== "object" || v === null || typeof v.type !== "string") return null;
    return v as ControlMessage;
  } catch {
    return null;
  }
}

/** Chunk frame header (big-endian): version u8, flags u8, reserved u16, transferId u32, chunkIndex u32. */
export interface ChunkHeader {
  version: number;
  flags: number;
  reserved: number;
  transferId: number;
  chunkIndex: number;
}

export function encodeChunk(
  transferId: number,
  chunkIndex: number,
  payload: ArrayBuffer,
): ArrayBuffer {
  const out = new ArrayBuffer(HEADER_SIZE + payload.byteLength);
  const view = new DataView(out);
  view.setUint8(0, PROTOCOL_VERSION);
  view.setUint8(1, 0);
  view.setUint16(2, 0);
  view.setUint32(4, transferId >>> 0);
  view.setUint32(8, chunkIndex >>> 0);
  new Uint8Array(out, HEADER_SIZE).set(new Uint8Array(payload));
  return out;
}

/** Parses the header. Returns null if the buffer is shorter than a header. */
export function decodeChunkHeader(buf: ArrayBuffer): ChunkHeader | null {
  if (buf.byteLength < HEADER_SIZE) return null;
  const view = new DataView(buf);
  return {
    version: view.getUint8(0),
    flags: view.getUint8(1),
    reserved: view.getUint16(2),
    transferId: view.getUint32(4),
    chunkIndex: view.getUint32(8),
  };
}

/** Returns the payload as its own ArrayBuffer (copy), so the header is not retained. */
export function chunkPayload(buf: ArrayBuffer): ArrayBuffer {
  return buf.slice(HEADER_SIZE);
}

export function expectedPayloadLength(
  size: number,
  chunkSize: number,
  chunkIndex: number,
  totalChunks: number,
): number {
  if (chunkIndex === totalChunks - 1) return size - chunkIndex * chunkSize;
  return chunkSize;
}
