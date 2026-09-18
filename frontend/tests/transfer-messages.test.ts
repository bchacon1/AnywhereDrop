import { describe, expect, it } from "vitest";
import {
  HEADER_SIZE,
  chunkPayload,
  decodeChunkHeader,
  decodeControl,
  encodeChunk,
  encodeControl,
  expectedPayloadLength,
} from "../src/transfer/messages";

describe("chunk frame (transfer-protocol.md §3.2)", () => {
  it("round-trips a header and payload", () => {
    const payload = new Uint8Array([1, 2, 3, 4, 5]).buffer;
    const frame = encodeChunk(0xdeadbeef, 42, payload);
    expect(frame.byteLength).toBe(HEADER_SIZE + 5);
    const h = decodeChunkHeader(frame)!;
    expect(h).toEqual({
      version: 1,
      flags: 0,
      reserved: 0,
      transferId: 0xdeadbeef,
      chunkIndex: 42,
    });
    expect([...new Uint8Array(chunkPayload(frame))]).toEqual([1, 2, 3, 4, 5]);
  });

  it("is big-endian with the documented byte layout", () => {
    const frame = new Uint8Array(encodeChunk(0x01020304, 0x0a0b0c0d, new ArrayBuffer(0)));
    expect([...frame]).toEqual([1, 0, 0, 0, 0x01, 0x02, 0x03, 0x04, 0x0a, 0x0b, 0x0c, 0x0d]);
  });

  it("rejects short buffers", () => {
    expect(decodeChunkHeader(new ArrayBuffer(11))).toBeNull();
  });

  it("computes the last chunk length", () => {
    expect(expectedPayloadLength(100, 30, 0, 4)).toBe(30);
    expect(expectedPayloadLength(100, 30, 3, 4)).toBe(10);
    expect(expectedPayloadLength(90, 30, 2, 3)).toBe(30);
  });
});

describe("control messages", () => {
  it("round-trips JSON and rejects junk", () => {
    const m = encodeControl({ type: "ACK", transferId: 7, committedOffset: 1024 });
    expect(decodeControl(m)).toEqual({ type: "ACK", transferId: 7, committedOffset: 1024 });
    expect(decodeControl("not json")).toBeNull();
    expect(decodeControl('{"x":1}')).toBeNull();
  });
});
