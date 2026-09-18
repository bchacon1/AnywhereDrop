import { createHash, randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";
import { Sha256, sha256Hex } from "../src/workers/sha256";

const enc = (s: string) => new TextEncoder().encode(s);
const node = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");

describe("Sha256 (FIPS 180-4 vectors)", () => {
  it("empty string", () => {
    expect(sha256Hex(new Uint8Array(0))).toBe(
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    );
  });
  it("abc", () => {
    expect(sha256Hex(enc("abc"))).toBe(
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
    );
  });
  it("two-block message", () => {
    expect(sha256Hex(enc("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"))).toBe(
      "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1",
    );
  });
  it("one million 'a' streamed in odd-sized pieces", () => {
    const h = new Sha256();
    const piece = enc("a".repeat(997));
    let n = 0;
    while (n + 997 <= 1_000_000) {
      h.update(piece);
      n += 997;
    }
    h.update(enc("a".repeat(1_000_000 - n)));
    expect(h.digestHex()).toBe("cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
  });
  it("matches Node's crypto for random inputs at block boundaries and beyond", () => {
    for (const len of [1, 55, 56, 57, 63, 64, 65, 119, 120, 128, 1000, 65536, 1_000_003]) {
      const data = randomBytes(len);
      expect(sha256Hex(data)).toBe(node(data));
    }
  });
  it("incremental equals one-shot regardless of chunking", () => {
    const data = randomBytes(200_000);
    const expected = node(data);
    for (const step of [1, 7, 64, 100, 4096, 65537]) {
      const h = new Sha256();
      for (let i = 0; i < data.length; i += step)
        h.update(data.subarray(i, Math.min(i + step, data.length)));
      expect(h.digestHex()).toBe(expected);
    }
  });
  it("refuses updates after digest and returns the same digest twice", () => {
    const h = new Sha256().update(enc("x"));
    const d1 = h.digestHex();
    expect(h.digestHex()).toBe(d1);
    expect(() => h.update(enc("y"))).toThrow();
  });
});
