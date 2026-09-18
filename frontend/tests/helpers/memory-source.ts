import type { FileSource } from "../../src/transfer/interfaces";

/** A deterministic in-memory file: byte i = (i * 7 + 3) & 0xff. */
export function memorySource(size: number, name = "test.bin"): FileSource & { bytes: Uint8Array } {
  const bytes = new Uint8Array(size);
  for (let i = 0; i < size; i++) bytes[i] = (i * 7 + 3) & 0xff;
  return {
    bytes,
    size,
    name,
    type: "application/octet-stream",
    lastModified: 1700000000000,
    read(offset, length) {
      return Promise.resolve(bytes.slice(offset, offset + length).buffer);
    },
  };
}
