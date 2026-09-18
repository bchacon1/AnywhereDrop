// FileSource over a browser File: reads slices lazily so the sender never
// holds the whole file in memory (transfer-protocol.md §4.2).

import type { FileSource } from "../transfer/interfaces";

export function fileSource(file: File): FileSource {
  return {
    size: file.size,
    name: file.name,
    type: file.type,
    lastModified: file.lastModified,
    read(offset, length) {
      return file.slice(offset, offset + length).arrayBuffer();
    },
  };
}
