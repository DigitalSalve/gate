// The one helper the meter needs from the host's utilities; the host re-exports it.

/** Decoded byte length of a base64 string, without decoding it. */
export function base64Bytes(b64: string): number {
  if (b64.length === 0) return 0;
  let padding = 0;
  if (b64.endsWith("==")) padding = 2;
  else if (b64.endsWith("=")) padding = 1;
  return Math.floor((b64.length * 3) / 4) - padding;
}
