// Minimal RFC 6455 WebSocket framing (server side), zero dependencies.
// Supports: text/binary frames, fragmentation via fin, ping/pong, close,
// and client frames masked with a 4-byte key. Sufficient for browser clients.

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const crypto = require('node:crypto');

function acceptKey(key) {
  return crypto.createHash('sha1').update(key + GUID).digest('base64');
}

class FrameError extends Error {
  constructor(message, code = 1002) {
    super(message);
    this.code = code;
  }
}

/**
 * Parse as many complete frames as possible from a Buffer.
 * Returns { frames, rest }. Each frame: { fin, opcode, payload(Buffer) }.
 * Control frames (close/ping/pong) must not be fragmented per RFC.
 */
function parseFrames(buf) {
  const frames = [];
  let offset = 0;

  while (buf.length - offset >= 2) {
    const b0 = buf[offset];
    const b1 = buf[offset + 1];
    const fin = (b0 & 0x80) === 0x80;
    const opcode = b0 & 0x0f;
    const masked = (b1 & 0x80) === 0x80;
    let len = b1 & 0x7f;
    let headerLen = 2;

    if (len === 126) {
      if (buf.length - offset < 4) break;
      len = buf.readUInt16BE(offset + 2);
      headerLen = 4;
    } else if (len === 127) {
      if (buf.length - offset < 10) break;
      len = Number(buf.readBigUInt64BE(offset + 2));
      headerLen = 10;
    }

    let maskKey;
    if (masked) {
      if (buf.length - offset < headerLen + 4) break;
      maskKey = buf.subarray(offset + headerLen, offset + headerLen + 4);
      headerLen += 4;
    }

    if (buf.length - offset < headerLen + len) break;

    const payload = Buffer.from(buf.subarray(offset + headerLen, offset + headerLen + len));
    if (masked) {
      for (let i = 0; i < payload.length; i++) payload[i] ^= maskKey[i % 4];
    }

    if (!fin && opcode >= 0x8) {
      throw new FrameError('control frame must not be fragmented', 1002);
    }
    if (payload.length > 65535 && opcode >= 0x8) {
      throw new FrameError('control payload too large', 1002);
    }

    frames.push({ fin, opcode, payload });
    offset += headerLen + len;
  }

  return { frames, rest: Buffer.from(buf.subarray(offset)) };
}

function buildFrame(data, { opcode = 0x1, fin = true } = {}) {
  const payload = Buffer.isBuffer(data) ? data : Buffer.from(data);
  const len = payload.length;
  let header;
  if (len < 126) {
    header = Buffer.alloc(2);
    header[1] = len;
  } else if (len <= 0xffff) {
    header = Buffer.alloc(4);
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  header[0] = (fin ? 0x80 : 0) | opcode;
  return Buffer.concat([header, payload]);
}

function buildCloseFrame(code, reason) {
  const body = Buffer.alloc(2 + Buffer.byteLength(reason ?? ''));
  body.writeUInt16BE(code ?? 1000, 0);
  if (reason) body.write(reason, 2);
  return buildFrame(body, { opcode: 0x8 });
}

module.exports = { acceptKey, parseFrames, buildFrame, buildCloseFrame, FrameError };
