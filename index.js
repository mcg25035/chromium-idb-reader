// Minimal pure-JS LevelDB reader for Chromium IndexedDB stores.
//
// Standard `level`/`classic-level` refuse to open these because Chromium uses a
// custom comparator ("idb_cmp1"). We don't need the comparator: we read the raw
// on-disk format (.log write-ahead log + .ldb SSTables), collect every
// (user_key, sequence, type, value) record, and keep the highest-sequence
// (newest) value per IndexedDB record key.
//
// Why sequence numbers matter: LevelDB is append-only. A record that was edited
// over time (e.g. a host whose saved address changed) leaves old versions on
// disk. The current value is the write with the greatest global sequence
// number. We also surface that sequence to callers, so that among sibling
// records (e.g. a host's per-connection session logs) the most recently written
// one — the current resolved address — can be identified.

const fs = require('fs');
const path = require('path');

// ---- Snappy decompression (block format) ---------------------------------
function snappyUncompress(input) {
  let ip = 0;
  let len = 0, shift = 0;
  for (;;) {
    const c = input[ip++];
    len |= (c & 0x7f) << shift;
    if (!(c & 0x80)) break;
    shift += 7;
  }
  len >>>= 0;
  const out = Buffer.allocUnsafe(len);
  let op = 0;
  while (ip < input.length) {
    const tag = input[ip];
    const t = tag & 0x03;
    if (t === 0) {
      let litLen = (tag >> 2) + 1;
      ip++;
      if (litLen > 60) {
        const bytes = litLen - 60;
        litLen = 0;
        for (let i = 0; i < bytes; i++) litLen |= input[ip + i] << (8 * i);
        litLen = (litLen >>> 0) + 1;
        ip += bytes;
      }
      input.copy(out, op, ip, ip + litLen);
      op += litLen;
      ip += litLen;
    } else {
      let offset, copyLen;
      if (t === 1) {
        copyLen = ((tag >> 2) & 0x07) + 4;
        offset = ((tag >> 5) << 8) | input[ip + 1];
        ip += 2;
      } else if (t === 2) {
        copyLen = (tag >> 2) + 1;
        offset = input[ip + 1] | (input[ip + 2] << 8);
        ip += 3;
      } else {
        copyLen = (tag >> 2) + 1;
        offset = (input[ip + 1] | (input[ip + 2] << 8) | (input[ip + 3] << 16) | (input[ip + 4] << 24)) >>> 0;
        ip += 5;
      }
      let src = op - offset;
      for (let i = 0; i < copyLen; i++) out[op++] = out[src++];
    }
  }
  return out;
}

function readVarint32(buf, off) {
  let result = 0, shift = 0, p = off;
  for (;;) {
    const c = buf[p++];
    result |= (c & 0x7f) << shift;
    if (!(c & 0x80)) break;
    shift += 7;
  }
  return [result >>> 0, p];
}

function readVarint64(buf, off) {
  let result = 0n, shift = 0n, p = off;
  for (;;) {
    const c = buf[p++];
    result |= BigInt(c & 0x7f) << shift;
    if (!(c & 0x80)) break;
    shift += 7n;
  }
  return [result, p];
}

// ---- SSTable (.ldb/.sst) --------------------------------------------------
function readBlock(file, offset, size) {
  const raw = file.slice(offset, offset + size);
  const compType = file[offset + size];
  if (compType === 0) return raw;
  if (compType === 1) return snappyUncompress(raw);
  throw new Error('unsupported block compression type ' + compType);
}

function* blockEntries(block) {
  const numRestarts = block.readUInt32LE(block.length - 4);
  const restartsStart = block.length - 4 - numRestarts * 4;
  let p = 0;
  let lastKey = Buffer.alloc(0);
  while (p < restartsStart) {
    let shared, nonShared, valLen;
    [shared, p] = readVarint32(block, p);
    [nonShared, p] = readVarint32(block, p);
    [valLen, p] = readVarint32(block, p);
    const keyDelta = block.slice(p, p + nonShared);
    p += nonShared;
    const value = block.slice(p, p + valLen);
    p += valLen;
    const key = Buffer.concat([lastKey.slice(0, shared), keyDelta]);
    lastKey = key;
    yield { key, value };
  }
}

function readSSTable(file, out) {
  const footer = file.slice(file.length - 48);
  const magic = footer.slice(40);
  if (magic.toString('hex') !== '57fb808b247547db') return;
  let p = 0;
  let mOff, mSize, iOff, iSize;
  [mOff, p] = readVarint64(footer, p); [mSize, p] = readVarint64(footer, p);
  [iOff, p] = readVarint64(footer, p); [iSize, p] = readVarint64(footer, p);
  const indexBlock = readBlock(file, Number(iOff), Number(iSize));
  for (const { value } of blockEntries(indexBlock)) {
    const [dOff, q1] = readVarint64(value, 0);
    const [dSize] = readVarint64(value, q1);
    let dataBlock;
    try { dataBlock = readBlock(file, Number(dOff), Number(dSize)); } catch (e) { continue; }
    for (const { key, value: v } of blockEntries(dataBlock)) {
      if (key.length < 8) continue;
      const userKey = key.slice(0, key.length - 8);
      const trailer = key.readBigUInt64LE(key.length - 8);
      const type = Number(trailer & 0xffn);
      const seq = trailer >> 8n;
      out.push({ userKey, seq, type, value: v });
    }
  }
}

// ---- Write-ahead log (.log) ----------------------------------------------
const LOG_BLOCK = 32768;

function readLog(file, out) {
  let pending = [];
  const flushBatch = (buf) => {
    let seq = buf.readBigUInt64LE(0);
    const count = buf.readUInt32LE(8);
    let p = 12;
    for (let i = 0; i < count; i++) {
      const type = buf[p++];
      let kLen; [kLen, p] = readVarint32(buf, p);
      const userKey = buf.slice(p, p + kLen); p += kLen;
      let value = Buffer.alloc(0);
      if (type === 1) {
        let vLen; [vLen, p] = readVarint32(buf, p);
        value = buf.slice(p, p + vLen); p += vLen;
      }
      out.push({ userKey, seq, type, value });
      seq += 1n;
    }
  };
  let pos = 0;
  while (pos + 7 <= file.length) {
    const blockEnd = Math.min(pos + LOG_BLOCK, file.length);
    let bp = pos;
    while (bp + 7 <= blockEnd) {
      const length = file.readUInt16LE(bp + 4);
      const rtype = file[bp + 6];
      const dStart = bp + 7;
      if (rtype === 0 || dStart + length > blockEnd) break;
      const data = file.slice(dStart, dStart + length);
      if (rtype === 1) { try { flushBatch(data); } catch (e) {} pending = []; }
      else if (rtype === 2) { pending = [data]; }
      else if (rtype === 3) { pending.push(data); }
      else if (rtype === 4) { pending.push(data); try { flushBatch(Buffer.concat(pending)); } catch (e) {} pending = []; }
      bp = dStart + length;
    }
    pos = blockEnd;
  }
}

// ---- Public ---------------------------------------------------------------
// Returns current (highest-sequence) records: [{ value: Buffer, seq: BigInt }].
// Superseded versions and tombstoned deletions are dropped.
function readCurrentRecords(dbPath) {
  const files = fs.readdirSync(dbPath);
  const records = [];
  for (const name of files) {
    if (name.endsWith('.ldb') || name.endsWith('.sst')) {
      try { readSSTable(fs.readFileSync(path.join(dbPath, name)), records); } catch (e) {}
    }
  }
  for (const name of files) {
    if (name.endsWith('.log')) {
      try { readLog(fs.readFileSync(path.join(dbPath, name)), records); } catch (e) {}
    }
  }
  const winner = new Map();
  for (const r of records) {
    const k = r.userKey.toString('latin1');
    const prev = winner.get(k);
    if (!prev || r.seq > prev.seq) winner.set(k, r);
  }
  const out = [];
  for (const r of winner.values()) {
    if (r.type === 1) out.push({ key: r.userKey, value: r.value, seq: r.seq });
  }
  return out;
}

module.exports = { readCurrentRecords, snappyUncompress };
