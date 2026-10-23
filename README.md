# @c0dingbear/chromium-idb-reader

Pure-JavaScript reader for **Chromium IndexedDB LevelDB** stores — the
`idb_cmp1`-comparator databases that `level` / `leveldown` refuse to open
("comparator does not match"). No native dependencies.

It reads the on-disk format directly:

- parses the write-ahead log (`.log`) and the SSTables (`.ldb`),
- decompresses **Snappy** blocks (bundled pure-JS decoder),
- applies **last-write-wins** by global sequence number,
- returns each current record's raw value **and its sequence number**.

Decoding the IndexedDB key format and the encrypted/serialized values is left to
the caller — this package gives you the current key/value bytes to work with.

## Usage

```js
const { readCurrentRecords } = require("@c0dingbear/chromium-idb-reader");

// dbPath = the "...IndexedDB/<origin>.indexeddb.leveldb" directory
for (const { key, value, seq } of readCurrentRecords(dbPath)) {
  // key:   Buffer (the LevelDB user key — decode the IDB prefix yourself)
  // value: Buffer (the record value)
  // seq:   BigInt (higher = written more recently)
}
```

`snappyUncompress(buf)` is also exported if you need the raw decoder.

## Why

At the time of writing there was no maintained pure-JS npm package that could
read Chrome/Electron IndexedDB LevelDB stores; the options were Python
(`ccl_chromium_reader`, `dfindexeddb`) or Go (`leveldb-cli`). This fills that
gap for the Node ecosystem.

## License

MIT
