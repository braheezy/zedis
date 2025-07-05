# Zedis

A Redis-like server written in Zig. Features:

- String operations (GET/SET/DEL)
- Sorted sets (ZADD/ZREM/ZSCORE/ZQUERY)
- Key expiration (PEXPIRE/PTTL)
- Thread pool for handling large data structure cleanup
- Non-blocking I/O using poll()
- AVL tree implementation for sorted sets
- Lock-free hash table with incremental rehashing

## Building

```bash
zig build
```

## Running

```bash
./zig-out/bin/zedis    # Server
./zig-out/bin/client   # Test client
```

## Testing

```bash
zig build test
python3 test_cmds.py   # Integration tests against a running server
```

Based on the [Build Your Own Redis](https://build-your-own.org/redis) tutorial, but implemented in Zig.
