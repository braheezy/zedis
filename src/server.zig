const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const root = @import("root.zig");
const readAll = root.readAll;
const writeAll = root.writeAll;
const max_msg_size = root.max_msg_size;
const max_args = 200 * 1000;
const idle_timeout_ms = 5 * 1000;
const Conn = @import("Conn.zig").Conn;
const Buffer = @import("Buffer.zig");
const hash = @import("hash.zig");
const DataType = root.DataType;
const ErrorCode = root.ErrorCode;
const ValueType = root.ValueType;
const zset = @import("zset.zig");
const Dlist = @import("Dlist.zig");
const heap = @import("heap.zig");
const ThreadPool = @import("threadPool.zig").ThreadPool;

var debug_allocator: std.heap.DebugAllocator(.{}) = .init;

pub fn main() !void {
    // Memory allocation setup
    const allocator, const is_debug = gpa: {
        if (builtin.os.tag == .wasi) break :gpa .{ std.heap.wasm_allocator, false };
        break :gpa switch (builtin.mode) {
            .Debug, .ReleaseSafe => .{ debug_allocator.allocator(), true },
            .ReleaseFast, .ReleaseSmall => .{ std.heap.smp_allocator, false },
        };
    };
    defer {
        if (is_debug) {
            if (debug_allocator.deinit() == .leak) {
                std.process.exit(1);
            }
        }
    }

    g_data = .{
        .conns = std.AutoHashMap(std.posix.socket_t, *Conn).init(allocator),
        .idle_list = .{},
        .thread_pool = try ThreadPool.init(allocator, 4),
    };
    Dlist.init(&g_data.idle_list);
    defer g_data.conns.deinit();

    const fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
    defer std.posix.close(fd);

    try std.posix.setsockopt(
        fd,
        std.posix.SOL.SOCKET,
        std.posix.SO.REUSEADDR,
        &std.mem.toBytes(@as(c_int, 1)),
    );

    const addr = std.posix.sockaddr.in{
        .family = std.posix.AF.INET,
        .port = std.mem.nativeToBig(u16, 1234),
        .addr = std.mem.nativeToBig(u32, 0),
    };

    try std.posix.bind(
        fd,
        @as(*const std.posix.sockaddr, @ptrCast(&addr)),
        @sizeOf(std.posix.sockaddr.in),
    );

    // set the listen fd to nonblocking mode
    try fcntlSetNonBlocking(fd);

    try std.posix.listen(fd, std.os.linux.SOMAXCONN);

    // the event loop
    var poll_args = std.ArrayList(std.posix.pollfd).init(allocator);
    defer poll_args.deinit();

    std.log.info("server is running on port 1234", .{});
    while (true) {
        // prepare the arguments of the poll()
        poll_args.clearAndFree();
        // put the listening sockets in the first position
        try poll_args.append(.{ .fd = fd, .events = std.os.linux.POLL.IN, .revents = 0 });

        // the rest are connection sockets
        var values_it = g_data.conns.valueIterator();
        while (values_it.next()) |conn| {
            var pfd = std.posix.pollfd{
                .fd = conn.*.fd,
                .events = std.os.linux.POLL.ERR,
                .revents = 0,
            };
            // poll() flags from the application's intent
            if (conn.*.want_read) {
                pfd.events |= std.os.linux.POLL.IN;
            }
            if (conn.*.want_write) {
                pfd.events |= std.os.linux.POLL.OUT;
            }
            try poll_args.append(pfd);
        }

        // wait for readiness
        const timeout_ms = try nextTimerMs();
        _ = try std.posix.poll(poll_args.items, timeout_ms);

        // handle the listening socket
        if (poll_args.items[0].revents != 0) {
            _ = try handleAccept(allocator, fd);
        }

        // handle connection sockets
        var i: usize = 1;
        while (i < poll_args.items.len) {
            defer i += 1;
            const ready = poll_args.items[i].revents;

            if (ready == 0) continue;

            const idx: usize = @intCast(poll_args.items[i].fd);
            const conn = g_data.conns.get(@intCast(idx)) orelse unreachable;

            // update the idle timer by moving conn to the end of the list
            conn.last_active_ms = try getMonotonicMs();
            conn.idle_node.detach();
            g_data.idle_list.insert_before(&conn.idle_node);

            // handle IO
            if (ready & std.os.linux.POLL.IN != 0) {
                // application logic
                assert(conn.want_read);
                try handleRead(allocator, conn);
            }
            if (ready & std.os.linux.POLL.OUT != 0) {
                // application logic
                assert(conn.want_write);
                try handleWrite(conn);
            }

            // close the socket from socket error or application logic
            if (ready & std.os.linux.POLL.ERR != 0 or conn.want_close) {
                connDestroy(conn, allocator);
            }
        }

        // handle timers
        try processTimers(allocator);
    }
}

const Status = enum(u32) {
    ok,
    err,
    nx,
};

const Response = struct {
    status: Status = .ok,
    data: []const u8 = undefined,
};

const GlobalData = struct {
    db: hash.Map = .{},
    // a map of all client connections, keyed by fd
    conns: std.AutoHashMap(std.posix.socket_t, *Conn) = undefined,
    // timers for idle connections
    idle_list: Dlist = undefined,
    // timers for TTLs
    heap: std.ArrayList(heap.Item) = undefined,
    // the thread pool
    thread_pool: ThreadPool = undefined,
};

var g_data: GlobalData = undefined;

fn connDestroy(conn: *Conn, allocator: std.mem.Allocator) void {
    std.posix.close(conn.fd);
    _ = g_data.conns.remove(conn.fd);
    Dlist.detach(&conn.idle_node);
    conn.deinit(allocator);
}

const Entry = struct {
    node: hash.Node = .{},
    key: []const u8 = undefined,
    // for TTL, array index to the heap item
    head_index: usize = std.math.maxInt(usize),
    value: ValueType = .init,
    // one of the following
    str: []const u8 = undefined,
    set: zset.Set = undefined,

    fn init(allocator: std.mem.Allocator, value_type: ValueType) !*Entry {
        const entry = try allocator.create(Entry);
        entry.* = .{
            .value = value_type,
        };
        return entry;
    }

    fn deinit(self: *Entry, allocator: std.mem.Allocator) void {
        // unlink it from any data structures
        // remove from the heap data structure
        self.setTtl(null) catch unreachable;
        // run the destructor in a thread pool for large data structures
        const set_size = if (self.value == .zset) self.set.map.size() else 0;
        const large_container_size = 1000;
        if (set_size > large_container_size) {
            // Try to allocate context for async deletion, fall back to sync if allocation fails
            if (allocator.create(EntryDelContext)) |ctx| {
                ctx.* = .{ .entry = self, .allocator = allocator };
                g_data.thread_pool.queueWork(entryDelFunc, @as(*anyopaque, @ptrCast(ctx)));
            } else |_| {
                self.deintSync(allocator); // fallback to sync on allocation failure
            }
        } else {
            self.deintSync(allocator); // small; avoid context switches
        }

        switch (self.value) {
            .init => {},
            .string => allocator.free(self.str),
            .zset => self.set.clear(allocator),
        }
        allocator.destroy(self);
    }

    fn deintSync(self: *Entry, allocator: std.mem.Allocator) void {
        if (self.value == .zset) {
            self.set.clear(allocator);
        }
        allocator.destroy(self);
    }

    fn setTtl(self: *Entry, ttl_ms: ?u64) !void {
        if (ttl_ms == null and self.head_index != std.math.maxInt(usize)) {
            // setting a negative TTL means removing the TTL
            heapDelete(&g_data.heap, self.head_index);
            self.head_index = std.math.maxInt(usize);
        } else if (ttl_ms != null) {
            // add or update the heap data structure
            const expire_at = try getMonotonicMs() + ttl_ms.?;
            const item = heap.Item{ .val = expire_at, .ref = if (self.head_index == std.math.maxInt(usize)) null else &self.head_index };
            try heapUpsert(&g_data.heap, if (self.head_index == std.math.maxInt(usize)) g_data.heap.items.len else self.head_index, item);
        }
    }
};

const EntryDelContext = struct {
    entry: *Entry,
    allocator: std.mem.Allocator,
};

fn entryDelFunc(arg: *anyopaque) void {
    const ctx = @as(*const EntryDelContext, @ptrCast(@alignCast(arg)));
    ctx.entry.deintSync(ctx.allocator);
    // Free the context
    ctx.allocator.destroy(ctx);
}

const LookupKey = struct {
    node: hash.Node = .{},
    key: []const u8 = undefined,
};

// equality comparison for the top-level hashstable
fn entryEq(node: *hash.Node, key: *hash.Node) bool {
    const left: *Entry = @fieldParentPtr("node", node);
    const right: *LookupKey = @fieldParentPtr("node", key);
    return std.mem.eql(u8, left.key, right.key);
}

fn keysCb(node: *hash.Node, arg: ?*anyopaque) bool {
    const entry: *Entry = @fieldParentPtr("node", node);
    const out: *Buffer = @ptrCast(@alignCast(arg.?));
    emitString(out, entry.key) catch return false;
    return true;
}

fn keys(out: *Buffer) !void {
    // First emit array header with total number of entries
    try emitArray(out, @intCast(g_data.db.newer.size + if (g_data.db.older.slots != null) g_data.db.older.size else 0));

    // Iterate over all entries
    _ = g_data.db.forEach(keysCb, out);
}

fn fcntlSetNonBlocking(fd: std.posix.socket_t) !void {
    const F = std.posix.F;
    const cmd = try std.posix.fcntl(fd, F.GETFL, 0) | std.posix.LOCK.NB;
    _ = try std.posix.fcntl(
        fd,
        @intCast(cmd),
        0,
    );
}

fn handleAccept(allocator: std.mem.Allocator, fd: std.posix.socket_t) !i32 {
    // accept
    var client_addr: std.posix.sockaddr = undefined;
    var client_addr_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);
    const client_fd = std.posix.accept(
        fd,
        @as(*std.posix.sockaddr, @ptrCast(&client_addr)),
        &client_addr_len,
        0,
    ) catch return -1;

    const ip = client_addr.data;
    std.log.info("new client from {d}.{d}.{d}.{d}", .{
        ip[0],
        ip[1],
        ip[2],
        ip[3],
    });

    // set the new connection fd to nonblocking mode
    fcntlSetNonBlocking(client_fd) catch return -1;

    var conn = try Conn.init(allocator, client_fd);
    conn.want_read = true;
    conn.last_active_ms = try getMonotonicMs();
    g_data.idle_list.insert_before(&conn.idle_node);

    // put it into the map
    try g_data.conns.put(conn.fd, conn);
    return 0;
}

fn handleRead(allocator: std.mem.Allocator, conn: *Conn) !void {
    // 1. Do a non-blocking read.
    var buf: [64 * 1024]u8 = undefined;
    const n = std.posix.read(conn.fd, &buf) catch {
        conn.want_close = true;
        return;
    };
    // handle EOF
    if (n == 0) {
        if (conn.incoming.len() == 0) {
            std.log.debug("client closed", .{});
        } else {
            std.log.debug("unexpected EOF", .{});
        }
        conn.want_close = true;
        return;
    }

    // 2. Add new data to the incoming buffer
    try conn.incoming.append(buf[0..n]);

    while (try oneRequest(allocator, conn)) {}

    // update the readiness intention
    if (conn.outgoing.len() > 0) { // has a response
        conn.want_read = false;
        conn.want_write = true;
        return handleWrite(conn);
    } // else: want read
}

fn handleWrite(conn: *Conn) !void {
    assert(conn.outgoing.len() > 0);
    const n = std.posix.write(conn.fd, conn.outgoing.slice()) catch |err| {
        if (err == error.WouldBlock) {
            // actually not ready
            return;
        }
        conn.want_close = true;
        return;
    };
    // remove written data from `outgoing`
    conn.outgoing.consume(n);

    // update the readiness intention
    if (conn.outgoing.len() == 0) { // all data written
        conn.want_read = true;
        conn.want_write = false;
    } // else: want write
}

// process 1 request if there is enough data
fn oneRequest(allocator: std.mem.Allocator, conn: *Conn) !bool {
    // Protocol: message header
    if (conn.incoming.len() < 4) {
        // want read
        return false;
    }

    const incoming_slice = conn.incoming.slice();
    const msg_len = std.mem.bytesToValue(u32, incoming_slice[0..4]);
    if (msg_len > max_msg_size) {
        // protocol error
        conn.want_close = true;
        return error.MessageTooLong;
    }

    // Protocol: message body
    if (4 + msg_len > incoming_slice.len) {
        // want read
        return false;
    }

    // request body
    const request = incoming_slice[4 .. 4 + msg_len];

    std.log.info("client says: len: {d}, data: {s}", .{ request.len, request[0..@min(request.len, 100)] });

    // Process the parsed message.
    const cmd = parseRequest(allocator, request) catch {
        conn.want_close = true;
        return false;
    };

    const header_pos = try responseBegin(&conn.outgoing);
    try doRequest(allocator, cmd, &conn.outgoing);
    try responseEnd(&conn.outgoing, header_pos);

    // Remove the message from incoming
    conn.incoming.consume(msg_len + 4);

    return true;
}

fn responseBegin(out: *Buffer) !usize {
    const pos = out.len();
    try out.appendU32(0);
    return pos;
}

fn responseEnd(out: *Buffer, pos: usize) !void {
    const msg_size = out.len() - pos - 4;
    if (msg_size > max_msg_size) {
        // Truncate the buffer to just the header
        const to_remove = out.len() - (pos + 4);
        out.consume(to_remove);
        // Write the error response
        try emitErr(out, @intFromEnum(ErrorCode.too_big), "response is too big");
        const new_size = out.len() - pos - 4;
        const size_bytes = std.mem.toBytes(@as(u32, @intCast(new_size)));
        @memcpy(out.slice()[pos .. pos + 4], &size_bytes);
        return;
    }

    const size_bytes = std.mem.toBytes(@as(u32, @intCast(msg_size)));
    @memcpy(out.slice()[pos .. pos + 4], &size_bytes);
}

fn parseRequest(allocator: std.mem.Allocator, request: []const u8) ![]const []const u8 {
    var pos: usize = 0;
    const nstr = try readU32(request, &pos);
    if (nstr > max_args) return error.TooManyArgs;

    var out = std.ArrayList([]const u8).init(allocator);

    while (out.items.len < nstr) {
        const len = try readU32(request, &pos);
        const s = try readSlice(request, &pos, len);
        try out.append(s);
    }

    if (pos != request.len) return error.TrailingGarbage;
    return out.toOwnedSlice();
}

fn readU32(data: []const u8, pos: *usize) !u32 {
    if (pos.* + 4 > data.len) return error.Truncated;
    const v = std.mem.bytesToValue(u32, data[pos.* .. pos.* + 4]);
    pos.* += 4;
    return v;
}

fn readSlice(data: []const u8, pos: *usize, len: u32) ![]const u8 {
    const count: usize = @intCast(len);
    if (pos.* + count > data.len) return error.Truncated;
    const slice = data[pos.* .. pos.* + count];
    pos.* += count;
    return slice;
}

fn doRequest(allocator: std.mem.Allocator, cmd: []const []const u8, out: *Buffer) !void {
    if (cmd.len == 2 and std.mem.eql(u8, cmd[0], "get")) {
        return get(cmd, out);
    } else if (cmd.len == 3 and std.mem.eql(u8, cmd[0], "set")) {
        return set(allocator, cmd, out);
    } else if (cmd.len == 2 and std.mem.eql(u8, cmd[0], "del")) {
        return del(allocator, cmd, out);
    } else if (cmd.len == 1 and std.mem.eql(u8, cmd[0], "keys")) {
        return keys(out);
    } else if (cmd.len == 4 and std.mem.eql(u8, cmd[0], "zadd")) {
        return zadd(allocator, cmd, out);
    } else if (cmd.len == 3 and std.mem.eql(u8, cmd[0], "zrem")) {
        return zrem(allocator, cmd, out);
    } else if (cmd.len == 3 and std.mem.eql(u8, cmd[0], "zscore")) {
        return zscore(cmd, out);
    } else if (cmd.len == 6 and std.mem.eql(u8, cmd[0], "zquery")) {
        return zquery(cmd, out);
    } else if (cmd.len == 3 and std.mem.eql(u8, cmd[0], "pexpire")) {
        return expire(cmd, out);
    } else if (cmd.len == 2 and std.mem.eql(u8, cmd[0], "pttl")) {
        return ttl(cmd, out);
    } else {
        // unknown command
        return try emitErr(
            out,
            @intFromEnum(ErrorCode.unknown),
            "unknown command",
        );
    }
}

fn makeResponse(response: Response, outgoing: *Buffer) !void {
    const response_len: u32 = @intCast(4 + response.data.len);
    try outgoing.append(&std.mem.toBytes(response_len));
    try outgoing.append(&std.mem.toBytes(response.status));
    try outgoing.append(response.data);
}

fn get(cmd: []const []const u8, out: *Buffer) !void {
    // a dummy struct just for the lookup
    var dummy_entry = LookupKey{};
    dummy_entry.key = cmd[1];
    dummy_entry.node.code = root.stringHash(cmd[1]);

    if (g_data.db.lookup(&dummy_entry.node, entryEq)) |node| {
        // copy the value
        const entry: *Entry = @fieldParentPtr("node", node);
        if (entry.value != .string) {
            return try emitErr(out, @intFromEnum(ErrorCode.bad_typ), "not a string value");
        }
        return emitString(out, entry.str);
    } else {
        return emitNil(out);
    }
}

fn set(allocator: std.mem.Allocator, cmd: []const []const u8, out: *Buffer) !void {
    // a dummy struct just for the lookup
    var dummy_entry = LookupKey{};
    dummy_entry.key = cmd[1];
    dummy_entry.node.code = root.stringHash(cmd[1]);

    if (g_data.db.lookup(&dummy_entry.node, entryEq)) |node| {
        // found, update the value
        const entry: *Entry = @fieldParentPtr("node", node);
        if (entry.value != .string) {
            return try emitErr(out, @intFromEnum(ErrorCode.bad_typ), "a non-string value exists");
        }
        // Free old value and duplicate new one
        allocator.free(entry.str);
        entry.str = try allocator.dupe(u8, cmd[2]);
    } else {
        // not found, allocate & insert a new pair
        const entry = try Entry.init(allocator, .string);
        entry.key = try allocator.dupe(u8, cmd[1]);
        entry.node.code = dummy_entry.node.code;
        entry.str = try allocator.dupe(u8, cmd[2]);
        try g_data.db.insert(&entry.node);
    }
    return emitNil(out);
}

fn del(allocator: std.mem.Allocator, cmd: []const []const u8, out: *Buffer) !void {
    // a dummy struct just for the lookup
    var dummy_entry = LookupKey{};
    dummy_entry.key = cmd[1];
    dummy_entry.node.code = root.stringHash(cmd[1]);
    if (g_data.db.delete(&dummy_entry.node, entryEq)) |node| {
        const entry: *Entry = @fieldParentPtr("node", node);
        // deallocate entry resources
        entry.deinit(allocator);
        return emitInt(out, 1);
    }
    return emitInt(out, 0);
}

// PEXPIRE key ttl_ms
fn expire(cmd: []const []const u8, out: *Buffer) !void {
    const parsed_ttl = try std.fmt.parseInt(i64, cmd[2], 10);
    const ttl_ms = if (parsed_ttl < 0) null else @as(u64, @intCast(parsed_ttl));

    var dummy_entry = LookupKey{};
    dummy_entry.key = cmd[1];
    dummy_entry.node.code = root.stringHash(cmd[1]);

    const node = g_data.db.lookup(&dummy_entry.node, entryEq);
    if (node) |n| {
        const entry: *Entry = @fieldParentPtr("node", n);
        try entry.setTtl(ttl_ms);
    }
    return emitInt(out, if (node == null) 0 else 1);
}

// PTTL key
fn ttl(cmd: []const []const u8, out: *Buffer) !void {
    var dummy_entry = LookupKey{};
    dummy_entry.key = cmd[1];
    dummy_entry.node.code = root.stringHash(cmd[1]);

    const node = g_data.db.lookup(&dummy_entry.node, entryEq);
    if (node == null) {
        return emitInt(out, -2); // not found
    }
    const n = node.?;

    const entry: *Entry = @fieldParentPtr("node", n);
    if (entry.head_index == std.math.maxInt(usize)) {
        return emitInt(out, -1); // no TTL
    }

    const expire_at = g_data.heap.items[entry.head_index].val;
    const now_ms = try getMonotonicMs();
    return emitInt(out, if (expire_at > now_ms) @intCast(expire_at - now_ms) else 0);
}

fn emitNil(out: *Buffer) !void {
    try out.appendU8(@intFromEnum(DataType.nil));
}

fn emitString(out: *Buffer, value: []const u8) !void {
    try out.appendU8(@intFromEnum(DataType.string));
    try out.appendU32(@intCast(value.len));
    try out.append(value);
}

fn emitInt(out: *Buffer, value: i64) !void {
    try out.appendU8(@intFromEnum(DataType.int));
    try out.appendI64(value);
}

fn emitArray(out: *Buffer, n: u32) !void {
    try out.appendU8(@intFromEnum(DataType.array));
    try out.appendU32(n);
}

fn emitBeginArray(out: *Buffer) !usize {
    try out.appendU8(@intFromEnum(DataType.array));
    try out.appendU32(0);
    return out.len() - 4;
}

fn emitEndArray(out: *Buffer, pos: usize, n: u32) !void {
    // Ensure the byte right before the length placeholder is the array tag
    assert(pos > 0);
    const slice = out.slice();
    assert(slice[pos - 1] == @intFromEnum(DataType.array));

    // Overwrite the 4-byte placeholder with the actual element count (little-endian)
    const len_bytes = std.mem.toBytes(n);
    @memcpy(slice[pos .. pos + 4], &len_bytes);
}

fn emitFloat(out: *Buffer, value: f64) !void {
    try out.appendU8(@intFromEnum(DataType.float));
    try out.appendF64(value);
}

fn emitErr(out: *Buffer, code: u32, msg: []const u8) !void {
    try out.appendU8(@intFromEnum(DataType.err));
    try out.appendU32(code);
    try out.appendU32(@intCast(msg.len));
    try out.append(msg);
}

fn getMonotonicMs() !u64 {
    const timespec_tv = try std.posix.clock_gettime(std.posix.CLOCK.MONOTONIC);
    return @as(u64, @intCast(timespec_tv.sec)) * 1000 + @as(u64, @intCast(timespec_tv.nsec)) / 1000 / 1000;
}

fn nextTimerMs() !i32 {
    const now_ms = try getMonotonicMs();
    var next_ms: ?u64 = null;

    // idle timers using a linked list
    if (!g_data.idle_list.empty()) {
        const conn: *Conn = @fieldParentPtr("idle_node", g_data.idle_list.next.?);
        next_ms = conn.last_active_ms + idle_timeout_ms;
    }

    // TTL timers using a heap
    if (g_data.heap.items.len != 0 and (next_ms != null and g_data.heap.items[0].val < next_ms.?)) {
        next_ms = g_data.heap.items[0].val;
    }
    // timeout value
    if (next_ms == null) {
        return -1; // no timers, no timeouts
    }
    if (next_ms.? <= now_ms) {
        return 0; // missed?
    }
    return @intCast(next_ms.? - now_ms);
}

fn processTimers(allocator: std.mem.Allocator) !void {
    const now_ms = try getMonotonicMs();
    // idle timers using a linked list
    while (!g_data.idle_list.empty()) {
        const conn: *Conn = @fieldParentPtr("idle_node", g_data.idle_list.next.?);
        const next_ms = conn.last_active_ms + idle_timeout_ms;
        if (next_ms >= now_ms) {
            break; // not expired
        }

        std.log.info("removing idle connection: {d}", .{conn.fd});
        connDestroy(conn, allocator);
    }
    // TTL timers using a heap
    const max_works = 2000;
    var nworks: usize = 0;
    const heap_copy = g_data.heap;
    while (heap_copy.items.len != 0 and heap_copy.items[0].val < now_ms) {
        if (heap_copy.items[0].ref) |ref| {
            const entry: *Entry = @fieldParentPtr("head_index", ref);
            const node = g_data.db.delete(&entry.node, nodeSame);
            assert(node == &entry.node);
            // delete the key
            entry.deinit(allocator);
        }
        if (nworks >= max_works) {
            // don't stall the server if too many keys are expiring at once
            break;
        }
        nworks += 1;
    }
}

fn heapDelete(h: *std.ArrayList(heap.Item), pos: usize) void {
    // swap the erased item with the last item
    if (pos >= h.items.len) return;

    // Move the last item to the position being deleted
    h.items[pos] = h.items[h.items.len - 1];
    _ = h.pop();

    // Update the heap if there are still items and we're not at the end
    if (pos < h.items.len) {
        heap.update(h.items, pos);
    }
}

fn heapUpsert(h: *std.ArrayList(heap.Item), pos: usize, item: heap.Item) !void {
    var p = pos;
    if (p < h.items.len) {
        // Update existing item
        h.items[p] = item;
    } else {
        // Add new item at the end
        try h.append(item);
        p = h.items.len - 1;
    }
    heap.update(h.items, p);
}

fn nodeSame(node: *hash.Node, key: *hash.Node) bool {
    return node == key;
}

// -----------------------------------------------------------------------------
// ZSet utilities and commands
// -----------------------------------------------------------------------------

var k_empty_zset = zset.Set{
    .root = null,
    .map = .{},
};

fn expectZSet(key: []const u8) ?*zset.Set {
    var dummy = LookupKey{};
    dummy.key = key;
    dummy.node.code = root.stringHash(key);
    if (g_data.db.lookup(&dummy.node, entryEq)) |node| {
        const entry: *Entry = @fieldParentPtr("node", node);
        return if (entry.value == .zset) &entry.set else null;
    }
    // non-existent key behaves like an empty zset
    return @constCast(&k_empty_zset);
}

// zadd zset score name
fn zadd(allocator: std.mem.Allocator, cmd: []const []const u8, out: *Buffer) !void {
    const score = std.fmt.parseFloat(f64, cmd[2]) catch {
        return try emitErr(out, @intFromEnum(ErrorCode.bad_arg), "expect float");
    };

    // look up or create the zset
    var dummy_entry = LookupKey{};
    dummy_entry.key = cmd[1];
    dummy_entry.node.code = root.stringHash(cmd[1]);

    const ent = if (g_data.db.lookup(&dummy_entry.node, entryEq)) |node| blk: {
        // check the existing key
        const entry: *Entry = @fieldParentPtr("node", node);
        if (entry.value != .zset) {
            return try emitErr(out, @intFromEnum(ErrorCode.bad_typ), "expect zset");
        }
        break :blk entry;
    } else blk: {
        // insert a new key
        const entry = try Entry.init(allocator, .zset);
        entry.key = try allocator.dupe(u8, cmd[1]);
        entry.node.code = dummy_entry.node.code;
        entry.set = .{
            .root = null,
            .map = .{},
        };
        try g_data.db.insert(&entry.node);
        break :blk entry;
    };

    // add or update the tuple
    const added = try ent.set.insert(allocator, cmd[3], score);
    return emitInt(out, @intFromBool(added));
}

// zrem zset name
fn zrem(allocator: std.mem.Allocator, cmd: []const []const u8, out: *Buffer) !void {
    const set_ptr = expectZSet(cmd[1]) orelse {
        return try emitErr(out, @intFromEnum(ErrorCode.bad_typ), "expect zset");
    };

    const name = cmd[2];
    const node = set_ptr.lookup(name);
    if (node) |znode| {
        set_ptr.delete(allocator, znode);
    }
    return emitInt(out, if (node == null) 0 else 1);
}

// zscore zset name
fn zscore(cmd: []const []const u8, out: *Buffer) !void {
    const set_ptr = expectZSet(cmd[1]) orelse {
        return try emitErr(out, @intFromEnum(ErrorCode.bad_typ), "expect zset");
    };

    const name = cmd[2];
    if (set_ptr.lookup(name)) |znode| {
        return emitFloat(out, znode.score);
    }
    return emitNil(out);
}

// zquery zset score name offset limit
fn zquery(cmd: []const []const u8, out: *Buffer) !void {
    // parse args
    const score = std.fmt.parseFloat(f64, cmd[2]) catch {
        return try emitErr(out, @intFromEnum(ErrorCode.bad_arg), "expect fp number");
    };
    const name = cmd[3];
    const offset = std.fmt.parseInt(i64, cmd[4], 10) catch {
        return try emitErr(out, @intFromEnum(ErrorCode.bad_arg), "expect int");
    };
    const limit = std.fmt.parseInt(i64, cmd[5], 10) catch {
        return try emitErr(out, @intFromEnum(ErrorCode.bad_arg), "expect int");
    };

    // get the zset
    const set_ptr = expectZSet(cmd[1]) orelse {
        return try emitErr(out, @intFromEnum(ErrorCode.bad_typ), "expect zset");
    };

    // seek to the key
    if (limit <= 0) {
        return emitArray(out, 0);
    }

    const limit_u: u32 = @intCast(limit);
    var node_opt = set_ptr.seekGe(score, name);
    node_opt = zset.offset(node_opt, @intCast(offset));

    const pos = try emitBeginArray(out);
    var n: u32 = 0;
    var current = node_opt;
    while (current) |znode| {
        if (n >= limit_u) break;
        try emitString(out, znode.name[0..znode.len]);
        try emitFloat(out, znode.score);
        current = zset.offset(current, 1);
        n += 2;
    }
    try emitEndArray(out, pos, n);
}
