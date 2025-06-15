const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const root = @import("root.zig");
const readAll = root.readAll;
const writeAll = root.writeAll;
const max_msg_size = root.max_msg_size;
const max_args = 200 * 1000;
const Conn = @import("Conn.zig").Conn;
const Buffer = @import("Buffer.zig");
const hash = @import("hash.zig");

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

    g_data = .{};

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

    // a map of all client connections, keyed by fd
    var conns = std.ArrayList(?*Conn).init(allocator);
    defer conns.deinit();

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
        for (conns.items) |maybe_conn| {
            if (maybe_conn) |conn| {
                var pfd = std.posix.pollfd{
                    .fd = conn.fd,
                    .events = std.os.linux.POLL.ERR,
                    .revents = 0,
                };
                // poll() flags from the application's intent
                if (conn.want_read) {
                    pfd.events |= std.os.linux.POLL.IN;
                }
                if (conn.want_write) {
                    pfd.events |= std.os.linux.POLL.OUT;
                }
                try poll_args.append(pfd);
            } else {
                continue;
            }
        }

        // wait for readiness
        _ = try std.posix.poll(poll_args.items, -1);

        // handle the listening socket
        if (poll_args.items[0].revents != 0) {
            if (try handleAccept(allocator, fd)) |conn| {
                const idx: usize = @intCast(conn.fd);
                while (conns.items.len <= idx) {
                    try conns.append(null);
                }
                conns.items[idx] = conn;
            }
        }

        // handle connection sockets
        var i: usize = 1;
        while (i < poll_args.items.len) {
            defer i += 1;
            const ready = poll_args.items[i].revents;

            if (ready == 0) continue;

            const idx: usize = @intCast(poll_args.items[i].fd);
            const conn = conns.items[idx] orelse unreachable;

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
                std.posix.close(conn.fd);
                const j: usize = @intCast(conn.fd);
                conns.items[j] = null;
                conn.deinit(allocator);
            }
        }
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
};

var g_data: GlobalData = undefined;

const Entry = struct {
    node: hash.Node = .{},
    key: []const u8 = undefined,
    value: []const u8 = undefined,
};

fn entryEq(a: *hash.Node, b: *hash.Node) bool {
    const left: *Entry = @fieldParentPtr("node", a);
    const right: *Entry = @fieldParentPtr("node", b);
    return std.mem.eql(u8, left.key, right.key);
}

fn stringHash(data: []const u8) u64 {
    var h: u64 = 0x811C9DC5;
    for (data) |c| {
        h = (h + c) *% 0x01000193;
    }
    return h;
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

fn handleAccept(allocator: std.mem.Allocator, fd: std.posix.socket_t) !?*Conn {
    // accept
    var client_addr: std.posix.sockaddr = undefined;
    var client_addr_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);
    const client_fd = std.posix.accept(
        fd,
        @as(*std.posix.sockaddr, @ptrCast(&client_addr)),
        &client_addr_len,
        0,
    ) catch return null;

    const ip = client_addr.data;
    std.log.info("new client from {d}.{d}.{d}.{d}", .{
        ip[0],
        ip[1],
        ip[2],
        ip[3],
    });

    // set the new connection fd to nonblocking mode
    fcntlSetNonBlocking(client_fd) catch return null;

    var conn = try Conn.init(allocator, client_fd);
    conn.want_read = true;

    return conn;
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
    const response = try doRequest(allocator, cmd);
    try makeResponse(response, &conn.outgoing);

    // Remove the message from incoming
    conn.incoming.consume(msg_len + 4);

    return true;
}

fn parseRequest(allocator: std.mem.Allocator, request: []const u8) ![]const []const u8 {
    var pos: usize = 0;
    const nstr = try readU32(request, &pos);
    if (nstr > max_args) return error.TooManyArgs;

    var out = std.ArrayList([]const u8).init(allocator);
    // defer out.deinit();

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

fn doRequest(allocator: std.mem.Allocator, cmd: []const []const u8) !Response {
    if (cmd.len == 2 and std.mem.eql(u8, cmd[0], "get")) {
        return get(cmd);
    } else if (cmd.len == 3 and std.mem.eql(u8, cmd[0], "set")) {
        return set(allocator, cmd);
    } else if (cmd.len == 2 and std.mem.eql(u8, cmd[0], "del")) {
        return del(allocator, cmd);
    } else {
        // unknown command
        return .{ .status = .err };
    }
}

fn makeResponse(response: Response, outgoing: *Buffer) !void {
    const response_len: u32 = @intCast(4 + response.data.len);
    try outgoing.append(&std.mem.toBytes(response_len));
    try outgoing.append(&std.mem.toBytes(response.status));
    try outgoing.append(response.data);
}

fn get(cmd: []const []const u8) Response {
    var out = Response{};
    // a dummy `Entry` just for the lookup
    var dummy_entry = Entry{};
    dummy_entry.key = cmd[1];
    dummy_entry.node.code = stringHash(cmd[1]);

    if (g_data.db.lookup(&dummy_entry.node, entryEq)) |node| {
        const entry: *Entry = @fieldParentPtr("node", node);
        const value = entry.value;
        assert(value.len < max_msg_size);
        out.data = value;
    } else {
        out.status = .nx;
    }

    return out;
}

fn set(allocator: std.mem.Allocator, cmd: []const []const u8) !Response {
    // a dummy `Entry` just for the lookup
    var dummy_entry = Entry{};
    dummy_entry.key = cmd[1];
    dummy_entry.node.code = stringHash(cmd[1]);

    if (g_data.db.lookup(&dummy_entry.node, entryEq)) |node| {
        // found, update the value
        const entry: *Entry = @fieldParentPtr("node", node);
        // Free old value and duplicate new one
        allocator.free(entry.value);
        entry.value = try allocator.dupe(u8, cmd[2]);
    } else {
        // not found, allocate & insert a new pair
        const entry = try allocator.create(Entry);
        entry.* = .{
            .key = try allocator.dupe(u8, cmd[1]),
            .node = .{
                .code = dummy_entry.node.code,
            },
            .value = try allocator.dupe(u8, cmd[2]),
        };
        try g_data.db.insert(&entry.node);
    }
    return .{};
}

fn del(allocator: std.mem.Allocator, cmd: []const []const u8) Response {
    // a dummy `Entry` just for the lookup
    var dummy_entry = Entry{};
    dummy_entry.key = cmd[1];
    dummy_entry.node.code = stringHash(cmd[1]);
    if (g_data.db.delete(&dummy_entry.node, entryEq)) |node| {
        const entry: *Entry = @fieldParentPtr("node", node);
        // free the strings and entry
        allocator.free(entry.key);
        allocator.free(entry.value);
        allocator.destroy(entry);
    }
    return .{};
}
