const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const root = @import("root.zig");
const readAll = root.readAll;
const writeAll = root.writeAll;
const max_msg_size = root.max_msg_size;
const Conn = @import("Conn.zig").Conn;

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
                try handleRead(conn);
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

const O_NONBLOCK = @as(i32, 0x0004); // This is the value for O_NONBLOCK on macOS
fn fcntlSetNonBlocking(fd: std.posix.socket_t) !void {
    const F = std.posix.F;
    const cmd = try std.posix.fcntl(fd, F.GETFL, 0) | O_NONBLOCK;
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

fn handleRead(conn: *Conn) !void {
    // 1. Do a non-blocking read.
    var buf: [64 * 1024]u8 = undefined;
    const n = std.posix.read(conn.fd, &buf) catch {
        conn.want_close = true;
        return;
    };
    // handle EOF
    if (n == 0) {
        if (conn.incoming.items.len == 0) {
            std.log.debug("client closed", .{});
        } else {
            std.log.debug("unexpected EOF", .{});
        }
        conn.want_close = true;
        return;
    }

    // 2. Add new data to the incoming buffer
    try conn.incoming.appendSlice(buf[0..n]);

    while (try conn.oneRequest()) {}

    // update the readiness intention
    if (conn.outgoing.items.len > 0) { // has a response
        conn.want_read = false;
        conn.want_write = true;
        return handleWrite(conn);
    } // else: want read
}

fn handleWrite(conn: *Conn) !void {
    assert(conn.outgoing.items.len > 0);
    const n = std.posix.write(conn.fd, conn.outgoing.items) catch {
        conn.want_close = true;
        return;
    };
    // remove written data from `outgoing`
    conn.outgoing.replaceRangeAssumeCapacity(0, n, &.{});

    // update the readiness intention
    if (conn.outgoing.items.len == 0) { // all data written
        conn.want_read = true;
        conn.want_write = false;
    } // else: want write
}
