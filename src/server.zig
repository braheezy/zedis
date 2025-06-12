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

    try std.posix.listen(fd, std.os.linux.SOMAXCONN);

    // a map of all client connections, keyed by fd
    var conns = std.ArrayList(?Conn).init(allocator);
    @memset(conns, null);
    defer conns.deinit();

    // the event loop
    var poll_args = std.ArrayList(std.os.linux.pollfd).init(allocator);
    defer poll_args.deinit();

    std.log.info("server is running on port 1234", .{});
    while (true) {
        // prepare the arguments of the poll()
        poll_args.clearAndFree();
        // put the listening sockets in the first position
        poll_args.append(.{ .fd = fd, .events = std.os.linux.POLL.IN, .revents = 0 });

        // the rest are connection sockets
        for (conns.items) |maybe_conn| {
            if (maybe_conn) |conn| {
                const pfd = std.os.linux.pollfd{
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
                poll_args.append(pfd);
            } else {
                continue;
            }
        }

        // wait for readiness
        try std.posix.poll(poll_args.items, -1);

        // handle the listening socket
        if (poll_args.items[0].revents != 0) {
            if (try handleAccept(fd)) |conn| {
                conns.items[conn.fd] = conn;
            }
        }

        // handle connection sockets
        var i: usize = 1;
        while (i < poll_args.items.len) {
            const ready = poll_args.items[i].revents;
            const conn = conns.items[poll_args.items[i].fd] orelse unreachable;

            if (ready & std.os.linux.POLL.IN != 0) {
                // application logic
                try handleRead(conn);
            }
            if (ready & std.os.linux.POLL.OUT != 0) {
                // application logic
                try handleWrite(conn);
            }

            // close the socket from socket error or application logic
            if (ready & std.os.linux.POLL.ERR != 0 or conn.want_close) {
                std.posix.close(conn.fd);
                conns.items[conn.fd] = null;
            }
        }
    }
}

fn oneRequest(conn_fd: std.posix.socket_t) !void {
    // 4 bytes header
    var buf: [4 + max_msg_size]u8 = undefined;
    try readAll(conn_fd, buf[0..4]);

    const msg_len = std.mem.readInt(u32, buf[0..4], .little);
    if (msg_len > max_msg_size) {
        return error.MessageTooLong;
    }

    // request body
    try readAll(conn_fd, buf[4 .. msg_len + 4]);

    std.debug.print("client says: {s}\n", .{buf[4 .. msg_len + 4]});
    // reply using the same protocol
    const reply = "world";
    var reply_buf: [4 + reply.len]u8 = undefined;
    @memcpy(reply_buf[0..4], &std.mem.toBytes(@as(u32, @intCast(reply.len))));
    @memcpy(reply_buf[4 .. reply.len + 4], reply);

    try writeAll(conn_fd, &reply_buf);
}

fn fcntlSetNonBlocking(fd: std.posix.socket_t) !void {
    const F = std.posix.F;
    try std.posix.fcntl(
        fd,
        try std.posix.fcntl(fd, F.GETFL, 0) | std.os.linux.SOCK.NONBLOCK,
        0,
    );
}

fn handleAccept(fd: std.posix.socket_t) !?Conn {
    // accept
    var client_addr: std.posix.sockaddr = undefined;
    var client_addr_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);
    const client_fd = std.posix.accept(
        fd,
        @as(*std.posix.sockaddr, @ptrCast(&client_addr)),
        &client_addr_len,
        0,
    ) catch return null;

    // set the new connection fd to nonblocking mode
    fcntlSetNonBlocking(client_fd) catch return null;

    return Conn{
        .fd = client_fd,
        .want_read = true,
    };
}
