const std = @import("std");
const assert = std.debug.assert;

const root = @import("root.zig");
const readAll = root.readAll;
const writeAll = root.writeAll;
const max_msg_size = root.max_msg_size;

pub fn main() !void {
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

    std.log.info("server is running on port 1234", .{});
    while (true) {
        // accept
        var client_addr: std.posix.sockaddr = undefined;
        var client_addr_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);
        const client_fd = try std.posix.accept(fd, &client_addr, &client_addr_len, 0);
        defer std.posix.close(client_fd);

        // only serves one client connection at once
        while (true) {
            oneRequest(client_fd) catch break;
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
