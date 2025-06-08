const std = @import("std");

const root = @import("root.zig");
const readAll = root.readAll;
const writeAll = root.writeAll;
const max_msg_size = root.max_msg_size;

// 127.0.0.1
const loopback_addr = 0x7F000001;

pub fn main() !void {
    const fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
    defer std.posix.close(fd);

    const addr = std.posix.sockaddr.in{
        .family = std.posix.AF.INET,
        .port = std.mem.nativeToBig(u16, 1234),
        .addr = std.mem.nativeToBig(u32, loopback_addr),
    };

    try std.posix.connect(
        fd,
        @as(*const std.posix.sockaddr, @ptrCast(&addr)),
        @sizeOf(std.posix.sockaddr.in),
    );

    try query(fd, "hello1");
    try query(fd, "hello2");
}

fn query(fd: std.posix.socket_t, msg: []const u8) !void {
    if (msg.len > max_msg_size) {
        return error.MessageTooLong;
    }

    // send request
    var write_buf: [4 + max_msg_size]u8 = undefined;
    @memcpy(write_buf[0..4], &std.mem.toBytes(@as(u32, @intCast(msg.len))));
    @memcpy(write_buf[4 .. msg.len + 4], msg);
    try writeAll(fd, write_buf[0 .. msg.len + 4]);

    // 4 bytes header
    var read_buf: [4 + max_msg_size + 1]u8 = undefined;
    try readAll(fd, read_buf[0..4]);

    const msg_len = std.mem.readInt(u32, read_buf[0..4], .little);
    if (msg_len > max_msg_size) {
        return error.MessageTooLong;
    }

    // reply body
    try readAll(fd, read_buf[4 .. msg_len + 4]);
    std.debug.print("server says: {s}\n", .{read_buf[4 .. msg_len + 4]});
}
