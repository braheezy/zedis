const std = @import("std");

pub const max_msg_size = 4096;

pub fn readAll(fd: std.posix.socket_t, buf: []u8) !void {
    var index: usize = 0;
    while (index < buf.len) {
        const bytes_read = try std.posix.read(fd, buf[index..]);
        if (bytes_read == 0) return error.UnexpectedEof;
        index += bytes_read;
    }
}

pub fn writeAll(fd: std.posix.socket_t, buf: []const u8) !void {
    var index: usize = 0;
    while (index < buf.len) {
        index += try std.posix.write(fd, buf[index..]);
    }
}
