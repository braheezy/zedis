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

pub const DataType = enum {
    nil,
    err,
    string,
    int,
    float,
    array,
};

pub const ValueType = enum {
    init,
    string,
    zset,
};

pub const ErrorCode = enum {
    unknown,
    too_big,
    bad_typ,
    bad_arg,
};

pub fn stringHash(data: []const u8) u64 {
    var h: u64 = 0x811C9DC5;
    for (data) |c| {
        h = (h + @as(u64, c)) *% 0x01000193;
    }
    return h;
}
