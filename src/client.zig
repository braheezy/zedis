const std = @import("std");

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

    const msg = "hello";
    _ = try std.posix.write(fd, msg);

    var buf: [1024]u8 = undefined;
    const bytes_read = try std.posix.read(fd, &buf);
    std.debug.print("server says: {s}\n", .{buf[0..bytes_read]});
}
