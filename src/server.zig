const std = @import("std");

pub fn main() !void {
    const fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);

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
        var client_addr: std.posix.sockaddr = undefined;
        var client_addr_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);
        const client_fd = try std.posix.accept(fd, &client_addr, &client_addr_len, 0);

        try doSomething(client_fd);
    }
}

fn doSomething(conn_fd: std.posix.socket_t) !void {
    var buf: [1024]u8 = undefined;
    const bytes_read = try std.posix.read(conn_fd, &buf);

    std.debug.print("client says: {s}\n", .{buf[0..bytes_read]});

    const wbuf = "world";
    _ = try std.posix.write(conn_fd, wbuf);
}
