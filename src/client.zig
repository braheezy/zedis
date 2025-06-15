const std = @import("std");
const builtin = @import("builtin");

const root = @import("root.zig");
const readAll = root.readAll;
const writeAll = root.writeAll;
const max_msg_size = root.max_msg_size;

// 127.0.0.1
const loopback_addr = 0x7F000001;

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
    // Read arguments
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

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

    try sendRequest(fd, args[1..]);

    try readResponse(fd);
}

fn sendRequest(fd: std.posix.socket_t, cmd: []const []const u8) !void {
    var len: u32 = 4;
    for (cmd) |s| {
        len += 4 + @as(u32, @intCast(s.len));
    }
    if (len > max_msg_size) {
        return error.MessageTooLong;
    }

    // send request
    var write_buf: [4 + max_msg_size]u8 = undefined;
    @memcpy(write_buf[0..4], &std.mem.toBytes(len));
    @memcpy(write_buf[4..8], &std.mem.toBytes(@as(u32, @intCast(cmd.len))));
    var pos: usize = 8;
    for (cmd) |s| {
        const p: u32 = @intCast(s.len);
        @memcpy(write_buf[pos .. pos + 4], &std.mem.toBytes(p));
        @memcpy(write_buf[pos + 4 .. pos + 4 + s.len], s);
        pos += 4 + p;
    }
    try writeAll(fd, write_buf[0..pos]);
}

fn readResponse(fd: std.posix.socket_t) !void {
    // 4 bytes header
    var read_buf: [4 + max_msg_size + 1]u8 = undefined;
    try readAll(fd, read_buf[0..4]);

    const msg_len = std.mem.readInt(u32, read_buf[0..4], .little);
    if (msg_len > max_msg_size) {
        return error.MessageTooLong;
    }

    // reply body
    try readAll(fd, read_buf[4 .. msg_len + 4]);

    if (msg_len < 4) {
        std.debug.print("bad response\n", .{});
        return error.BadResponse;
    }

    // Parse status code
    const rescode = std.mem.readInt(u32, read_buf[4..8], .little);
    const msg = read_buf[8 .. msg_len + 4];

    const status_str = switch (rescode) {
        0 => "ok",
        1 => "err",
        2 => "not found",
        else => "unknown",
    };
    std.debug.print("server says: [{s}] {s}\n", .{ status_str, msg });
}
