///! A simple ring buffer
const std = @import("std");

const initial_capacity = 1024;

pub const Buffer = @This();

allocator: std.mem.Allocator,
data: []u8,
data_begin: usize,
data_end: usize,

pub fn init(allocator: std.mem.Allocator) !Buffer {
    const buf = try allocator.alloc(u8, initial_capacity);
    const mid = initial_capacity / 2;

    return .{
        .allocator = allocator,
        .data = buf,
        .data_begin = mid,
        .data_end = mid,
    };
}

pub fn deinit(self: *Buffer) void {
    self.allocator.free(self.data);
}

pub fn len(self: *Buffer) usize {
    return self.data_end - self.data_begin;
}

pub fn needsGrow(self: *Buffer, n: usize, at_back: bool) bool {
    return if (at_back)
        self.data.len - self.data_end < n
    else
        self.data_begin < n;
}

pub fn grow(self: *Buffer, extra: usize) !void {
    const old_data = self.data;
    const old_begin = self.data_begin;
    const old_end = self.data_end;
    const old_len = self.len();

    const needed = extra + old_len;
    const new_cap = @max(old_data.len * 2, needed * 2);
    const new_buf = try self.allocator.alloc(u8, new_cap);
    const new_mid = (new_cap - old_len) / 2;

    // Copy the data from the old buffer to the new buffer
    @memcpy(new_buf[new_mid .. new_mid + old_len], old_data[old_begin..old_end]);
    self.allocator.free(old_data);

    self.data = new_buf;
    self.data_begin = new_mid;
    self.data_end = new_mid + old_len;
}

pub fn append(self: *Buffer, bytes: []const u8) !void {
    if (self.needsGrow(bytes.len, true)) try self.grow(bytes.len);
    @memcpy(self.data[self.data_end .. self.data_end + bytes.len], bytes);
    self.data_end += bytes.len;
}

pub fn prepend(self: *Buffer, bytes: []const u8) !void {
    if (self.needsGrow(bytes.len, false)) try self.grow(bytes.len);
    self.data_begin -= bytes.len;
    @memcpy(self.data[self.data_begin .. self.data_begin + bytes.len], bytes);
}

pub fn consume(self: *Buffer, n: usize) void {
    if (n >= self.len()) {
        // empty
        self.data_begin = self.data_end;
    } else {
        self.data_begin += n;
    }
}

pub fn slice(self: *Buffer) []u8 {
    return self.data[self.data_begin..self.data_end];
}
