const std = @import("std");
const testing = std.testing;
const avl = @import("avl.zig");
const Node = avl.Node;
const Random = @import("std").rand.Random;

const Data = struct {
    node: Node,
    val: u32,

    pub fn init(allocator: std.mem.Allocator, val: u32) !*Data {
        const data = try allocator.create(Data);
        data.* = .{
            .node = Node{},
            .val = val,
        };
        return data;
    }
};

const Container = struct {
    root: ?*Node,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) Container {
        return .{
            .root = null,
            .allocator = allocator,
        };
    }

    pub fn add(self: *Container, val: u32) !void {
        const data = try Data.init(self.allocator, val);

        var cur: ?*Node = null;
        var from: *?*Node = &self.root; // the incoming pointer to the next node
        while (from.*) |node| { // tree search
            cur = node;
            const data_ptr: *Data = @ptrCast(@alignCast(node));
            from = if (val < data_ptr.val) &node.left else &node.right;
        }

        from.* = &data.node; // attach the new node
        data.node.parent = cur;
        if (data.node.fix()) |fixed| {
            self.root = fixed;
        }
    }

    pub fn delete(self: *Container, val: u32) !bool {
        var cur = self.root;
        while (cur) |node| {
            const data_ptr: *Data = @ptrCast(@alignCast(node));
            if (val == data_ptr.val) {
                break;
            }
            cur = if (val < data_ptr.val) node.left else node.right;
        }
        if (cur == null) return false;
        self.root = cur.?.delete() orelse null;
        const data: *Data = @ptrCast(@alignCast(cur.?));
        self.allocator.destroy(data);
        return true;
    }

    pub fn deinit(self: *Container) void {
        while (self.root) |root| {
            self.root = root.delete() orelse null;
            const data: *Data = @ptrCast(@alignCast(root));
            self.allocator.destroy(data);
        }
    }
};

fn verifyNode(parent: ?*Node, n: ?*Node) !void {
    const node = n orelse return;

    try testing.expectEqual(parent, node.parent);
    try verifyNode(node, node.left);
    try verifyNode(node, node.right);

    // Verify count
    const expected_count = 1 + avl.count(node.left) + avl.count(node.right);
    try testing.expectEqual(expected_count, node.count);

    // Verify height balance
    const l_height = avl.height(node.left);
    const r_height = avl.height(node.right);
    try testing.expect(l_height == r_height or l_height + 1 == r_height or l_height == r_height + 1);
    try testing.expectEqual(1 + @max(l_height, r_height), node.height);

    // Verify BST property
    const data_ptr: *Data = @ptrCast(@alignCast(node));
    if (node.left) |left| {
        try testing.expectEqual(node, left.parent);
        const left_data: *Data = @ptrCast(@alignCast(left));
        try testing.expect(left_data.val <= data_ptr.val);
    }
    if (node.right) |right| {
        try testing.expectEqual(node, right.parent);
        const right_data: *Data = @ptrCast(@alignCast(right));
        try testing.expect(right_data.val >= data_ptr.val);
    }
}

const InorderIterator = struct {
    current: ?*Node,
    stack: std.ArrayList(*Node),

    pub fn init(allocator: std.mem.Allocator) InorderIterator {
        return .{
            .current = null,
            .stack = std.ArrayList(*Node).init(allocator),
        };
    }

    pub fn deinit(self: *InorderIterator) void {
        self.stack.deinit();
    }

    pub fn next(self: *InorderIterator) ?*Node {
        while (self.current) |node| {
            self.stack.append(node) catch return null;
            self.current = node.left;
        }

        const node = self.stack.pop() orelse return null;
        self.current = node.right;
        return node;
    }
};

fn verifyContainer(c: *const Container, values: []const u32) !void {
    try verifyNode(null, c.root);
    try testing.expectEqual(values.len, avl.count(c.root));

    // Verify in-order traversal matches sorted values
    const sorted = try testing.allocator.dupe(u32, values);
    defer testing.allocator.free(sorted);
    std.sort.insertion(u32, sorted, {}, comptime std.sort.asc(u32));

    var i: usize = 0;
    var it = InorderIterator.init(testing.allocator);
    defer it.deinit();
    it.current = c.root;
    while (it.next()) |node| {
        const data_ptr: *Data = @ptrCast(@alignCast(node));
        try testing.expectEqual(sorted[i], data_ptr.val);
        i += 1;
    }
}

test "basic operations" {
    var c = Container.init(testing.allocator);
    defer c.deinit();

    // Empty tree verification
    try verifyContainer(&c, &[_]u32{});

    // Single insert
    try c.add(123);
    try verifyContainer(&c, &[_]u32{123});

    // Failed delete
    try testing.expect(!try c.delete(124));

    // Successful delete
    try testing.expect(try c.delete(123));
    try verifyContainer(&c, &[_]u32{});
}

test "sequential insertion" {
    var c = Container.init(testing.allocator);
    defer c.deinit();

    var values = std.ArrayList(u32).init(testing.allocator);
    defer values.deinit();

    var i: u32 = 0;
    while (i < 100) : (i += 3) {
        try c.add(i);
        try values.append(i);
        try verifyContainer(&c, values.items);
    }
}

test "random insertion and deletion" {
    var c = Container.init(testing.allocator);
    defer c.deinit();

    var values = std.ArrayList(u32).init(testing.allocator);
    defer values.deinit();

    // Random insertions
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        const val = i % 1000; // Simple deterministic "random" values
        try c.add(val);
        try values.append(val);
        try verifyContainer(&c, values.items);
    }

    // Random deletions
    i = 0;
    while (i < 50) : (i += 1) {
        const val = i % 1000; // Try to delete values in the same pattern
        if (try c.delete(val)) {
            // Find and remove the value from our reference array
            for (values.items, 0..) |v, idx| {
                if (v == val) {
                    _ = values.orderedRemove(idx);
                    break;
                }
            }
        }
        try verifyContainer(&c, values.items);
    }
}

test "edge cases" {
    var c = Container.init(testing.allocator);
    defer c.deinit();

    // Test inserting duplicates
    try c.add(1);
    try c.add(1);
    try verifyContainer(&c, &[_]u32{ 1, 1 });

    // Test deleting from single-child nodes
    try c.add(2);
    try testing.expect(try c.delete(1));
    try verifyContainer(&c, &[_]u32{ 1, 2 });

    // Test deleting root
    try testing.expect(try c.delete(2));
    try testing.expect(try c.delete(1));
    try verifyContainer(&c, &[_]u32{});
}
