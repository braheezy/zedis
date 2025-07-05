pub const Item = struct {
    val: u64 = 0,
    ref: ?*usize = null,
};

pub fn parent(i: usize) usize {
    return (i + 1) / 2 - 1;
}

pub fn left(i: usize) usize {
    return 2 * i + 1;
}

pub fn right(i: usize) usize {
    return 2 * i + 2;
}

pub fn up(heap: []Item, pos: usize) void {
    const t = heap[pos];
    var p = pos;
    while (p > 0 and heap[parent(p)].val > t.val) {
        // swap the parent
        heap[p] = heap[parent(p)];
        if (heap[p].ref) |ref| {
            ref.* = p;
        }
        p = parent(p);
    }
    heap[p] = t;
    if (heap[p].ref) |ref| {
        ref.* = p;
    }
}

pub fn down(heap: []Item, pos: usize) void {
    const t = heap[pos];
    var p = pos;
    while (true) {
        // find the smallest one among the parent and their kids
        const l = left(p);
        const r = right(p);
        var min_pos = p;
        var min_val = t.val;
        if (l < heap.len and heap[l].val < min_val) {
            min_pos = l;
            min_val = heap[l].val;
        }
        if (r < heap.len and heap[r].val < min_val) {
            min_pos = r;
        }
        if (min_pos == p) break;
        // swap with the kid
        heap[p] = heap[min_pos];
        if (heap[p].ref) |ref| {
            ref.* = p;
        }
        p = min_pos;
    }
    heap[p] = t;
    if (heap[p].ref) |ref| {
        ref.* = p;
    }
}

pub fn update(heap: []Item, pos: usize) void {
    if (pos > 0 and heap[parent(pos)].val > heap[pos].val) {
        up(heap, pos);
    } else {
        down(heap, pos);
    }
}

const std = @import("std");
const testing = std.testing;

const Data = struct {
    heap_idx: usize = std.math.maxInt(usize),
};

const Container = struct {
    heap: std.ArrayList(Item),
    map: std.AutoHashMap(u64, *Data),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) Container {
        return .{
            .heap = std.ArrayList(Item).init(allocator),
            .map = std.AutoHashMap(u64, *Data).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Container) void {
        var it = self.map.valueIterator();
        while (it.next()) |data| {
            self.allocator.destroy(data.*);
        }
        self.heap.deinit();
        self.map.deinit();
    }

    pub fn add(self: *Container, val: u64) !void {
        // First check if value already exists
        if (self.map.get(val)) |_| {
            return; // Value already exists
        }

        const d = try self.allocator.create(Data);
        errdefer self.allocator.destroy(d);
        d.* = .{};

        try self.map.put(val, d);
        errdefer _ = self.map.remove(val);

        const idx = self.heap.items.len;
        d.heap_idx = idx;

        try self.heap.append(.{
            .ref = &d.heap_idx,
            .val = val,
        });

        if (idx > 0) {
            up(self.heap.items, idx);
        }
    }

    pub fn del(self: *Container, val: u64) !void {
        const d = self.map.get(val) orelse return error.NotFound;

        // Verify heap integrity
        testing.expect(self.heap.items[d.heap_idx].val == val) catch unreachable;
        testing.expect(self.heap.items[d.heap_idx].ref == &d.heap_idx) catch unreachable;

        const last_idx = self.heap.items.len - 1;
        if (d.heap_idx != last_idx) {
            // Move last element to the deleted position
            self.heap.items[d.heap_idx] = self.heap.items[last_idx];
            // Update the moved element's reference
            if (self.heap.items[d.heap_idx].ref) |ref| {
                ref.* = d.heap_idx;
            }
        }

        _ = self.heap.pop();

        // Update heap if necessary
        if (d.heap_idx < self.heap.items.len) {
            update(self.heap.items, d.heap_idx);
        }

        // Clean up - remove from map first, then free memory
        _ = self.map.remove(val);
        self.allocator.destroy(d);
    }

    pub fn verify(self: *Container) !void {
        try testing.expectEqual(self.heap.items.len, self.map.count());

        for (self.heap.items, 0..) |item, i| {
            const l = left(i);
            const r = right(i);

            if (l < self.heap.items.len) {
                try testing.expect(self.heap.items[l].val >= item.val);
            }
            if (r < self.heap.items.len) {
                try testing.expect(self.heap.items[r].val >= item.val);
            }
            try testing.expect(item.ref.?.* == i);
        }
    }
};

test "heap operations with various sizes" {
    const allocator = testing.allocator;

    // Test smaller range more thoroughly
    {
        var sz: usize = 0;
        while (sz < 10) : (sz += 1) {
            // Test adding values
            {
                var j: u32 = 0;
                while (j < 2 + sz * 2) : (j += 1) {
                    var c = Container.init(allocator);
                    defer c.deinit();

                    // Add initial elements
                    var i: u32 = 0;
                    while (i < sz) : (i += 1) {
                        try c.add(1 + i * 2);
                    }
                    try c.verify();

                    // Add test value
                    try c.add(j);
                    try c.verify();
                }
            }

            // Test removing values
            {
                var j: u32 = 0;
                while (j < sz) : (j += 1) {
                    var c = Container.init(allocator);
                    defer c.deinit();

                    // Add initial elements
                    var i: u32 = 0;
                    while (i < sz) : (i += 1) {
                        try c.add(i);
                    }
                    try c.verify();

                    // Remove test value
                    try c.del(j);
                    try c.verify();
                }
            }
        }
    }

    // Test larger sizes with fewer iterations
    {
        const sizes = [_]usize{ 10, 20, 50, 100 };
        for (sizes) |sz| {
            var c = Container.init(allocator);
            defer c.deinit();

            // Add elements in ascending order
            var i: u32 = 0;
            while (i < sz) : (i += 1) {
                try c.add(i);
                try c.verify();
            }

            // Add elements in descending order
            i = @intCast(sz);
            while (i > 0) : (i -= 1) {
                try c.add(sz + i);
                try c.verify();
            }

            // Remove every other element
            i = 0;
            while (i < sz) : (i += 2) {
                try c.del(i);
                try c.verify();
            }
        }
    }
}
