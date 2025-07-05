const std = @import("std");
const assert = std.debug.assert;

pub const LookupFn = *const fn (*Node, *Node) bool;
pub const ForEachFn = *const fn (*Node, ?*anyopaque) bool;
const max_load_factor = 8;
const rehashing_work = 128;

pub const Node = struct {
    next: ?*Node = null,
    code: u64 = 0,
};

pub const Table = struct {
    slots: ?[]?*Node = null,
    // power of 2 array size, 2^n - 1
    mask: usize = 0,
    // number of keys
    size: usize = 0,

    fn init(self: *Table, size: usize) !void {
        // must be a power of two
        assert(size > 0 and (size - 1) & size == 0);
        self.slots = try std.heap.page_allocator.alloc(?*Node, size);
        // Initialize all slots to null
        for (self.slots.?) |*slot| {
            slot.* = null;
        }
        self.mask = size - 1;
        self.size = 0;
    }

    fn deinit(self: *Table) void {
        if (self.slots) |slots| std.heap.page_allocator.free(slots);
    }

    fn insert(self: *Table, node: *Node) void {
        const pos = node.code & self.mask;
        const next = self.slots.?[pos];
        node.next = next;
        self.slots.?[pos] = node;
        self.size += 1;
    }

    fn lookup(self: *Table, key: *Node, eq: LookupFn) ?*?*Node {
        if (self.slots == null) return null;

        const pos = key.code & self.mask;
        // incoming pointer to the target
        var from = &self.slots.?[pos];

        while (true) {
            // load the current entry
            const curOpt = from.*;
            if (curOpt == null) break;
            const cur = curOpt.?;

            // match hash + key
            if (cur.code == key.code and eq(cur, key)) {
                return from; // may be a node, may be a slot
            }

            // advance to next pointer‐slot
            from = &cur.next;
        }
        return null;
    }

    fn detach(self: *Table, from: *?*Node) *Node {
        // the target node
        const node = from.*.?;
        // update the incoming pointer to the target
        from.* = node.next;
        self.size -= 1;
        return node;
    }

    // Helper function to iterate over a single table
    fn forEach(self: *Table, cb: ForEachFn, arg: ?*anyopaque) bool {
        if (self.slots) |slots| {
            var i: usize = 0;
            while (i <= self.mask) : (i += 1) {
                var node = slots[i];
                while (node) |current| {
                    if (!cb(current, arg)) return false;
                    node = current.next;
                }
            }
        }
        return true;
    }
};

pub const Map = struct {
    newer: Table = .{},
    older: Table = .{},
    migrate_pos: usize = 0,

    pub fn init(allocator: std.mem.Allocator) !*Map {
        const map = try allocator.create(Map);
        map.newer = .{};
        map.older = .{};
        try map.newer.init(4);
        return map;
    }

    pub fn deinit(self: *Map, allocator: std.mem.Allocator) void {
        self.newer.deinit();
        self.older.deinit();
        allocator.destroy(self);
    }

    pub fn lookup(self: *Map, key: *Node, eq: LookupFn) ?*Node {
        // migrate some keys
        self.helpRehashing();

        var from = self.newer.lookup(key, eq);
        if (from == null) {
            from = self.older.lookup(key, eq);
        }

        return if (from) |f| f.* else null;
    }

    pub fn delete(self: *Map, key: *Node, eq: LookupFn) ?*Node {
        // migrate some keys
        self.helpRehashing();

        if (self.newer.lookup(key, eq)) |from| {
            return self.newer.detach(from);
        }
        if (self.older.lookup(key, eq)) |from| {
            return self.older.detach(from);
        }
        return null;
    }

    pub fn insert(self: *Map, key: *Node) !void {
        if (self.newer.slots == null) {
            try self.newer.init(4);
        }

        self.newer.insert(key);
        if (self.older.slots == null) {
            const threshold = (self.newer.mask + 1) * max_load_factor;
            if (self.newer.size >= threshold) {
                try self.triggerRehashing();
            }
        }
        // migrate some keys
        self.helpRehashing();
    }

    /// Iterates over all nodes in both tables, calling the callback function for each node.
    /// The callback function should return true to continue iteration, or false to stop.
    /// Returns true if iteration completed, false if it was stopped early by the callback.
    pub fn forEach(self: *Map, cb: ForEachFn, arg: ?*anyopaque) bool {
        // First try the newer table
        if (!self.newer.forEach(cb, arg)) return false;
        // Then the older table if it exists
        if (self.older.slots != null) {
            return self.older.forEach(cb, arg);
        }
        return true;
    }

    pub fn size(self: *Map) usize {
        return self.newer.size + self.older.size;
    }

    fn triggerRehashing(self: *Map) !void {
        self.older = self.newer;
        try self.newer.init((self.newer.mask + 1) * 2);
        self.migrate_pos = 0;
    }

    fn helpRehashing(self: *Map) void {
        var work: usize = 0;
        while (work < rehashing_work and self.older.size > 0) {
            // find a non-empty slot
            const from = &self.older.slots.?[self.migrate_pos];
            if (from.* == null) {
                self.migrate_pos += 1;
                // empty slot
                continue;
            }
            // move the first list item to the newer table
            self.newer.insert(self.older.detach(from));
            work += 1;
        }

        // discard the old table if done
        if (self.older.size == 0 and self.older.slots != null) {
            self.older.deinit();
            self.older.slots = null;
        }
    }
};
