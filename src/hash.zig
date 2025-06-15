const std = @import("std");
const assert = std.debug.assert;

pub const LookupFn = *const fn (*Node, *Node) bool;
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
                return from; // pointer‐to‐slot, for deletion
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
};

pub const Map = struct {
    newer: Table = .{},
    older: Table = .{},
    migrate_pos: usize = 0,

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
