const std = @import("std");
const avl = @import("avl.zig");
const hash = @import("hash.zig");

const stringHash = @import("root.zig").stringHash;

pub const Set = struct {
    // index by (score, name)
    root: ?*avl.Node = null,
    // index by name
    map: hash.Map,

    pub fn clear(self: *Set, allocator: std.mem.Allocator) void {
        self.map.deinit(allocator);
    }

    // insert into the AVL tree
    fn treeInsert(self: *Set, node: *Node) void {
        // insert under this node
        var parent: ?*avl.Node = null;
        // the incoming pointer to the next node
        var from = &self.root;
        // tree search
        while (from.*) |next| {
            parent = next;
            from = if (lessNode(&node.tree, parent.?)) &parent.?.left else &parent.?.right;
        }
        // attach the new node
        from.* = &node.tree;
        node.tree.parent = parent;
        self.root = node.tree.fix();
    }

    // update the score of an existing node
    fn update(self: *Set, node: *Node, score: f64) void {
        if (node.score == score) {
            return;
        }
        // detach the tree node
        self.root = node.tree.delete();
        node.tree = avl.Node{};
        // reinsert the tree node
        node.score = score;
        self.treeInsert(node);
    }

    // add a new (score, name) tuple, or update the score of the existing tuple
    pub fn insert(self: *Set, allocator: std.mem.Allocator, name: []const u8, score: f64) !bool {
        var node = self.lookup(name);
        if (node) |n| {
            self.update(n, score);
            return false;
        } else {
            node = try Node.init(allocator, name, score);
            try self.map.insert(&node.?.map);
            self.treeInsert(node.?);
            return true;
        }
    }

    // lookup by name
    pub fn lookup(self: *Set, name: []const u8) ?*Node {
        if (self.root) |_| {
            var key = Key{
                .node = .{ .next = null, .code = stringHash(name) },
                .name = name,
                .len = name.len,
            };
            const found = self.map.lookup(&key.node, cmp);
            return if (found) |node| @fieldParentPtr("map", node) else null;
        } else {
            return null;
        }
    }

    // delete a node
    pub fn delete(self: *Set, allocator: std.mem.Allocator, node: *Node) void {
        // remove from the hashtable
        var key = Key{
            .node = .{ .next = null, .code = node.map.code },
            .name = node.name[0..node.len],
            .len = node.len,
        };
        const found = self.map.delete(&key.node, cmp);
        if (found == null) unreachable;
        // remove from the tree
        self.root = node.tree.delete();
        // deallocate the node
        node.deinit(allocator);
    }

    // find the first (score, name) tuple that is >= key.
    pub fn seekGe(self: *Set, score: f64, name: []const u8) ?*Node {
        var found: ?*avl.Node = null;
        var node = self.root;
        while (node) |n| {
            if (lessByKey(n, score, name)) {
                // node < key
                node = n.right;
            } else {
                // candidate
                found = n;
                node = n.left;
            }
        }
        return if (found) |n| @fieldParentPtr("tree", n) else null;
    }
};

pub const Node = struct {
    tree: avl.Node,
    map: hash.Node,
    score: f64 = 0,
    len: usize = 0,
    // pointer to the first byte of the name stored immediately after this struct
    name: [*]u8,

    pub fn init(allocator: std.mem.Allocator, name: []const u8, score: f64) !*Node {
        const header_size = @sizeOf(Node);
        const total_size = header_size + name.len;
        // allocate contiguous memory for Node + name bytes
        const buf = try allocator.alloc(u8, total_size);
        const self: *Node = @ptrCast(@alignCast(buf.ptr));

        // initialise fields
        self.tree = avl.Node{};
        self.map.next = null;
        self.map.code = stringHash(name);
        self.score = score;
        self.len = name.len;

        // copy name bytes into the tail region right after the struct header
        const tail_slice = buf[header_size .. header_size + name.len];
        @memcpy(tail_slice, name);
        self.name = tail_slice.ptr;

        return self;
    }

    pub fn deinit(self: *Node, allocator: std.mem.Allocator) void {
        const total_size = @sizeOf(Node) + self.len;
        const bytes_ptr: [*]u8 = @ptrCast(@alignCast(self));
        const bytes = bytes_ptr[0..total_size];
        allocator.free(bytes);
    }
};

// offset into the succeeding or preceding node.
pub fn offset(node: ?*Node, off: isize) ?*Node {
    const tnode = if (node) |n| n.tree.offset(off) else null;
    return if (tnode) |n| @fieldParentPtr("tree", n) else null;
}

// compare by the (score, name) tuple
fn lessByKey(lhs: *avl.Node, score: f64, name: []const u8) bool {
    const zl: *Node = @fieldParentPtr("tree", lhs);
    if (zl.score != score) return zl.score < score;
    // Compare the common length of both strings
    const min_len = @min(zl.len, name.len);
    if (min_len > 0) {
        const zl_slice = zl.name[0..zl.len];
        for (zl_slice[0..min_len], name[0..min_len]) |a, b| {
            if (a != b) return a < b;
        }
    }
    // If the common prefix is equal, shorter string is less
    return zl.len < name.len;
}

fn lessNode(
    lhs: *avl.Node,
    rhs: *avl.Node,
) bool {
    const zr: *Node = @fieldParentPtr("tree", rhs);
    return lessByKey(lhs, zr.score, zr.name[0..zr.len]);
}

const Key = struct {
    node: hash.Node = .{},
    name: []const u8 = &[_]u8{},
    len: usize = 0,
};

fn cmp(node: *hash.Node, key: *hash.Node) bool {
    const znode: *Node = @fieldParentPtr("map", node);
    const zkey: *Key = @fieldParentPtr("node", key);
    if (znode.len != zkey.len) return false;
    return std.mem.eql(u8, znode.name[0..znode.len], zkey.name[0..zkey.len]);
}
