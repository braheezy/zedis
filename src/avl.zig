pub const Node = struct {
    parent: ?*Node = null,
    left: ?*Node = null,
    right: ?*Node = null,
    height: u32 = 1,
    count: u32 = 1,

    // maintaine the height
    pub fn update(self: *Node) void {
        self.height = 1 + @max(height(self.left), height(self.right));
        self.count = 1 + count(self.left) + count(self.right);
    }

    fn rotateLeft(self: *Node) *Node {
        const parent = self.parent;
        const new_node = self.right;
        const inner = new_node.?.left;
        // self <-> inner
        self.right = inner;
        if (inner) |node| node.parent = self;
        // parent <-> new_node
        new_node.?.parent = parent;
        // new_node <-> self
        new_node.?.left = self;
        self.parent = new_node;
        // auxiliary data
        self.update();
        new_node.?.update();
        return new_node.?;
    }

    fn rotateRight(self: *Node) *Node {
        const parent = self.parent;
        const new_node = self.left;
        const inner = new_node.?.right;
        // self <-> inner
        self.left = inner;
        if (inner) |node| node.parent = self;
        // parent <-> new_node
        new_node.?.parent = parent;
        // new_node <-> self
        new_node.?.right = self;
        self.parent = new_node;
        // auxiliary data
        self.update();
        new_node.?.update();
        return new_node.?;
    }

    // the left subtree is taller by 2
    fn fixLeft(self: *Node) *Node {
        if (height(self.left.?.left) < height(self.left.?.right)) {
            self.left = self.left.?.rotateLeft();
        }
        return self.rotateRight();
    }

    // the right subtree is taller by 2
    fn fixRight(self: *Node) *Node {
        if (height(self.right.?.right) < height(self.right.?.left)) {
            self.right = self.right.?.rotateRight();
        }
        return self.rotateLeft();
    }

    // correct imbalanced nodes and maintain invariants until the root is reached
    pub fn fix(self: *Node) ?*Node {
        var current = self;
        while (true) {
            // save the fixed subtree
            var from: **Node = &current;
            const parent = current.parent;
            if (parent) |p| {
                // attach the fixed subtree to the parent
                from = if (p.left == current) &p.left.? else &p.right.?;
            }
            // auxiliary data
            current.update();
            // now fix the height difference of 2
            const l = height(current.left);
            const r = height(current.right);
            if (l == r + 2) {
                from.* = current.fixLeft();
            } else if (l + 2 == r) {
                from.* = current.fixRight();
            }
            // root node, stop
            if (parent == null) return from.*;
            // continue to the parent node because its height may be changed
            current = parent.?;
        }
    }

    fn deleteEasy(self: *Node) ?*Node {
        const child = if (self.left) |node| node else self.right;
        const parent = self.parent;
        // update the child's parent pointer
        if (child) |node| node.parent = parent;
        // attach the child to the grandparent
        if (parent == null) return child; // removing the root node
        if (parent.?.left) |left| {
            if (left == self) {
                parent.?.left = child;
            }
        }
        if (parent.?.right) |right| {
            if (right == self) {
                parent.?.right = child;
            }
        }
        // rebalance the updated tree
        if (parent.?.fix()) |fixed| {
            return fixed;
        }
        return child;
    }

    // detach a node and returns the new root of the tree
    pub fn delete(self: *Node) ?*Node {
        // the easy case of 0 or 1 child
        if (self.left == null or self.right == null) return self.deleteEasy();
        // find the successor
        var victim = self.right.?;
        while (victim.left) |left| {
            victim = left;
        }
        // detach the successor
        const new_root = victim.deleteEasy();
        // swap with the successor
        victim.* = self.*;
        if (victim.left) |left| {
            left.parent = victim;
        }
        if (victim.right) |right| {
            right.parent = victim;
        }
        // attach the successor to the parent
        if (self.parent) |p| {
            if (p.left == self) {
                p.left = victim;
            } else {
                p.right = victim;
            }
            victim.parent = p;
            return new_root;
        }
        // root node
        victim.parent = null;
        return victim;
    }
};

pub fn height(self: ?*Node) u32 {
    return if (self) |node| node.height else 0;
}

pub fn count(self: ?*Node) u32 {
    return if (self) |node| node.count else 0;
}
