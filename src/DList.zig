pub const DList = @This();
prev: ?*DList = null,
next: ?*DList = null,

pub fn init(node: *DList) void {
    node.prev = node;
    node.next = node;
}

pub fn empty(node: *DList) bool {
    return node.next == node;
}

pub fn detach(node: *DList) void {
    const prev = node.prev;
    const next = node.next;
    prev.?.next = next;
    next.?.prev = prev;
}

pub fn insert_before(target: *DList, rookie: *DList) void {
    const prev = target.prev;
    prev.?.next = rookie;
    rookie.prev = prev;
    rookie.next = target;
    target.prev = rookie;
}
