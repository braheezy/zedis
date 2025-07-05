const std = @import("std");

pub const WorkFn = *const fn (arg: anytype) void;

pub const Work = struct {
    f: *const fn (*anyopaque) void,
    arg: *anyopaque,

    pub fn init(func: *const fn (*anyopaque) void, argument: *anyopaque) Work {
        return .{
            .f = func,
            .arg = argument,
        };
    }
};

pub const ThreadPool = struct {
    const Self = @This();

    threads: []std.Thread,
    queue: std.ArrayList(Work),
    mutex: std.Thread.Mutex,
    not_empty: std.Thread.Condition,

    allocator: std.mem.Allocator,

    fn worker(self: *Self) void {
        while (true) {
            self.mutex.lock();

            // wait for the condition: a non-empty queue
            while (self.queue.items.len == 0) {
                self.not_empty.wait(&self.mutex);
            }

            // got the job
            const w = self.queue.orderedRemove(0);
            self.mutex.unlock();

            // do the work
            w.f(w.arg);
        }
    }

    pub fn init(allocator: std.mem.Allocator, num_threads: usize) !Self {
        std.debug.assert(num_threads > 0);

        const threads = try allocator.alloc(std.Thread, num_threads);
        errdefer allocator.free(threads);

        var self = Self{
            .threads = threads,
            .queue = std.ArrayList(Work).init(allocator),
            .mutex = std.Thread.Mutex{},
            .not_empty = std.Thread.Condition{},
            .allocator = allocator,
        };

        // Create the worker threads
        for (threads) |*thread| {
            thread.* = try std.Thread.spawn(.{}, worker, .{&self});
        }

        return self;
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.threads);
        self.queue.deinit();
    }

    pub fn queueWork(self: *Self, func: *const fn (*anyopaque) void, arg: *anyopaque) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        self.queue.append(Work{ .f = func, .arg = arg }) catch unreachable;
        self.not_empty.signal();
    }
};
