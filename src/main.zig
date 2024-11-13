const std = @import("std");
const storage = @import("./storage.zig");

pub fn main() void {
    std.debug.print("Hello, world!\n", .{});
}

test {
    // see tests for current functionality:
    _ = storage;
}
