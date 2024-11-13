//--------------------------------------------------------------------------------------------------
// dbthing storage engine
//
// the storage engine follows a traditional RDMS B-tree model.  Data is stored in segment files,
// which are split into 4KB pages for atomic writes, which are split into a list of tuples.
//
// pages are formatted as follows:
//
// --------------------------------------------------------------------
// | header | line pointers |   ....... free space ........ | tuples  |
// --------------------------------------------------------------------
//
// the line pointers contain the offsets of each tuple, and tuples grow backwards from the end of
// the page.
//
// both table data and index data is stored in this format.
//--------------------------------------------------------------------------------------------------

const std = @import("std");
const assert = std.debug.assert;

const PAGE_SIZE: comptime_int = 4 * 1024;

fn structEncoder(comptime T: type) type {
    return struct {
        /// Encodes the type into the buffer pointed to by `dest`.  The memory at which the struct
        /// is stored may overlap with `dest`.
        fn encode(self: *const T, dest: []u8) void {
            std.mem.copyForwards(u8, dest, std.mem.asBytes(self));
        }

        /// Interprets the first N bytes of `src` as the struct T.
        fn decode(src: []const u8) *T {
            // all this garbage makes me feel like I'm doing something wrong but
            // 1) alignCast to ensure that the pointer is aligned correct
            // 2) ptrCast to cast to a *const T
            // 3) constCast to remove const-ness so we can write into the memory
            return @constCast(@ptrCast(@alignCast(src[0..@sizeOf(T)])));
        }
    };
}

pub const TupleFlags = packed struct {
    dead: bool,
};

/// Represents the bytes at the start of each tuple
pub const TupleHeader = packed struct {
    len: u31, // keep header size to 4 bytes
    flags: TupleFlags,

    const Self = @This();
    const serde = structEncoder(Self);

    pub const encode = serde.encode;
    pub const decode = serde.decode;

    test "tuple header encoding" {
        try std.testing.expectEqual(@sizeOf(Self), 4);
        // build-time sanity check that our tuple header can fit the data sizes we expect:
        try std.testing.expect(std.math.maxInt(u31) > PAGE_SIZE);

        const header = TupleHeader{ .len = 123, .flags = .{ .dead = true } };
        var buf: [12]u8 = undefined;

        header.encode(&buf);
        const newHeader = TupleHeader.decode(&buf);

        try std.testing.expectEqual(newHeader.len, header.len);
        try std.testing.expectEqual(newHeader.flags.dead, header.flags.dead);
    }
};

/// Represents the bytes at the start of each page, containing information required to parse the
/// following page.
pub const PageHeader = packed struct {
    version: u8,
    size: u32,
    free_start: u32,
    free_end: u32,
    checksum: u32, // ignored for now

    const Self = @This();
    const serde = structEncoder(Self);

    pub const encode = serde.encode;
    pub const decode = serde.decode;

    pub fn freeSpace(self: Self) u32 {
        return self.free_end - self.free_start;
    }
};

// Writes a single page of data at a specific offset into a file, returning an error if we cannot
// do so atomically.
fn checkedPageWrite(file: std.fs.File, data: []const u8, offset: u64) !void {
    const r = try file.pwrite(data, offset);
    if (r != PAGE_SIZE) {
        return error.NonAtomicWrite;
    }
}

pub const Segment = struct {
    // TODO: the allocations here are quite messy, but I'm putting off fixing it until I write a
    // buffer pool implementation to hold pages.
    allocator: std.mem.Allocator,
    file: std.fs.File,

    const Self = @This();

    const LinePointer = packed struct {
        offset: u32,
        len: u32,

        const serde = structEncoder(LinePointer);

        pub const encode = serde.encode;
        pub const decode = serde.decode;
    };

    pub const TupleEntry = struct {
        header: *TupleHeader,
        data: []u8,
    };

    const PageEntry = struct {
        data: []u8,
        header: *PageHeader,

        fn init(data: []u8) PageEntry {
            assert(data.len == PAGE_SIZE);
            const header = PageHeader.decode(data);

            return .{ .data = data, .header = header };
        }

        // Updates `self.data` with the tuple, including updating the header and line pointer array.
        // Does _not_ actually write out the file, as a PageEntry does not own the buffer it
        // manipulates.
        fn writeTuple(self: *PageEntry, tuple: []const u8) void {
            assert(self.header.freeSpace() >= tuple.len);

            const dataLen = @sizeOf(TupleHeader) + tuple.len;
            const tupleHeader: TupleHeader = .{
                .flags = .{ .dead = false },
                .len = @intCast(tuple.len),
            };

            // first available location where we _could_ put the tuple
            var ptr = self.header.free_end - dataLen;
            // ensure correct alignment:
            const alignment: u32 = @intCast(@alignOf(TupleHeader));
            ptr = ptr & ~(alignment - 1);

            const linePointer: LinePointer = .{
                .offset = @intCast(ptr),
                .len = @intCast(dataLen),
            };

            // 1. Write the tuple data at the end of free space:
            const allocatedSpace = self.data[linePointer.offset..self.header.free_end];
            std.mem.copyForwards(u8, allocatedSpace, std.mem.asBytes(&tupleHeader));
            const dataSpace = allocatedSpace[@sizeOf(TupleHeader)..];
            std.mem.copyForwards(u8, dataSpace, tuple);

            // 2. Update the line pointer array:
            // FIXME: works but is sloppy with alignment if we change the size of these structs
            linePointer.encode(self.data[self.header.free_start..]);

            // 3. Update the page header (don't need to encode since header _is_ a pointer into
            // buffer, we've just cast it to a struct)
            self.header.free_start += @sizeOf(LinePointer);
            self.header.free_end = linePointer.offset;
        }

        fn readTuple(self: PageEntry, lp: *const LinePointer) TupleEntry {
            const tupleEnd = lp.offset + lp.len;
            const data = self.data[lp.offset..tupleEnd];
            const header = TupleHeader.decode(data);
            const tupleData = data[@sizeOf(TupleHeader)..];

            return .{
                .header = header,
                .data = tupleData,
            };
        }
    };

    const PageIterator = struct {
        segment: Segment,
        seg_off: u32,
        buffer: []u8,

        fn init(segment: Segment, buffer: []u8) PageIterator {
            assert(buffer.len == PAGE_SIZE);
            return .{ .segment = segment, .seg_off = 0, .buffer = buffer };
        }

        // Iterates pages by replacing the page at `buffer` with the next page of data, and
        // returning a `PageEntry` struct that interprets that data.  The `PageEntry` that is
        // returned is only valid until the next call to `next()`, as the backing memory for it is
        // replaced on each `next()` invocation.
        fn next(self: *PageIterator) !?PageEntry {
            const bytesRead = try self.segment.file.preadAll(self.buffer, self.seg_off);
            if (bytesRead == 0) {
                return null;
            }
            self.seg_off += PAGE_SIZE;
            return PageEntry.init(self.buffer);
        }
    };

    // Creates and initializes a new segment at a given path, returning an error if a file already
    // exists at that path.
    pub fn createAtPath(allocator: std.mem.Allocator, path: []const u8) !Self {
        const file = try std.fs.cwd().createFile(path, .{ .exclusive = true, .read = true });
        const page = try allocatePage(allocator);
        defer allocator.free(page.data);
        try checkedPageWrite(file, page.data, 0);
        return .{ .allocator = allocator, .file = file };
    }

    // Creates a new Segment struct with a given path, assuming that a valid segment file exists at
    // that location
    pub fn fromPath(allocator: std.mem.Allocator, path: []const u8) Self {
        const file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
        return .{ .allocator = allocator, .file = file };
    }

    pub fn deinit(self: Self) void {
        self.file.close();
    }

    pub fn writeTuple(self: Self, tuple: []const u8) !void {
        const maxTupleSize = PAGE_SIZE - @sizeOf(PageHeader) - @sizeOf(LinePointer);
        assert(tuple.len < maxTupleSize);

        const pageBuffer = try self.allocator.alloc(u8, PAGE_SIZE);
        defer self.allocator.free(pageBuffer);
        var it = PageIterator.init(self, pageBuffer);

        // This awkwardness feels like a zig problem...I should be able to use mutable loop capture
        // here, e.g. while (it.next()) |*page| ..., but I can't, because that returns a *const
        // PageEntry, see: https://github.com/ziglang/zig/issues/15086
        var i: u32 = 0;
        while (true) {
            i += 1;
            var page = try it.next();

            if (page) |*entry| {
                // If we do not have free space, advance to the next page
                if (entry.header.freeSpace() >= tuple.len) {
                    entry.writeTuple(tuple);
                    try checkedPageWrite(self.file, entry.data, it.seg_off - PAGE_SIZE);
                    break;
                }
            } else {
                // if we have advanced through all pages without writing, then we must allocate
                // a new page
                var entry = try allocatePage(self.allocator);
                entry.writeTuple(tuple);
                try checkedPageWrite(self.file, entry.data, it.seg_off);
                self.allocator.free(entry.data);
                break;
            }
        }
    }

    pub const TupleIterator = struct {
        iter: PageIterator,
        page_buffer: []u8,
        allocator: std.mem.Allocator,
        current_page: ?PageEntry,
        lp_off: usize,

        fn init(segment: Segment) !TupleIterator {
            const pageBuffer = try segment.allocator.alloc(u8, PAGE_SIZE);
            var iter = PageIterator.init(segment, pageBuffer);
            const first_entry = try iter.next();

            return .{
                .iter = iter,
                .page_buffer = pageBuffer,
                .allocator = segment.allocator,
                .current_page = first_entry,
                .lp_off = @sizeOf(PageHeader),
            };
        }

        fn deinit(self: *TupleIterator) void {
            self.allocator.free(self.page_buffer);
        }

        pub fn next(self: *TupleIterator) !?TupleEntry {
            if (self.current_page == null) {
                return null;
            }

            // 1. Check whether we need to advance to the next page:
            if (self.lp_off >= self.current_page.?.header.free_start) {
                self.current_page = try self.iter.next() orelse {
                    return null;
                };
                self.lp_off = @sizeOf(PageHeader);
            }

            // 2. Once we know that we are on the page that we need to be on, just read the
            // tuple at the provided offset
            const lp = LinePointer.decode(self.current_page.?.data[self.lp_off..]);
            self.lp_off += @sizeOf(LinePointer);
            return self.current_page.?.readTuple(lp);
        }
    };

    pub fn tupleIterator(self: Self) !TupleIterator {
        return try TupleIterator.init(self);
    }

    /// Reads a tuple at a given index into the page
    pub fn readTupleAtIndex(self: Self, page: usize, idx: u32, buffer: []u8) !TupleEntry {
        assert(buffer.len == PAGE_SIZE);

        const r = try self.file.preadAll(buffer, PAGE_SIZE * (page - 1));
        if (r != PAGE_SIZE) {
            return error.InvalidPage;
        }

        const entry = PageEntry.init(buffer);

        const lpOff = @sizeOf(PageHeader) + (@sizeOf(LinePointer) * idx);

        if (lpOff < @sizeOf(PageHeader) or lpOff > entry.header.free_start) {
            return error.InvalidOffset;
        }

        const lp = LinePointer.decode(entry.data[lpOff..]);
        return entry.readTuple(lp);
    }

    fn allocatePage(allocator: std.mem.Allocator) !PageEntry {
        const initialHeader: PageHeader = .{
            .version = 1,
            .size = PAGE_SIZE,
            .free_start = @sizeOf(PageHeader),
            .free_end = PAGE_SIZE,
            .checksum = 0,
        };

        const initialPageBuffer = try allocator.alloc(u8, PAGE_SIZE);
        initialHeader.encode(initialPageBuffer);

        return PageEntry.init(initialPageBuffer);
    }

    test "writing tuples" {
        const timestamp = std.time.timestamp();
        const fileName = try std.fmt.allocPrint(std.testing.allocator, "tmp_{d}", .{timestamp});
        defer std.testing.allocator.free(fileName);
        defer std.fs.cwd().deleteFile(fileName) catch unreachable;

        var segment = try Segment.createAtPath(std.testing.allocator, fileName);
        try segment.writeTuple("foobar");
        defer segment.deinit();

        var it = try segment.tupleIterator();
        defer it.deinit();

        var tuples = std.ArrayList(Segment.TupleEntry).init(std.testing.allocator);
        defer tuples.deinit();

        while (try it.next()) |tuple| {
            try tuples.append(tuple);
        }

        try std.testing.expectEqual(1, tuples.items.len);
        const firstEntry = tuples.items[0];

        try std.testing.expectEqual(firstEntry.header.len, "foobar".len);
        try std.testing.expectEqualSlices(u8, firstEntry.data, "foobar");

        const pageBuffer = try std.testing.allocator.alloc(u8, PAGE_SIZE);
        defer std.testing.allocator.free(pageBuffer);

        const entryByIndex = try segment.readTupleAtIndex(1, 0, pageBuffer);
        try std.testing.expectEqualSlices(u8, entryByIndex.data, "foobar");
    }
};

test {
    _ = TupleHeader;
    _ = Segment;
}
