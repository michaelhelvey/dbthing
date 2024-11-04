const std = @import("std");

pub fn build(b: *std.Build) void {
    const opt = b.standardOptimizeOption(.{});
    const target = b.standardTargetOptions(.{});

    const exe = b.addExecutable(.{
        .name = "db",
        .root_source_file = b.path("./src/main.zig"),
        .target = target,
        .optimize = opt,
    });

    b.installArtifact(exe);
}
