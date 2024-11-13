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

    const test_exe = b.addTest(.{
        .root_source_file = b.path("./src/main.zig"),
        .target = target,
        .optimize = opt,
    });

    const run_test_exe = b.addRunArtifact(test_exe);
    const test_step = b.step("test", "run unit tests");
    test_step.dependOn(&run_test_exe.step);
}
