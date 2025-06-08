const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const server_mod = b.createModule(.{
        .root_source_file = b.path("src/server.zig"),
        .target = target,
        .optimize = optimize,
    });

    const server_exe = b.addExecutable(.{
        .name = "server",
        .root_module = server_mod,
    });

    const client_mod = b.createModule(.{
        .root_source_file = b.path("src/client.zig"),
        .target = target,
        .optimize = optimize,
    });

    const client_exe = b.addExecutable(.{
        .name = "client",
        .root_module = client_mod,
    });

    b.installArtifact(server_exe);
    b.installArtifact(client_exe);

    const run_server_cmd = b.addRunArtifact(server_exe);
    run_server_cmd.step.dependOn(b.getInstallStep());
    const run_client_cmd = b.addRunArtifact(client_exe);
    run_client_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_server_cmd.addArgs(args);
        run_client_cmd.addArgs(args);
    }
    const run_server_step = b.step("server", "Run the server");
    const run_client_step = b.step("client", "Run the client");
    run_server_step.dependOn(&run_server_cmd.step);
    run_client_step.dependOn(&run_client_cmd.step);
}
