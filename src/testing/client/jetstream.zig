//! JetStream Integration Tests
//!
//! End-to-end tests for JetStream stream/consumer CRUD,
//! publish with ack, and pull-based fetch.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const ServerManager = utils.ServerManager;

const js_port = utils.jetstream_port;

fn threadSleepNs(ns: u64) void {
    var ts: std.posix.timespec = .{
        .sec = @intCast(ns / 1_000_000_000),
        .nsec = @intCast(ns % 1_000_000_000),
    };
    _ = std.posix.system.nanosleep(&ts, &ts);
}

pub fn testStreamCreateAndInfo(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_stream_create",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    // Create stream
    var resp = js.createStream(.{
        .name = "TEST_CREATE",
        .subjects = &.{"test.create.>"},
        .storage = .memory,
    }) catch |err| {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "create failed: {}",
            .{err},
        ) catch "error";
        reportResult("js_stream_create", false, msg);
        return;
    };
    defer resp.deinit();

    if (resp.value.config) |cfg| {
        if (!std.mem.eql(
            u8,
            cfg.name,
            "TEST_CREATE",
        )) {
            reportResult(
                "js_stream_create",
                false,
                "wrong name",
            );
            return;
        }
    } else {
        reportResult(
            "js_stream_create",
            false,
            "no config",
        );
        return;
    }

    // Get stream info
    var info = js.streamInfo(
        "TEST_CREATE",
    ) catch {
        reportResult(
            "js_stream_create",
            false,
            "info failed",
        );
        return;
    };
    defer info.deinit();

    if (info.value.state) |state| {
        if (state.messages != 0) {
            reportResult(
                "js_stream_create",
                false,
                "expected 0 msgs",
            );
            return;
        }
    }

    // Cleanup
    var del = js.deleteStream(
        "TEST_CREATE",
    ) catch {
        reportResult(
            "js_stream_create",
            false,
            "delete failed",
        );
        return;
    };
    defer del.deinit();

    if (!del.value.success) {
        reportResult(
            "js_stream_create",
            false,
            "delete not success",
        );
        return;
    }

    reportResult("js_stream_create", true, "");
}

pub fn testPublishAndAck(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_publish_ack",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    // Create stream
    var stream = js.createStream(.{
        .name = "TEST_PUB",
        .subjects = &.{"test.pub.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_publish_ack",
            false,
            "create stream failed",
        );
        return;
    };
    defer stream.deinit();

    // Publish
    var ack = js.publish(
        "test.pub.hello",
        "hello world",
    ) catch |err| {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "publish failed: {}",
            .{err},
        ) catch "error";
        reportResult("js_publish_ack", false, msg);
        return;
    };
    defer ack.deinit();

    if (ack.value.seq != 1) {
        reportResult(
            "js_publish_ack",
            false,
            "expected seq 1",
        );
        return;
    }

    if (ack.value.stream) |s| {
        if (!std.mem.eql(u8, s, "TEST_PUB")) {
            reportResult(
                "js_publish_ack",
                false,
                "wrong stream",
            );
            return;
        }
    } else {
        reportResult(
            "js_publish_ack",
            false,
            "no stream in ack",
        );
        return;
    }

    // Publish second message
    var ack2 = js.publish(
        "test.pub.world",
        "second",
    ) catch {
        reportResult(
            "js_publish_ack",
            false,
            "publish 2 failed",
        );
        return;
    };
    defer ack2.deinit();

    if (ack2.value.seq != 2) {
        reportResult(
            "js_publish_ack",
            false,
            "expected seq 2",
        );
        return;
    }

    // Cleanup
    var del = js.deleteStream("TEST_PUB") catch {
        reportResult(
            "js_publish_ack",
            false,
            "delete failed",
        );
        return;
    };
    defer del.deinit();

    reportResult("js_publish_ack", true, "");
}

pub fn testConsumerCRUD(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_consumer_crud",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    // Create stream
    var stream = js.createStream(.{
        .name = "TEST_CONS",
        .subjects = &.{"test.cons.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_consumer_crud",
            false,
            "create stream failed",
        );
        return;
    };
    defer stream.deinit();

    // Create consumer
    var cons = js.createConsumer(
        "TEST_CONS",
        .{
            .name = "my-consumer",
            .durable_name = "my-consumer",
            .ack_policy = .explicit,
        },
    ) catch |err| {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "create consumer: {}",
            .{err},
        ) catch "error";
        reportResult(
            "js_consumer_crud",
            false,
            msg,
        );
        return;
    };
    defer cons.deinit();

    if (cons.value.name) |n| {
        if (!std.mem.eql(u8, n, "my-consumer")) {
            reportResult(
                "js_consumer_crud",
                false,
                "wrong consumer name",
            );
            return;
        }
    }

    // Consumer info
    var info = js.consumerInfo(
        "TEST_CONS",
        "my-consumer",
    ) catch {
        reportResult(
            "js_consumer_crud",
            false,
            "info failed",
        );
        return;
    };
    defer info.deinit();

    // Delete consumer
    var del_c = js.deleteConsumer(
        "TEST_CONS",
        "my-consumer",
    ) catch {
        reportResult(
            "js_consumer_crud",
            false,
            "delete consumer failed",
        );
        return;
    };
    defer del_c.deinit();

    if (!del_c.value.success) {
        reportResult(
            "js_consumer_crud",
            false,
            "delete not success",
        );
        return;
    }

    // Cleanup stream
    var del_s = js.deleteStream("TEST_CONS") catch {
        reportResult(
            "js_consumer_crud",
            false,
            "delete stream failed",
        );
        return;
    };
    defer del_s.deinit();

    reportResult("js_consumer_crud", true, "");
}

pub fn testApiError(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_api_error",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    // Try to get info for non-existent stream
    var info = js.streamInfo("NONEXISTENT");
    if (info) |*r| {
        r.deinit();
        reportResult(
            "js_api_error",
            false,
            "expected error",
        );
        return;
    } else |err| {
        if (err != error.ApiError) {
            reportResult(
                "js_api_error",
                false,
                "wrong error type",
            );
            return;
        }
    }

    // Check last API error
    if (js.lastApiError()) |api_err| {
        if (api_err.err_code !=
            nats.jetstream.errors.ErrCode.stream_not_found)
        {
            reportResult(
                "js_api_error",
                false,
                "wrong err_code",
            );
            return;
        }
    } else {
        reportResult(
            "js_api_error",
            false,
            "no last api error",
        );
        return;
    }

    reportResult("js_api_error", true, "");
}

pub fn testStreamNames(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_stream_names",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    // Create 3 streams
    var s1 = js.createStream(.{
        .name = "NAMES_A",
        .subjects = &.{"names.a.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_stream_names",
            false,
            "create A failed",
        );
        return;
    };
    defer s1.deinit();

    var s2 = js.createStream(.{
        .name = "NAMES_B",
        .subjects = &.{"names.b.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_stream_names",
            false,
            "create B failed",
        );
        return;
    };
    defer s2.deinit();

    var s3 = js.createStream(.{
        .name = "NAMES_C",
        .subjects = &.{"names.c.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_stream_names",
            false,
            "create C failed",
        );
        return;
    };
    defer s3.deinit();

    // List names
    var resp = js.streamNames() catch {
        reportResult(
            "js_stream_names",
            false,
            "list failed",
        );
        return;
    };
    defer resp.deinit();

    const names = resp.value.streams orelse {
        reportResult(
            "js_stream_names",
            false,
            "no streams",
        );
        return;
    };

    if (names.len < 3) {
        reportResult(
            "js_stream_names",
            false,
            "expected >= 3 streams",
        );
        return;
    }

    // Cleanup
    {
        var d1 = js.deleteStream("NAMES_A") catch {
            reportResult("js_stream_names", true, "");
            return;
        };
        d1.deinit();
    }
    {
        var d2 = js.deleteStream("NAMES_B") catch {
            reportResult("js_stream_names", true, "");
            return;
        };
        d2.deinit();
    }
    {
        var d3 = js.deleteStream("NAMES_C") catch {
            reportResult("js_stream_names", true, "");
            return;
        };
        d3.deinit();
    }

    reportResult("js_stream_names", true, "");
}

pub fn testStreamList(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_stream_list",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var s1 = js.createStream(.{
        .name = "LIST_A",
        .subjects = &.{"list.a.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_stream_list",
            false,
            "create failed",
        );
        return;
    };
    defer s1.deinit();

    var resp = js.streams() catch {
        reportResult(
            "js_stream_list",
            false,
            "list failed",
        );
        return;
    };
    defer resp.deinit();

    const streams = resp.value.streams orelse {
        reportResult(
            "js_stream_list",
            false,
            "no streams",
        );
        return;
    };

    if (streams.len < 1) {
        reportResult(
            "js_stream_list",
            false,
            "expected >= 1",
        );
        return;
    }

    // Verify we get StreamInfo with config
    var found = false;
    for (streams) |si| {
        if (si.config) |cfg| {
            if (std.mem.eql(u8, cfg.name, "LIST_A")) {
                found = true;
                break;
            }
        }
    }
    if (!found) {
        reportResult(
            "js_stream_list",
            false,
            "LIST_A not found",
        );
        return;
    }

    var d = js.deleteStream("LIST_A") catch {
        reportResult("js_stream_list", true, "");
        return;
    };
    d.deinit();

    reportResult("js_stream_list", true, "");
}

pub fn testConsumerNames(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_consumer_names",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var stream = js.createStream(.{
        .name = "CONS_NAMES",
        .subjects = &.{"cnames.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_consumer_names",
            false,
            "create stream failed",
        );
        return;
    };
    defer stream.deinit();

    var c1 = js.createConsumer(
        "CONS_NAMES",
        .{
            .name = "cons-alpha",
            .durable_name = "cons-alpha",
            .ack_policy = .explicit,
        },
    ) catch {
        reportResult(
            "js_consumer_names",
            false,
            "create cons failed",
        );
        return;
    };
    defer c1.deinit();

    var c2 = js.createConsumer(
        "CONS_NAMES",
        .{
            .name = "cons-beta",
            .durable_name = "cons-beta",
            .ack_policy = .explicit,
        },
    ) catch {
        reportResult(
            "js_consumer_names",
            false,
            "create cons2 failed",
        );
        return;
    };
    defer c2.deinit();

    var resp = js.consumerNames(
        "CONS_NAMES",
    ) catch {
        reportResult(
            "js_consumer_names",
            false,
            "list failed",
        );
        return;
    };
    defer resp.deinit();

    const names = resp.value.consumers orelse {
        reportResult(
            "js_consumer_names",
            false,
            "no consumers",
        );
        return;
    };

    if (names.len < 2) {
        reportResult(
            "js_consumer_names",
            false,
            "expected >= 2",
        );
        return;
    }

    var d = js.deleteStream("CONS_NAMES") catch {
        reportResult("js_consumer_names", true, "");
        return;
    };
    d.deinit();

    reportResult("js_consumer_names", true, "");
}

pub fn testConsumerList(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_consumer_list",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var stream = js.createStream(.{
        .name = "CONS_LIST",
        .subjects = &.{"clist.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_consumer_list",
            false,
            "create stream failed",
        );
        return;
    };
    defer stream.deinit();

    var c1 = js.createConsumer(
        "CONS_LIST",
        .{
            .name = "list-cons",
            .durable_name = "list-cons",
            .ack_policy = .explicit,
        },
    ) catch {
        reportResult(
            "js_consumer_list",
            false,
            "create cons failed",
        );
        return;
    };
    defer c1.deinit();

    var resp = js.consumers("CONS_LIST") catch {
        reportResult(
            "js_consumer_list",
            false,
            "list failed",
        );
        return;
    };
    defer resp.deinit();

    const consumers = resp.value.consumers orelse {
        reportResult(
            "js_consumer_list",
            false,
            "no consumers",
        );
        return;
    };

    if (consumers.len < 1) {
        reportResult(
            "js_consumer_list",
            false,
            "expected >= 1",
        );
        return;
    }

    // Verify ConsumerInfo has config
    if (consumers[0].config) |cfg| {
        if (cfg.name) |n| {
            if (!std.mem.eql(u8, n, "list-cons")) {
                reportResult(
                    "js_consumer_list",
                    false,
                    "wrong name",
                );
                return;
            }
        }
    } else {
        reportResult(
            "js_consumer_list",
            false,
            "no config",
        );
        return;
    }

    var d = js.deleteStream("CONS_LIST") catch {
        reportResult("js_consumer_list", true, "");
        return;
    };
    d.deinit();

    reportResult("js_consumer_list", true, "");
}

pub fn testAccountInfo(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_account_info",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var resp = js.accountInfo() catch {
        reportResult(
            "js_account_info",
            false,
            "request failed",
        );
        return;
    };
    defer resp.deinit();

    if (resp.value.limits == null) {
        reportResult(
            "js_account_info",
            false,
            "no limits",
        );
        return;
    }

    reportResult("js_account_info", true, "");
}

pub fn testMetadata(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_metadata",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    // Create stream + consumer
    var stream = js.createStream(.{
        .name = "TEST_META",
        .subjects = &.{"test.meta.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_metadata",
            false,
            "create stream failed",
        );
        return;
    };
    defer stream.deinit();

    var cons = js.createConsumer(
        "TEST_META",
        .{
            .name = "meta-cons",
            .durable_name = "meta-cons",
            .ack_policy = .explicit,
        },
    ) catch {
        reportResult(
            "js_metadata",
            false,
            "create consumer failed",
        );
        return;
    };
    defer cons.deinit();

    // Publish
    var ack = js.publish(
        "test.meta.hello",
        "metadata test",
    ) catch {
        reportResult(
            "js_metadata",
            false,
            "publish failed",
        );
        return;
    };
    defer ack.deinit();

    // Fetch and check metadata
    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_META",
        .consumer = "meta-cons",
    };

    var msg = (pull.next(5000) catch {
        reportResult(
            "js_metadata",
            false,
            "next failed",
        );
        return;
    }) orelse {
        reportResult(
            "js_metadata",
            false,
            "no message",
        );
        return;
    };
    defer msg.deinit();

    const md = msg.metadata() orelse {
        reportResult(
            "js_metadata",
            false,
            "no metadata",
        );
        return;
    };

    if (!std.mem.eql(u8, md.stream, "TEST_META")) {
        reportResult(
            "js_metadata",
            false,
            "wrong stream",
        );
        return;
    }
    if (!std.mem.eql(u8, md.consumer, "meta-cons")) {
        reportResult(
            "js_metadata",
            false,
            "wrong consumer",
        );
        return;
    }
    if (md.stream_seq != 1) {
        reportResult(
            "js_metadata",
            false,
            "expected seq 1",
        );
        return;
    }

    // Cleanup
    var d = js.deleteStream("TEST_META") catch {
        reportResult(
            "js_metadata",
            false,
            "delete failed",
        );
        return;
    };
    defer d.deinit();

    reportResult("js_metadata", true, "");
}

pub fn testFetchNoWait(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_fetch_no_wait",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var stream = js.createStream(.{
        .name = "TEST_NOWAIT",
        .subjects = &.{"test.nowait.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_fetch_no_wait",
            false,
            "create failed",
        );
        return;
    };
    defer stream.deinit();

    var cons = js.createConsumer(
        "TEST_NOWAIT",
        .{
            .name = "nowait-cons",
            .durable_name = "nowait-cons",
            .ack_policy = .explicit,
        },
    ) catch {
        reportResult(
            "js_fetch_no_wait",
            false,
            "create consumer failed",
        );
        return;
    };
    defer cons.deinit();

    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_NOWAIT",
        .consumer = "nowait-cons",
    };

    // Fetch no-wait on empty consumer -> 0 messages
    var result = pull.fetchNoWait(10) catch {
        reportResult(
            "js_fetch_no_wait",
            false,
            "fetchNoWait failed",
        );
        return;
    };
    defer result.deinit();

    if (result.count() != 0) {
        reportResult(
            "js_fetch_no_wait",
            false,
            "expected 0 messages",
        );
        return;
    }

    // Cleanup
    var d = js.deleteStream("TEST_NOWAIT") catch {
        reportResult(
            "js_fetch_no_wait",
            false,
            "delete failed",
        );
        return;
    };
    defer d.deinit();

    reportResult("js_fetch_no_wait", true, "");
}

pub fn testMessages(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_messages",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var stream = js.createStream(.{
        .name = "TEST_MSGS",
        .subjects = &.{"test.msgs.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_messages",
            false,
            "create stream failed",
        );
        return;
    };
    defer stream.deinit();

    var cons = js.createConsumer(
        "TEST_MSGS",
        .{
            .name = "msgs-cons",
            .durable_name = "msgs-cons",
            .ack_policy = .explicit,
        },
    ) catch {
        reportResult(
            "js_messages",
            false,
            "create consumer failed",
        );
        return;
    };
    defer cons.deinit();

    // Publish 5 messages
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        var a = js.publish(
            "test.msgs.data",
            "hello",
        ) catch {
            reportResult(
                "js_messages",
                false,
                "publish failed",
            );
            return;
        };
        a.deinit();
    }

    // Use messages iterator
    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_MSGS",
        .consumer = "msgs-cons",
    };

    var ctx = pull.messages(.{
        .max_messages = 10,
        .expires_ms = 5000,
    }) catch {
        reportResult(
            "js_messages",
            false,
            "messages() failed",
        );
        return;
    };
    defer ctx.deinit();

    var received: u32 = 0;
    while (received < 5) {
        var msg = (ctx.next() catch {
            break;
        }) orelse break;
        msg.ack() catch {};
        msg.deinit();
        received += 1;
    }

    if (received != 5) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "got {d}, expected 5",
            .{received},
        ) catch "count mismatch";
        reportResult("js_messages", false, m);
        return;
    }

    var d = js.deleteStream("TEST_MSGS") catch {
        reportResult("js_messages", true, "");
        return;
    };
    d.deinit();

    reportResult("js_messages", true, "");
}

pub fn testConsume(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_consume",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var stream = js.createStream(.{
        .name = "TEST_CONSUME",
        .subjects = &.{"test.consume.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_consume",
            false,
            "create stream failed",
        );
        return;
    };
    defer stream.deinit();

    var cons = js.createConsumer(
        "TEST_CONSUME",
        .{
            .name = "consume-cons",
            .durable_name = "consume-cons",
            .ack_policy = .explicit,
        },
    ) catch {
        reportResult(
            "js_consume",
            false,
            "create consumer failed",
        );
        return;
    };
    defer cons.deinit();

    // Publish 10 messages
    var i: u32 = 0;
    while (i < 10) : (i += 1) {
        var a = js.publish(
            "test.consume.data",
            "consume-test",
        ) catch {
            reportResult(
                "js_consume",
                false,
                "publish failed",
            );
            return;
        };
        a.deinit();
    }

    // Use consume() with callback handler
    const Counter = struct {
        count: u32 = 0,
        pub fn onMessage(
            self: *@This(),
            msg: *nats.jetstream.JsMsg,
        ) void {
            msg.ack() catch {};
            self.count += 1;
        }
    };

    var counter = Counter{};
    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_CONSUME",
        .consumer = "consume-cons",
    };

    var ctx = pull.consume(
        nats.jetstream.JsMsgHandler.init(
            Counter,
            &counter,
        ),
        .{
            .max_messages = 10,
            .expires_ms = 5000,
        },
    ) catch {
        reportResult(
            "js_consume",
            false,
            "consume() failed",
        );
        return;
    };

    // Wait for messages to be consumed
    var wait: u32 = 0;
    while (counter.count < 10 and wait < 50) : (wait += 1) {
        threadSleepNs(100_000_000);
    }

    ctx.stop();
    ctx.deinit();

    if (counter.count < 10) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "got {d}, expected 10",
            .{counter.count},
        ) catch "count mismatch";
        reportResult("js_consume", false, m);
        return;
    }

    var d = js.deleteStream("TEST_CONSUME") catch {
        reportResult("js_consume", true, "");
        return;
    };
    d.deinit();

    reportResult("js_consume", true, "");
}

pub fn testOrderedConsumer(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_ordered",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var stream = js.createStream(.{
        .name = "TEST_ORDERED",
        .subjects = &.{"test.ordered.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_ordered",
            false,
            "create stream failed",
        );
        return;
    };
    defer stream.deinit();

    // Publish 5 messages
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        var a = js.publish(
            "test.ordered.data",
            "ordered-msg",
        ) catch {
            reportResult(
                "js_ordered",
                false,
                "publish failed",
            );
            return;
        };
        a.deinit();
    }

    // Create ordered consumer
    var oc = nats.jetstream.OrderedConsumer.init(
        &js,
        "TEST_ORDERED",
        .{
            .filter_subject = "test.ordered.>",
            .deliver_policy = .all,
        },
    );
    defer oc.deinit();

    // Fetch all 5 messages in order
    var received: u32 = 0;
    var last_seq: u64 = 0;
    while (received < 5) {
        var msg = (oc.next(5000) catch {
            break;
        }) orelse break;

        // Verify ordering
        if (msg.metadata()) |md| {
            if (md.stream_seq <= last_seq) {
                reportResult(
                    "js_ordered",
                    false,
                    "out of order",
                );
                msg.deinit();
                return;
            }
            last_seq = md.stream_seq;
        }

        msg.deinit();
        received += 1;
    }

    if (received != 5) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "got {d}, expected 5",
            .{received},
        ) catch "count mismatch";
        reportResult("js_ordered", false, m);
        return;
    }

    // Verify stream_seq tracked correctly
    if (oc.stream_seq != 5) {
        reportResult(
            "js_ordered",
            false,
            "wrong stream_seq",
        );
        return;
    }

    var d = js.deleteStream("TEST_ORDERED") catch {
        reportResult("js_ordered", true, "");
        return;
    };
    d.deinit();

    reportResult("js_ordered", true, "");
}

// -- Ack protocol tests --

pub fn testAckPreventsRedeliver(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("js_ack", false, "connect");
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var s = js.createStream(.{
        .name = "TEST_ACK",
        .subjects = &.{"test.ack.>"},
        .storage = .memory,
    }) catch {
        reportResult("js_ack", false, "create stream");
        return;
    };
    defer s.deinit();

    var c = js.createConsumer("TEST_ACK", .{
        .name = "ack-cons",
        .durable_name = "ack-cons",
        .ack_policy = .explicit,
        .ack_wait = 1_000_000_000,
    }) catch {
        reportResult(
            "js_ack",
            false,
            "create consumer",
        );
        return;
    };
    defer c.deinit();

    // Publish 1 message
    var a = js.publish(
        "test.ack.one",
        "ack-test",
    ) catch {
        reportResult("js_ack", false, "publish");
        return;
    };
    a.deinit();

    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_ACK",
        .consumer = "ack-cons",
    };

    // Fetch and ACK
    var msg = (pull.next(5000) catch {
        reportResult("js_ack", false, "fetch 1");
        return;
    }) orelse {
        reportResult("js_ack", false, "no msg 1");
        return;
    };
    msg.ack() catch {
        reportResult("js_ack", false, "ack failed");
        msg.deinit();
        return;
    };
    msg.deinit();

    // Fetch again -> should be empty (acked)
    var result = pull.fetchNoWait(10) catch {
        reportResult("js_ack", false, "fetch 2");
        return;
    };
    defer result.deinit();

    if (result.count() != 0) {
        reportResult(
            "js_ack",
            false,
            "expected 0 after ack",
        );
        return;
    }

    var d = js.deleteStream("TEST_ACK") catch {
        reportResult("js_ack", true, "");
        return;
    };
    d.deinit();
    reportResult("js_ack", true, "");
}

pub fn testNakCausesRedeliver(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("js_nak", false, "connect");
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var s = js.createStream(.{
        .name = "TEST_NAK",
        .subjects = &.{"test.nak.>"},
        .storage = .memory,
    }) catch {
        reportResult("js_nak", false, "create stream");
        return;
    };
    defer s.deinit();

    var c = js.createConsumer("TEST_NAK", .{
        .name = "nak-cons",
        .durable_name = "nak-cons",
        .ack_policy = .explicit,
        .ack_wait = 2_000_000_000,
        .max_deliver = 3,
    }) catch {
        reportResult(
            "js_nak",
            false,
            "create consumer",
        );
        return;
    };
    defer c.deinit();

    var a = js.publish(
        "test.nak.one",
        "nak-test",
    ) catch {
        reportResult("js_nak", false, "publish");
        return;
    };
    a.deinit();

    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_NAK",
        .consumer = "nak-cons",
    };

    // Fetch and NAK
    var msg1 = (pull.next(5000) catch {
        reportResult("js_nak", false, "fetch 1");
        return;
    }) orelse {
        reportResult("js_nak", false, "no msg 1");
        return;
    };
    msg1.nak() catch {
        reportResult("js_nak", false, "nak failed");
        msg1.deinit();
        return;
    };
    msg1.deinit();

    // Fetch again -> should get redelivered message
    var msg2 = (pull.next(5000) catch {
        reportResult("js_nak", false, "fetch 2");
        return;
    }) orelse {
        reportResult(
            "js_nak",
            false,
            "no redeliver",
        );
        return;
    };

    // Verify it's the same data
    if (!std.mem.eql(u8, msg2.data(), "nak-test")) {
        reportResult(
            "js_nak",
            false,
            "wrong redeliver data",
        );
        msg2.deinit();
        return;
    }

    // Verify num_delivered > 1
    if (msg2.metadata()) |md| {
        if (md.num_delivered < 2) {
            reportResult(
                "js_nak",
                false,
                "expected redeliver count",
            );
            msg2.deinit();
            return;
        }
    }

    msg2.ack() catch {};
    msg2.deinit();

    var d = js.deleteStream("TEST_NAK") catch {
        reportResult("js_nak", true, "");
        return;
    };
    d.deinit();
    reportResult("js_nak", true, "");
}

pub fn testTermStopsRedeliver(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("js_term", false, "connect");
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var s = js.createStream(.{
        .name = "TEST_TERM",
        .subjects = &.{"test.term.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_term",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    var c = js.createConsumer("TEST_TERM", .{
        .name = "term-cons",
        .durable_name = "term-cons",
        .ack_policy = .explicit,
        .max_deliver = 5,
    }) catch {
        reportResult(
            "js_term",
            false,
            "create consumer",
        );
        return;
    };
    defer c.deinit();

    var a = js.publish(
        "test.term.one",
        "term-test",
    ) catch {
        reportResult("js_term", false, "publish");
        return;
    };
    a.deinit();

    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_TERM",
        .consumer = "term-cons",
    };

    // Fetch and TERM
    var msg = (pull.next(5000) catch {
        reportResult("js_term", false, "fetch");
        return;
    }) orelse {
        reportResult("js_term", false, "no msg");
        return;
    };
    msg.term() catch {
        reportResult("js_term", false, "term failed");
        msg.deinit();
        return;
    };
    msg.deinit();

    // Fetch again -> should be empty (terminated)
    var result = pull.fetchNoWait(10) catch {
        reportResult("js_term", false, "fetch 2");
        return;
    };
    defer result.deinit();

    if (result.count() != 0) {
        reportResult(
            "js_term",
            false,
            "expected 0 after term",
        );
        return;
    }

    var d = js.deleteStream("TEST_TERM") catch {
        reportResult("js_term", true, "");
        return;
    };
    d.deinit();
    reportResult("js_term", true, "");
}

// -- Batch fetch tests --

pub fn testBatchFetch(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("js_batch", false, "connect");
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var s = js.createStream(.{
        .name = "TEST_BATCH",
        .subjects = &.{"test.batch.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_batch",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    var c = js.createConsumer("TEST_BATCH", .{
        .name = "batch-cons",
        .durable_name = "batch-cons",
        .ack_policy = .explicit,
    }) catch {
        reportResult(
            "js_batch",
            false,
            "create consumer",
        );
        return;
    };
    defer c.deinit();

    // Publish 10 messages
    var i: u32 = 0;
    while (i < 10) : (i += 1) {
        var a = js.publish(
            "test.batch.data",
            "batch-msg",
        ) catch {
            reportResult(
                "js_batch",
                false,
                "publish",
            );
            return;
        };
        a.deinit();
    }

    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_BATCH",
        .consumer = "batch-cons",
    };

    // Fetch batch of 5
    var r1 = pull.fetch(.{
        .max_messages = 5,
        .timeout_ms = 5000,
    }) catch {
        reportResult("js_batch", false, "fetch 1");
        return;
    };

    if (r1.count() != 5) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "batch1: got {d}",
            .{r1.count()},
        ) catch "wrong";
        reportResult("js_batch", false, m);
        r1.deinit();
        return;
    }

    // Ack all in first batch
    for (r1.messages) |*msg| {
        msg.ack() catch {};
    }
    r1.deinit();

    // Fetch remaining 5
    var r2 = pull.fetch(.{
        .max_messages = 5,
        .timeout_ms = 5000,
    }) catch {
        reportResult("js_batch", false, "fetch 2");
        return;
    };

    if (r2.count() != 5) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "batch2: got {d}",
            .{r2.count()},
        ) catch "wrong";
        reportResult("js_batch", false, m);
        r2.deinit();
        return;
    }

    for (r2.messages) |*msg| {
        msg.ack() catch {};
    }
    r2.deinit();

    // Fetch again -> should be empty
    var r3 = pull.fetchNoWait(10) catch {
        reportResult("js_batch", false, "fetch 3");
        return;
    };
    defer r3.deinit();

    if (r3.count() != 0) {
        reportResult(
            "js_batch",
            false,
            "expected 0 after all acked",
        );
        return;
    }

    var d = js.deleteStream("TEST_BATCH") catch {
        reportResult("js_batch", true, "");
        return;
    };
    d.deinit();
    reportResult("js_batch", true, "");
}

// -- Publish options tests --

pub fn testPublishDedup(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("js_dedup", false, "connect");
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var s = js.createStream(.{
        .name = "TEST_DEDUP",
        .subjects = &.{"test.dedup.>"},
        .storage = .memory,
        .duplicate_window = 60_000_000_000,
    }) catch {
        reportResult(
            "js_dedup",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    // Publish with same msg-id twice
    var a1 = js.publishWithOpts(
        "test.dedup.data",
        "first",
        .{ .msg_id = "unique-1" },
    ) catch {
        reportResult("js_dedup", false, "pub 1");
        return;
    };
    const seq1 = a1.value.seq;
    a1.deinit();

    var a2 = js.publishWithOpts(
        "test.dedup.data",
        "duplicate",
        .{ .msg_id = "unique-1" },
    ) catch {
        reportResult("js_dedup", false, "pub 2");
        return;
    };

    // Should get same seq (deduplicated)
    if (a2.value.seq != seq1) {
        reportResult(
            "js_dedup",
            false,
            "not deduped",
        );
        a2.deinit();
        return;
    }

    // Should be marked as duplicate
    if (a2.value.duplicate == null or
        !a2.value.duplicate.?)
    {
        reportResult(
            "js_dedup",
            false,
            "no dup flag",
        );
        a2.deinit();
        return;
    }
    a2.deinit();

    // Different msg-id -> new message
    var a3 = js.publishWithOpts(
        "test.dedup.data",
        "second",
        .{ .msg_id = "unique-2" },
    ) catch {
        reportResult("js_dedup", false, "pub 3");
        return;
    };

    if (a3.value.seq != seq1 + 1) {
        reportResult(
            "js_dedup",
            false,
            "wrong seq for new msg",
        );
        a3.deinit();
        return;
    }
    a3.deinit();

    var d = js.deleteStream("TEST_DEDUP") catch {
        reportResult("js_dedup", true, "");
        return;
    };
    d.deinit();
    reportResult("js_dedup", true, "");
}

pub fn testPublishExpectedSeq(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("js_exp_seq", false, "connect");
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var s = js.createStream(.{
        .name = "TEST_EXPSEQ",
        .subjects = &.{"test.expseq.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_exp_seq",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    // Publish first message
    var a1 = js.publish(
        "test.expseq.data",
        "first",
    ) catch {
        reportResult("js_exp_seq", false, "pub 1");
        return;
    };
    a1.deinit();

    // Publish with correct expected_last_seq=1
    var a2 = js.publishWithOpts(
        "test.expseq.data",
        "second",
        .{ .expected_last_seq = 1 },
    ) catch {
        reportResult("js_exp_seq", false, "pub 2");
        return;
    };
    a2.deinit();

    // Publish with WRONG expected_last_seq=0
    // -> should fail
    var a3 = js.publishWithOpts(
        "test.expseq.data",
        "should-fail",
        .{ .expected_last_seq = 0 },
    );
    if (a3) |*r| {
        r.deinit();
        reportResult(
            "js_exp_seq",
            false,
            "should have failed",
        );
        return;
    } else |err| {
        if (err != error.ApiError) {
            reportResult(
                "js_exp_seq",
                false,
                "wrong error",
            );
            return;
        }
        // Verify the error code
        if (js.lastApiError()) |api_err| {
            if (api_err.err_code !=
                nats.jetstream.errors
                    .ErrCode.stream_wrong_last_seq)
            {
                reportResult(
                    "js_exp_seq",
                    false,
                    "wrong err_code",
                );
                return;
            }
        }
    }

    var d = js.deleteStream("TEST_EXPSEQ") catch {
        reportResult("js_exp_seq", true, "");
        return;
    };
    d.deinit();
    reportResult("js_exp_seq", true, "");
}

// -- Stream operations tests --

pub fn testPurgeStream(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("js_purge", false, "connect");
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var s = js.createStream(.{
        .name = "TEST_PURGE",
        .subjects = &.{"test.purge.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_purge",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    // Publish 5 messages
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        var a = js.publish(
            "test.purge.data",
            "purge-msg",
        ) catch {
            reportResult(
                "js_purge",
                false,
                "publish",
            );
            return;
        };
        a.deinit();
    }

    // Verify 5 messages exist
    var info1 = js.streamInfo("TEST_PURGE") catch {
        reportResult("js_purge", false, "info 1");
        return;
    };
    if (info1.value.state) |st| {
        if (st.messages != 5) {
            reportResult(
                "js_purge",
                false,
                "expected 5 msgs",
            );
            info1.deinit();
            return;
        }
    }
    info1.deinit();

    // Purge
    var p = js.purgeStream("TEST_PURGE") catch {
        reportResult("js_purge", false, "purge");
        return;
    };
    if (!p.value.success) {
        reportResult(
            "js_purge",
            false,
            "purge not success",
        );
        p.deinit();
        return;
    }
    if (p.value.purged != 5) {
        reportResult(
            "js_purge",
            false,
            "wrong purge count",
        );
        p.deinit();
        return;
    }
    p.deinit();

    // Verify 0 messages
    var info2 = js.streamInfo("TEST_PURGE") catch {
        reportResult("js_purge", false, "info 2");
        return;
    };
    defer info2.deinit();
    if (info2.value.state) |st| {
        if (st.messages != 0) {
            reportResult(
                "js_purge",
                false,
                "expected 0 after purge",
            );
            return;
        }
    }

    var d = js.deleteStream("TEST_PURGE") catch {
        reportResult("js_purge", true, "");
        return;
    };
    d.deinit();
    reportResult("js_purge", true, "");
}

// -- Stream update test --

pub fn testStreamUpdate(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("js_update", false, "connect");
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var s = js.createStream(.{
        .name = "TEST_UPDATE",
        .subjects = &.{"test.update.>"},
        .storage = .memory,
        .max_msgs = 100,
    }) catch {
        reportResult(
            "js_update",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    // Update max_msgs
    var u = js.updateStream(.{
        .name = "TEST_UPDATE",
        .subjects = &.{"test.update.>"},
        .storage = .memory,
        .max_msgs = 200,
    }) catch {
        reportResult("js_update", false, "update");
        return;
    };
    defer u.deinit();

    if (u.value.config) |cfg| {
        if (cfg.max_msgs != 200) {
            reportResult(
                "js_update",
                false,
                "max_msgs not updated",
            );
            return;
        }
    }

    var d = js.deleteStream("TEST_UPDATE") catch {
        reportResult("js_update", true, "");
        return;
    };
    d.deinit();
    reportResult("js_update", true, "");
}

// -- InProgress (WPI) test --

pub fn testInProgress(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("js_wpi", false, "connect");
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var s = js.createStream(.{
        .name = "TEST_WPI",
        .subjects = &.{"test.wpi.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_wpi",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    var c = js.createConsumer("TEST_WPI", .{
        .name = "wpi-cons",
        .durable_name = "wpi-cons",
        .ack_policy = .explicit,
        .ack_wait = 2_000_000_000,
    }) catch {
        reportResult(
            "js_wpi",
            false,
            "create consumer",
        );
        return;
    };
    defer c.deinit();

    var a = js.publish(
        "test.wpi.one",
        "wpi-test",
    ) catch {
        reportResult("js_wpi", false, "publish");
        return;
    };
    a.deinit();

    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_WPI",
        .consumer = "wpi-cons",
    };

    var msg = (pull.next(5000) catch {
        reportResult("js_wpi", false, "fetch");
        return;
    }) orelse {
        reportResult("js_wpi", false, "no msg");
        return;
    };

    // Send inProgress to extend deadline
    msg.inProgress() catch {
        reportResult("js_wpi", false, "wpi failed");
        msg.deinit();
        return;
    };

    // Can call inProgress multiple times
    msg.inProgress() catch {
        reportResult("js_wpi", false, "wpi 2");
        msg.deinit();
        return;
    };

    // Now ack
    msg.ack() catch {
        reportResult("js_wpi", false, "ack");
        msg.deinit();
        return;
    };
    msg.deinit();

    var d = js.deleteStream("TEST_WPI") catch {
        reportResult("js_wpi", true, "");
        return;
    };
    d.deinit();
    reportResult("js_wpi", true, "");
}

// -- Consumer not found test --

pub fn testConsumerNotFound(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_cons_not_found",
            false,
            "connect",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var s = js.createStream(.{
        .name = "TEST_CNF",
        .subjects = &.{"test.cnf.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_cons_not_found",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    var info = js.consumerInfo(
        "TEST_CNF",
        "nonexistent",
    );
    if (info) |*r| {
        r.deinit();
        reportResult(
            "js_cons_not_found",
            false,
            "should fail",
        );
        return;
    } else |err| {
        if (err != error.ApiError) {
            reportResult(
                "js_cons_not_found",
                false,
                "wrong error",
            );
            return;
        }
        if (js.lastApiError()) |api_err| {
            if (api_err.err_code !=
                nats.jetstream.errors
                    .ErrCode.consumer_not_found)
            {
                reportResult(
                    "js_cons_not_found",
                    false,
                    "wrong err_code",
                );
                return;
            }
        }
    }

    var d = js.deleteStream("TEST_CNF") catch {
        reportResult(
            "js_cons_not_found",
            true,
            "",
        );
        return;
    };
    d.deinit();
    reportResult("js_cons_not_found", true, "");
}

// -- Stream by subject test --

pub fn testStreamBySubject(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_by_subject",
            false,
            "connect",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var s = js.createStream(.{
        .name = "TEST_BYSUB",
        .subjects = &.{"bysub.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_by_subject",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    var resp = js.streamNameBySubject(
        "bysub.test",
    ) catch {
        reportResult(
            "js_by_subject",
            false,
            "lookup failed",
        );
        return;
    };
    defer resp.deinit();

    const names = resp.value.streams orelse {
        reportResult(
            "js_by_subject",
            false,
            "no result",
        );
        return;
    };

    if (names.len != 1) {
        reportResult(
            "js_by_subject",
            false,
            "expected 1 match",
        );
        return;
    }

    if (!std.mem.eql(u8, names[0], "TEST_BYSUB")) {
        reportResult(
            "js_by_subject",
            false,
            "wrong stream",
        );
        return;
    }

    var d = js.deleteStream("TEST_BYSUB") catch {
        reportResult("js_by_subject", true, "");
        return;
    };
    d.deinit();
    reportResult("js_by_subject", true, "");
}

// -- Key-Value Store tests --

pub fn testKvPutGet(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("kv_put_get", false, "connect");
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var kv = js.createKeyValue(.{
        .bucket = "TEST_KV",
        .storage = .memory,
        .history = 5,
    }) catch {
        reportResult(
            "kv_put_get",
            false,
            "create bucket",
        );
        return;
    };

    // Put
    const rev1 = kv.put("mykey", "hello") catch {
        reportResult("kv_put_get", false, "put");
        return;
    };
    if (rev1 == 0) {
        reportResult(
            "kv_put_get",
            false,
            "rev should be > 0",
        );
        return;
    }

    // Get
    const entry = (kv.get("mykey") catch {
        reportResult("kv_put_get", false, "get");
        return;
    }) orelse {
        reportResult(
            "kv_put_get",
            false,
            "key not found",
        );
        return;
    };

    if (entry.revision != rev1) {
        reportResult(
            "kv_put_get",
            false,
            "wrong revision",
        );
        return;
    }
    if (entry.operation != .put) {
        reportResult(
            "kv_put_get",
            false,
            "wrong operation",
        );
        return;
    }

    // Get non-existent key
    const missing = kv.get("nonexistent") catch {
        reportResult(
            "kv_put_get",
            false,
            "get missing err",
        );
        return;
    };
    if (missing != null) {
        reportResult(
            "kv_put_get",
            false,
            "should be null",
        );
        return;
    }

    var d = js.deleteKeyValue("TEST_KV") catch {
        reportResult("kv_put_get", true, "");
        return;
    };
    d.deinit();
    reportResult("kv_put_get", true, "");
}

pub fn testKvCreate(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("kv_create", false, "connect");
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var kv = js.createKeyValue(.{
        .bucket = "TEST_KV_CREATE",
        .storage = .memory,
    }) catch {
        reportResult(
            "kv_create",
            false,
            "create bucket",
        );
        return;
    };

    // Create succeeds on new key
    _ = kv.create("newkey", "value1") catch {
        reportResult("kv_create", false, "create 1");
        return;
    };

    // Create fails on existing key
    _ = kv.create("newkey", "value2") catch |err| {
        if (err == error.ApiError) {
            var d = js.deleteKeyValue(
                "TEST_KV_CREATE",
            ) catch {
                reportResult("kv_create", true, "");
                return;
            };
            d.deinit();
            reportResult("kv_create", true, "");
            return;
        }
        reportResult(
            "kv_create",
            false,
            "wrong error",
        );
        return;
    };

    reportResult(
        "kv_create",
        false,
        "should have failed",
    );
}

pub fn testKvUpdate(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("kv_update", false, "connect");
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var kv = js.createKeyValue(.{
        .bucket = "TEST_KV_UPDATE",
        .storage = .memory,
    }) catch {
        reportResult(
            "kv_update",
            false,
            "create bucket",
        );
        return;
    };

    const rev1 = kv.put("key1", "v1") catch {
        reportResult("kv_update", false, "put");
        return;
    };

    // Update with correct revision
    const rev2 = kv.update(
        "key1",
        "v2",
        rev1,
    ) catch {
        reportResult(
            "kv_update",
            false,
            "update ok",
        );
        return;
    };

    if (rev2 <= rev1) {
        reportResult(
            "kv_update",
            false,
            "rev not incremented",
        );
        return;
    }

    // Update with wrong revision -> fail
    _ = kv.update("key1", "v3", rev1) catch |err| {
        if (err == error.ApiError) {
            var d = js.deleteKeyValue(
                "TEST_KV_UPDATE",
            ) catch {
                reportResult("kv_update", true, "");
                return;
            };
            d.deinit();
            reportResult("kv_update", true, "");
            return;
        }
        reportResult(
            "kv_update",
            false,
            "wrong error",
        );
        return;
    };

    reportResult(
        "kv_update",
        false,
        "should have failed",
    );
}

pub fn testKvDelete(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("kv_delete", false, "connect");
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var kv = js.createKeyValue(.{
        .bucket = "TEST_KV_DEL",
        .storage = .memory,
        .history = 5,
    }) catch {
        reportResult(
            "kv_delete",
            false,
            "create bucket",
        );
        return;
    };

    _ = kv.put("delkey", "value") catch {
        reportResult("kv_delete", false, "put");
        return;
    };

    // Delete
    kv.delete("delkey") catch {
        reportResult("kv_delete", false, "delete");
        return;
    };

    // Get should show delete marker
    const entry = (kv.get("delkey") catch {
        reportResult("kv_delete", false, "get");
        return;
    }) orelse {
        // Key gone completely (ok for history=1)
        var d = js.deleteKeyValue(
            "TEST_KV_DEL",
        ) catch {
            reportResult("kv_delete", true, "");
            return;
        };
        d.deinit();
        reportResult("kv_delete", true, "");
        return;
    };

    if (entry.operation != .delete) {
        reportResult(
            "kv_delete",
            false,
            "expected delete op",
        );
        return;
    }

    var d = js.deleteKeyValue("TEST_KV_DEL") catch {
        reportResult("kv_delete", true, "");
        return;
    };
    d.deinit();
    reportResult("kv_delete", true, "");
}

pub fn testKvKeys(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("kv_keys", false, "connect");
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var kv = js.createKeyValue(.{
        .bucket = "TEST_KV_KEYS",
        .storage = .memory,
    }) catch {
        reportResult(
            "kv_keys",
            false,
            "create bucket",
        );
        return;
    };

    // Put 3 keys
    _ = kv.put("alpha", "1") catch {
        reportResult("kv_keys", false, "put 1");
        return;
    };
    _ = kv.put("beta", "2") catch {
        reportResult("kv_keys", false, "put 2");
        return;
    };
    _ = kv.put("gamma", "3") catch {
        reportResult("kv_keys", false, "put 3");
        return;
    };

    const key_list = kv.keys(allocator) catch {
        reportResult("kv_keys", false, "keys()");
        return;
    };
    defer {
        for (key_list) |k| allocator.free(k);
        allocator.free(key_list);
    }

    if (key_list.len != 3) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "got {d} keys, expected 3",
            .{key_list.len},
        ) catch "wrong count";
        reportResult("kv_keys", false, m);
        return;
    }

    var d = js.deleteKeyValue("TEST_KV_KEYS") catch {
        reportResult("kv_keys", true, "");
        return;
    };
    d.deinit();
    reportResult("kv_keys", true, "");
}

pub fn testKvHistory(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("kv_history", false, "connect");
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var kv = js.createKeyValue(.{
        .bucket = "TEST_KV_HIST",
        .storage = .memory,
        .history = 10,
    }) catch {
        reportResult(
            "kv_history",
            false,
            "create bucket",
        );
        return;
    };

    // Put same key 3 times
    _ = kv.put("hkey", "v1") catch {
        reportResult("kv_history", false, "put 1");
        return;
    };
    _ = kv.put("hkey", "v2") catch {
        reportResult("kv_history", false, "put 2");
        return;
    };
    _ = kv.put("hkey", "v3") catch {
        reportResult("kv_history", false, "put 3");
        return;
    };

    const hist = kv.history(
        allocator,
        "hkey",
    ) catch {
        reportResult(
            "kv_history",
            false,
            "history()",
        );
        return;
    };
    defer allocator.free(hist);

    if (hist.len != 3) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "got {d}, expected 3",
            .{hist.len},
        ) catch "wrong";
        reportResult("kv_history", false, m);
        return;
    }

    // Verify revisions are increasing
    if (hist.len >= 2) {
        if (hist[1].revision <= hist[0].revision) {
            reportResult(
                "kv_history",
                false,
                "revs not increasing",
            );
            return;
        }
    }

    var d = js.deleteKeyValue(
        "TEST_KV_HIST",
    ) catch {
        reportResult("kv_history", true, "");
        return;
    };
    d.deinit();
    reportResult("kv_history", true, "");
}

pub fn testKvWatch(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("kv_watch", false, "connect");
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var kv = js.createKeyValue(.{
        .bucket = "TEST_KV_WATCH",
        .storage = .memory,
    }) catch {
        reportResult(
            "kv_watch",
            false,
            "create bucket",
        );
        return;
    };

    // Put a key before watching
    _ = kv.put("pre-watch", "initial") catch {
        reportResult("kv_watch", false, "put");
        return;
    };

    // Start watching
    var watcher = kv.watchAll() catch {
        reportResult(
            "kv_watch",
            false,
            "watchAll()",
        );
        return;
    };
    defer watcher.deinit();

    // Should get the initial key
    const entry = (watcher.next(5000) catch |err| {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "watch next: {}",
            .{err},
        ) catch "watch err";
        reportResult("kv_watch", false, m);
        return;
    }) orelse {
        reportResult(
            "kv_watch",
            false,
            "no initial entry",
        );
        return;
    };

    if (!std.mem.eql(u8, entry.key, "pre-watch")) {
        reportResult(
            "kv_watch",
            false,
            "wrong key",
        );
        return;
    }

    var d = js.deleteKeyValue(
        "TEST_KV_WATCH",
    ) catch {
        reportResult("kv_watch", true, "");
        return;
    };
    d.deinit();
    reportResult("kv_watch", true, "");
}

pub fn testKvBucketLifecycle(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "kv_lifecycle",
            false,
            "connect",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    // Create
    var kv = js.createKeyValue(.{
        .bucket = "TEST_KV_LIFE",
        .storage = .memory,
    }) catch {
        reportResult(
            "kv_lifecycle",
            false,
            "create",
        );
        return;
    };

    // Status
    var st = kv.status() catch {
        reportResult(
            "kv_lifecycle",
            false,
            "status",
        );
        return;
    };
    defer st.deinit();

    if (st.value.config) |cfg| {
        if (!std.mem.eql(
            u8,
            cfg.name,
            "KV_TEST_KV_LIFE",
        )) {
            reportResult(
                "kv_lifecycle",
                false,
                "wrong stream name",
            );
            return;
        }
    }

    // Bind
    const kv2 = js.keyValue("TEST_KV_LIFE") catch {
        reportResult(
            "kv_lifecycle",
            false,
            "bind",
        );
        return;
    };
    _ = kv2;

    // Delete
    var del = js.deleteKeyValue(
        "TEST_KV_LIFE",
    ) catch {
        reportResult(
            "kv_lifecycle",
            false,
            "delete",
        );
        return;
    };
    defer del.deinit();

    if (!del.value.success) {
        reportResult(
            "kv_lifecycle",
            false,
            "delete failed",
        );
        return;
    }

    // Bind to deleted bucket -> should fail
    _ = js.keyValue("TEST_KV_LIFE") catch {
        reportResult("kv_lifecycle", true, "");
        return;
    };

    reportResult(
        "kv_lifecycle",
        false,
        "bind should fail after delete",
    );
}

// -- Behavioral correctness tests --

pub fn testFilteredConsumer(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_filtered_cons",
            false,
            "connect",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var s = js.createStream(.{
        .name = "TEST_FILTER",
        .subjects = &.{"test.filter.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_filtered_cons",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    // Publish to different subjects
    var a1 = js.publish(
        "test.filter.a",
        "msg-a",
    ) catch {
        reportResult(
            "js_filtered_cons",
            false,
            "pub a",
        );
        return;
    };
    a1.deinit();

    var a2 = js.publish(
        "test.filter.b",
        "msg-b",
    ) catch {
        reportResult(
            "js_filtered_cons",
            false,
            "pub b",
        );
        return;
    };
    a2.deinit();

    // Create consumer filtered on "test.filter.a"
    var c = js.createConsumer("TEST_FILTER", .{
        .name = "filter-cons",
        .durable_name = "filter-cons",
        .ack_policy = .explicit,
        .filter_subject = "test.filter.a",
    }) catch |err| {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "create cons: {}",
            .{err},
        ) catch "err";
        reportResult("js_filtered_cons", false, m);
        return;
    };
    defer c.deinit();

    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_FILTER",
        .consumer = "filter-cons",
    };

    // Should only get "msg-a" (filtered)
    var msg = (pull.next(5000) catch {
        reportResult(
            "js_filtered_cons",
            false,
            "fetch",
        );
        return;
    }) orelse {
        reportResult(
            "js_filtered_cons",
            false,
            "no msg",
        );
        return;
    };

    if (!std.mem.eql(u8, msg.data(), "msg-a")) {
        reportResult(
            "js_filtered_cons",
            false,
            "wrong data",
        );
        msg.deinit();
        return;
    }
    msg.ack() catch {};
    msg.deinit();

    // No more messages (msg-b filtered out)
    var r = pull.fetchNoWait(10) catch {
        reportResult(
            "js_filtered_cons",
            false,
            "fetch 2",
        );
        return;
    };
    defer r.deinit();

    if (r.count() != 0) {
        reportResult(
            "js_filtered_cons",
            false,
            "expected 0 after filter",
        );
        return;
    }

    var d = js.deleteStream("TEST_FILTER") catch {
        reportResult("js_filtered_cons", true, "");
        return;
    };
    d.deinit();
    reportResult("js_filtered_cons", true, "");
}

pub fn testPurgeSubject(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_purge_subj",
            false,
            "connect",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var s = js.createStream(.{
        .name = "TEST_PURGE_S",
        .subjects = &.{"test.purge.s.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_purge_subj",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    // Publish to 2 subjects
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        var a = js.publish(
            "test.purge.s.keep",
            "keep",
        ) catch {
            reportResult(
                "js_purge_subj",
                false,
                "pub keep",
            );
            return;
        };
        a.deinit();
    }
    i = 0;
    while (i < 2) : (i += 1) {
        var a = js.publish(
            "test.purge.s.remove",
            "remove",
        ) catch {
            reportResult(
                "js_purge_subj",
                false,
                "pub remove",
            );
            return;
        };
        a.deinit();
    }

    // Purge only "remove" subject
    var p = js.purgeStreamSubject(
        "TEST_PURGE_S",
        "test.purge.s.remove",
    ) catch {
        reportResult(
            "js_purge_subj",
            false,
            "purge",
        );
        return;
    };
    defer p.deinit();

    if (p.value.purged != 2) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "purged {d}, expected 2",
            .{p.value.purged},
        ) catch "wrong count";
        reportResult("js_purge_subj", false, m);
        return;
    }

    // Verify "keep" messages still exist
    var info = js.streamInfo("TEST_PURGE_S") catch {
        reportResult(
            "js_purge_subj",
            false,
            "info",
        );
        return;
    };
    defer info.deinit();

    if (info.value.state) |st| {
        if (st.messages != 3) {
            var buf: [64]u8 = undefined;
            const m = std.fmt.bufPrint(
                &buf,
                "{d} msgs, expected 3",
                .{st.messages},
            ) catch "wrong";
            reportResult(
                "js_purge_subj",
                false,
                m,
            );
            return;
        }
    }

    var d = js.deleteStream("TEST_PURGE_S") catch {
        reportResult("js_purge_subj", true, "");
        return;
    };
    d.deinit();
    reportResult("js_purge_subj", true, "");
}

pub fn testPaginatedStreamNames(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_paginated",
            false,
            "connect",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    // Create 3 streams
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        var name_buf: [32]u8 = undefined;
        const sname = std.fmt.bufPrint(
            &name_buf,
            "PAG_{d}",
            .{i},
        ) catch unreachable;
        var subj_b: [32]u8 = undefined;
        const ssubj = std.fmt.bufPrint(
            &subj_b,
            "pag.{d}.>",
            .{i},
        ) catch unreachable;
        const subjects: [1][]const u8 = .{ssubj};
        var r = js.createStream(.{
            .name = sname,
            .subjects = &subjects,
            .storage = .memory,
        }) catch {
            reportResult(
                "js_paginated",
                false,
                "create",
            );
            return;
        };
        r.deinit();
    }

    // Use allStreamNames (pagination)
    const all = js.allStreamNames(allocator) catch {
        reportResult(
            "js_paginated",
            false,
            "allStreamNames",
        );
        return;
    };
    defer {
        for (all) |n| allocator.free(n);
        allocator.free(all);
    }

    if (all.len < 3) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "got {d}, expected >= 3",
            .{all.len},
        ) catch "wrong";
        reportResult("js_paginated", false, m);
        return;
    }

    // Cleanup
    i = 0;
    while (i < 3) : (i += 1) {
        var name_buf: [32]u8 = undefined;
        const sname = std.fmt.bufPrint(
            &name_buf,
            "PAG_{d}",
            .{i},
        ) catch unreachable;
        var r = js.deleteStream(sname) catch continue;
        r.deinit();
    }

    reportResult("js_paginated", true, "");
}

pub fn runAll(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    std.debug.print(
        "\n--- JetStream Tests ---\n",
        .{},
    );

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    _ = manager.startServer(
        allocator,
        io.io(),
        .{ .port = js_port, .jetstream = true },
    ) catch |err| {
        std.debug.print(
            "Failed to start JS server: {}\n",
            .{err},
        );
        return;
    };

    testStreamCreateAndInfo(allocator);
    testPublishAndAck(allocator);
    testConsumerCRUD(allocator);
    testApiError(allocator);
    testStreamNames(allocator);
    testStreamList(allocator);
    testConsumerNames(allocator);
    testConsumerList(allocator);
    testAccountInfo(allocator);
    testMetadata(allocator);
    testFetchNoWait(allocator);
    testMessages(allocator);
    testConsume(allocator);
    testOrderedConsumer(allocator);
    // Ack protocol
    testAckPreventsRedeliver(allocator);
    testNakCausesRedeliver(allocator);
    testTermStopsRedeliver(allocator);
    testInProgress(allocator);
    // Batch + publish opts
    testBatchFetch(allocator);
    testPublishDedup(allocator);
    testPublishExpectedSeq(allocator);
    // Stream ops
    testPurgeStream(allocator);
    testStreamUpdate(allocator);
    // Error paths
    testConsumerNotFound(allocator);
    testStreamBySubject(allocator);
    // Key-Value Store
    testKvPutGet(allocator);
    testKvCreate(allocator);
    testKvUpdate(allocator);
    testKvDelete(allocator);
    testKvKeys(allocator);
    testKvHistory(allocator);
    testKvWatch(allocator);
    testKvBucketLifecycle(allocator);
    // Behavioral correctness
    testFilteredConsumer(allocator);
    testPurgeSubject(allocator);
    testPaginatedStreamNames(allocator);
    // Cross-verification with nats CLI
    testCrossVerifyKvPut(allocator);
    testCrossVerifyKvGet(allocator);
}

// -- Cross-verification with nats CLI --

const nats_cli = "/home/m64/go/bin/nats";

/// Run nats CLI command, return stdout.
fn runNatsCli(
    allocator: std.mem.Allocator,
    io: std.Io,
    args: []const []const u8,
) ?[]const u8 {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var full_args: [16][]const u8 = undefined;
    full_args[0] = nats_cli;
    full_args[1] = "--server";
    full_args[2] = url;
    const n = @min(args.len, 12);
    for (args[0..n], 0..) |a, i| {
        full_args[3 + i] = a;
    }

    var child = std.process.spawn(io, .{
        .argv = full_args[0 .. 3 + n],
        .stdout = .pipe,
        .stderr = .ignore,
    }) catch return null;

    var buf: [4096]u8 = undefined;
    var total: usize = 0;
    if (child.stdout) |*file| {
        while (total < buf.len) {
            var slice = [_][]u8{buf[total..]};
            const rd = file.readStreaming(
                io,
                &slice,
            ) catch break;
            if (rd == 0) break;
            total += rd;
        }
    }

    const term = child.wait(io) catch return null;
    if (term.exited != 0) return null;

    if (total == 0) return null;
    return allocator.dupe(u8, buf[0..total]) catch
        null;
}

/// Zig writes KV, nats CLI reads and verifies.
pub fn testCrossVerifyKvPut(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("cross_kv_put", false, "connect");
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var kv = js.createKeyValue(.{
        .bucket = "CROSS_PUT",
        .storage = .memory,
    }) catch {
        reportResult(
            "cross_kv_put",
            false,
            "create bucket",
        );
        return;
    };

    // Zig puts a value
    _ = kv.put("hello", "from-zig") catch {
        reportResult("cross_kv_put", false, "put");
        return;
    };

    // nats CLI reads it
    const output = runNatsCli(
        allocator,
        io.io(),
        &.{ "kv", "get", "CROSS_PUT", "hello", "--raw" },
    ) orelse {
        reportResult(
            "cross_kv_put",
            false,
            "nats cli get failed",
        );
        return;
    };
    defer allocator.free(output);

    if (std.mem.indexOf(u8, output, "from-zig") ==
        null)
    {
        reportResult(
            "cross_kv_put",
            false,
            "cli got wrong value",
        );
        return;
    }

    var d = js.deleteKeyValue("CROSS_PUT") catch {
        reportResult("cross_kv_put", true, "");
        return;
    };
    d.deinit();
    reportResult("cross_kv_put", true, "");
}

/// nats CLI writes KV, Zig reads and verifies.
pub fn testCrossVerifyKvGet(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    // CLI creates bucket and puts value
    _ = runNatsCli(
        allocator,
        io.io(),
        &.{ "kv", "add", "CROSS_GET", "--storage", "memory" },
    ) orelse {
        reportResult(
            "cross_kv_get",
            false,
            "cli add bucket",
        );
        return;
    };

    _ = runNatsCli(
        allocator,
        io.io(),
        &.{ "kv", "put", "CROSS_GET", "greeting", "hello-from-cli" },
    ) orelse {
        reportResult(
            "cross_kv_get",
            false,
            "cli put",
        );
        return;
    };

    // Zig reads it
    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "cross_kv_get",
            false,
            "connect",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

    var kv = js.keyValue("CROSS_GET") catch {
        reportResult(
            "cross_kv_get",
            false,
            "bind bucket",
        );
        return;
    };

    const entry = (kv.get("greeting") catch {
        reportResult("cross_kv_get", false, "get");
        return;
    }) orelse {
        reportResult(
            "cross_kv_get",
            false,
            "key not found",
        );
        return;
    };

    if (entry.revision == 0) {
        reportResult(
            "cross_kv_get",
            false,
            "no revision",
        );
        return;
    }

    var d = js.deleteKeyValue("CROSS_GET") catch {
        reportResult("cross_kv_get", true, "");
        return;
    };
    d.deinit();
    reportResult("cross_kv_get", true, "");
}
