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
    while (counter.count < 10 and wait < 50) : (
        wait += 1
    ) {
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
}
