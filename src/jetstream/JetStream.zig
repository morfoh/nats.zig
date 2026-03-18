//! JetStream context providing stream/consumer CRUD, publish,
//! and pull subscription operations over core NATS request/reply.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("types.zig");
const errors = @import("errors.zig");

const nats = @import("../nats.zig");
const Client = nats.Client;
const headers = nats.protocol.headers;

pub const Response = types.Response;
pub const StreamConfig = types.StreamConfig;
pub const StreamInfo = types.StreamInfo;
pub const ConsumerConfig = types.ConsumerConfig;
pub const ConsumerInfo = types.ConsumerInfo;
pub const CreateConsumerRequest = types.CreateConsumerRequest;
pub const DeleteResponse = types.DeleteResponse;
pub const PurgeResponse = types.PurgeResponse;
pub const PubAck = types.PubAck;
pub const PublishOpts = types.PublishOpts;
pub const StreamNamesResponse = types.StreamNamesResponse;
pub const StreamListResponse = types.StreamListResponse;
pub const ConsumerNamesResponse = types.ConsumerNamesResponse;
pub const ConsumerListResponse = types.ConsumerListResponse;
pub const ListRequest = types.ListRequest;
pub const AccountInfo = types.AccountInfo;
pub const ApiError = errors.ApiError;
pub const ApiErrorJson = errors.ApiErrorJson;

const JetStream = @This();

client: *Client,
allocator: Allocator,
api_prefix_buf: [128]u8 = undefined,
api_prefix_len: u8 = 0,
timeout_ms: u32 = 5000,
last_api_err: ?ApiError = null,
_reserved_async_ctx: ?*anyopaque = null,

/// JetStream context options for API prefix, timeout, and
/// multi-tenant domain configuration.
pub const Options = struct {
    api_prefix: []const u8 = "$JS.API.",
    timeout_ms: u32 = 5000,
    domain: ?[]const u8 = null,
};

/// Initializes a JetStream context bound to the given client.
pub fn init(client: *Client, opts: Options) JetStream {
    std.debug.assert(client.isConnected());
    var js = JetStream{
        .client = client,
        .allocator = client.allocator,
        .timeout_ms = opts.timeout_ms,
    };
    if (opts.domain) |d| {
        std.debug.assert(d.len > 0);
        // "$JS." + domain + ".API." = 9 overhead
        std.debug.assert(d.len <= 119);
        var buf: [128]u8 = undefined;
        const p = std.fmt.bufPrint(
            &buf,
            "$JS.{s}.API.",
            .{d},
        ) catch unreachable;
        @memcpy(
            js.api_prefix_buf[0..p.len],
            p,
        );
        js.api_prefix_len = @intCast(p.len);
    } else {
        const p = opts.api_prefix;
        std.debug.assert(p.len > 0);
        std.debug.assert(p.len <= js.api_prefix_buf.len);
        @memcpy(js.api_prefix_buf[0..p.len], p);
        js.api_prefix_len = @intCast(p.len);
    }
    return js;
}

/// Returns the last API error from the server, if any.
pub fn lastApiError(self: *const JetStream) ?ApiError {
    return self.last_api_err;
}

// -- Stream CRUD --

/// Creates a stream with the given configuration.
pub fn createStream(
    self: *JetStream,
    config: StreamConfig,
) !Response(StreamInfo) {
    std.debug.assert(config.name.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [256]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "STREAM.CREATE.{s}",
        .{config.name},
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequest(StreamInfo, subj, config);
}

/// Updates a stream with the given configuration.
pub fn updateStream(
    self: *JetStream,
    config: StreamConfig,
) !Response(StreamInfo) {
    std.debug.assert(config.name.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [256]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "STREAM.UPDATE.{s}",
        .{config.name},
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequest(StreamInfo, subj, config);
}

/// Deletes a stream by name.
pub fn deleteStream(
    self: *JetStream,
    name: []const u8,
) !Response(DeleteResponse) {
    std.debug.assert(name.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [256]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "STREAM.DELETE.{s}",
        .{name},
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequestNoPayload(
        DeleteResponse,
        subj,
    );
}

/// Gets stream info by name.
pub fn streamInfo(
    self: *JetStream,
    name: []const u8,
) !Response(StreamInfo) {
    std.debug.assert(name.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [256]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "STREAM.INFO.{s}",
        .{name},
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequestNoPayload(StreamInfo, subj);
}

/// Purges a stream by name. Optionally filter by
/// subject to only purge matching messages.
pub fn purgeStream(
    self: *JetStream,
    name: []const u8,
) !Response(PurgeResponse) {
    return self.purgeStreamFiltered(name, null);
}

/// Purges messages matching a specific subject.
pub fn purgeStreamSubject(
    self: *JetStream,
    name: []const u8,
    subject: []const u8,
) !Response(PurgeResponse) {
    std.debug.assert(subject.len > 0);
    return self.purgeStreamFiltered(name, subject);
}

fn purgeStreamFiltered(
    self: *JetStream,
    name: []const u8,
    subject: ?[]const u8,
) !Response(PurgeResponse) {
    std.debug.assert(name.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [256]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "STREAM.PURGE.{s}",
        .{name},
    ) catch return errors.Error.SubjectTooLong;
    if (subject) |s| {
        return self.apiRequest(
            PurgeResponse,
            subj,
            types.PurgeRequest{ .filter = s },
        );
    }
    return self.apiRequestNoPayload(
        PurgeResponse,
        subj,
    );
}

// -- Consumer CRUD --

/// Creates a consumer on the given stream. The
/// filter_subject (if any) is sent in the JSON body.
pub fn createConsumer(
    self: *JetStream,
    stream: []const u8,
    config: ConsumerConfig,
) !Response(ConsumerInfo) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [512]u8 = undefined;
    const name = config.name orelse
        config.durable_name orelse "";
    std.debug.assert(name.len > 0);
    const subj = std.fmt.bufPrint(
        &buf,
        "CONSUMER.CREATE.{s}.{s}",
        .{ stream, name },
    ) catch return errors.Error.SubjectTooLong;
    const req = CreateConsumerRequest{
        .stream_name = stream,
        .config = config,
    };
    return self.apiRequest(ConsumerInfo, subj, req);
}

/// Updates a consumer on the given stream.
pub fn updateConsumer(
    self: *JetStream,
    stream: []const u8,
    config: ConsumerConfig,
) !Response(ConsumerInfo) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [512]u8 = undefined;
    const name = config.name orelse
        config.durable_name orelse "";
    std.debug.assert(name.len > 0);
    const subj = std.fmt.bufPrint(
        &buf,
        "CONSUMER.CREATE.{s}.{s}",
        .{ stream, name },
    ) catch return errors.Error.SubjectTooLong;
    const req = CreateConsumerRequest{
        .stream_name = stream,
        .config = config,
        .action = "update",
    };
    return self.apiRequest(ConsumerInfo, subj, req);
}

/// Deletes a consumer from a stream.
pub fn deleteConsumer(
    self: *JetStream,
    stream: []const u8,
    consumer: []const u8,
) !Response(DeleteResponse) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(consumer.len > 0);
    var buf: [512]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "CONSUMER.DELETE.{s}.{s}",
        .{ stream, consumer },
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequestNoPayload(
        DeleteResponse,
        subj,
    );
}

/// Gets consumer info.
pub fn consumerInfo(
    self: *JetStream,
    stream: []const u8,
    consumer: []const u8,
) !Response(ConsumerInfo) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(consumer.len > 0);
    var buf: [512]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "CONSUMER.INFO.{s}.{s}",
        .{ stream, consumer },
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequestNoPayload(
        ConsumerInfo,
        subj,
    );
}

// -- Listing & Account Info --

/// Returns stream names. Pass offset=0 for first
/// page. Check response total/offset/limit for
/// pagination. Returns one page per call.
pub fn streamNames(
    self: *JetStream,
) !Response(StreamNamesResponse) {
    return self.streamNamesOffset(0);
}

/// Returns stream names starting at offset.
pub fn streamNamesOffset(
    self: *JetStream,
    offset: u64,
) !Response(StreamNamesResponse) {
    std.debug.assert(self.timeout_ms > 0);
    return self.apiRequest(
        StreamNamesResponse,
        "STREAM.NAMES",
        ListRequest{ .offset = offset },
    );
}

/// Returns all stream names across all pages.
/// Caller owns the returned slice; free each
/// string and the slice with allocator.
pub fn allStreamNames(
    self: *JetStream,
    allocator: Allocator,
) ![][]const u8 {
    std.debug.assert(self.timeout_ms > 0);
    var result: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (result.items) |n|
            allocator.free(n);
        result.deinit(allocator);
    }
    var offset: u64 = 0;
    while (true) {
        var resp = try self.streamNamesOffset(
            offset,
        );
        defer resp.deinit();
        const names = resp.value.streams orelse
            break;
        for (names) |n| {
            const owned = try allocator.dupe(u8, n);
            try result.append(allocator, owned);
        }
        offset += names.len;
        if (offset >= resp.value.total) break;
    }
    return result.toOwnedSlice(allocator);
}

/// Returns stream info list (one page).
pub fn streams(
    self: *JetStream,
) !Response(StreamListResponse) {
    std.debug.assert(self.timeout_ms > 0);
    return self.apiRequest(
        StreamListResponse,
        "STREAM.LIST",
        ListRequest{},
    );
}

/// Finds the stream name that captures a subject.
pub fn streamNameBySubject(
    self: *JetStream,
    subject: []const u8,
) !Response(StreamNamesResponse) {
    std.debug.assert(subject.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    return self.apiRequest(
        StreamNamesResponse,
        "STREAM.NAMES",
        ListRequest{ .subject = subject },
    );
}

/// Returns consumer names (one page).
pub fn consumerNames(
    self: *JetStream,
    stream: []const u8,
) !Response(ConsumerNamesResponse) {
    return self.consumerNamesOffset(stream, 0);
}

/// Returns consumer names at offset.
pub fn consumerNamesOffset(
    self: *JetStream,
    stream: []const u8,
    offset: u64,
) !Response(ConsumerNamesResponse) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [256]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "CONSUMER.NAMES.{s}",
        .{stream},
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequest(
        ConsumerNamesResponse,
        subj,
        ListRequest{ .offset = offset },
    );
}

/// Returns consumer info list (one page).
pub fn consumers(
    self: *JetStream,
    stream: []const u8,
) !Response(ConsumerListResponse) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [256]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "CONSUMER.LIST.{s}",
        .{stream},
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequest(
        ConsumerListResponse,
        subj,
        ListRequest{},
    );
}

/// Returns JetStream account information including
/// usage stats and limits.
pub fn accountInfo(
    self: *JetStream,
) !Response(AccountInfo) {
    std.debug.assert(self.timeout_ms > 0);
    return self.apiRequestNoPayload(
        AccountInfo,
        "INFO",
    );
}

// -- Key-Value Store --

const kv_mod = @import("kv.zig");
pub const KeyValue = kv_mod.KeyValue;
pub const KvWatcher = kv_mod.KvWatcher;
const KeyValueConfig = types.KeyValueConfig;

/// Creates a new key-value bucket backed by a
/// JetStream stream. Returns a KeyValue handle.
pub fn createKeyValue(
    self: *JetStream,
    cfg: KeyValueConfig,
) !KeyValue {
    std.debug.assert(cfg.bucket.len > 0);
    std.debug.assert(cfg.bucket.len <= 64);
    std.debug.assert(self.timeout_ms > 0);

    var stream_buf: [68]u8 = undefined;
    const stream_name = std.fmt.bufPrint(
        &stream_buf,
        "KV_{s}",
        .{cfg.bucket},
    ) catch return errors.Error.SubjectTooLong;

    var subj_buf: [128]u8 = undefined;
    const subj_pattern = std.fmt.bufPrint(
        &subj_buf,
        "$KV.{s}.>",
        .{cfg.bucket},
    ) catch return errors.Error.SubjectTooLong;

    const subjects: [1][]const u8 = .{subj_pattern};
    const hist: i64 = if (cfg.history) |h|
        @intCast(h)
    else
        1;

    // Duplicate window: 2min or TTL (whichever smaller)
    const two_min_ns: i64 = 120_000_000_000;
    const dup_window: ?i64 = if (cfg.ttl) |ttl|
        @min(ttl, two_min_ns)
    else
        two_min_ns;

    var resp = try self.createStream(.{
        .name = stream_name,
        .subjects = &subjects,
        .max_msgs_per_subject = hist,
        .max_bytes = cfg.max_bytes,
        .max_age = cfg.ttl,
        .max_msg_size = cfg.max_value_size,
        .storage = cfg.storage orelse .file,
        .num_replicas = cfg.replicas,
        .discard = .new,
        .duplicate_window = dup_window,
        .max_msgs = -1,
        .max_consumers = -1,
        .allow_rollup_hdrs = true,
        .deny_delete = true,
        .deny_purge = false,
        .allow_direct = true,
        .mirror_direct = false,
        .description = cfg.description,
    });
    resp.deinit();

    return self.initKeyValue(cfg.bucket);
}

/// Binds to an existing key-value bucket.
/// Returns error if the stream doesn't exist.
pub fn keyValue(
    self: *JetStream,
    bucket_name: []const u8,
) !KeyValue {
    std.debug.assert(bucket_name.len > 0);
    std.debug.assert(bucket_name.len <= 64);

    var stream_buf: [68]u8 = undefined;
    const stream_name = std.fmt.bufPrint(
        &stream_buf,
        "KV_{s}",
        .{bucket_name},
    ) catch return errors.Error.SubjectTooLong;

    // Verify stream exists
    var resp = try self.streamInfo(stream_name);
    resp.deinit();

    return self.initKeyValue(bucket_name);
}

/// Deletes a key-value bucket and its backing stream.
pub fn deleteKeyValue(
    self: *JetStream,
    bucket_name: []const u8,
) !Response(DeleteResponse) {
    std.debug.assert(bucket_name.len > 0);
    var stream_buf: [68]u8 = undefined;
    const stream_name = std.fmt.bufPrint(
        &stream_buf,
        "KV_{s}",
        .{bucket_name},
    ) catch return errors.Error.SubjectTooLong;
    return self.deleteStream(stream_name);
}

fn initKeyValue(
    self: *JetStream,
    bucket_name: []const u8,
) KeyValue {
    std.debug.assert(bucket_name.len > 0);
    var kv = KeyValue{ .js = self };

    @memcpy(
        kv.bucket_buf[0..bucket_name.len],
        bucket_name,
    );
    kv.bucket_len = @intCast(bucket_name.len);

    var stream_buf: [68]u8 = undefined;
    const sn = std.fmt.bufPrint(
        &stream_buf,
        "KV_{s}",
        .{bucket_name},
    ) catch unreachable;
    @memcpy(kv.stream_buf[0..sn.len], sn);
    kv.stream_len = @intCast(sn.len);

    return kv;
}

// -- JetStream Publish --

/// Publishes a message to a JetStream stream subject
/// and waits for a PubAck. Retries up to 2 times on
/// NoResponders (matching Go client behavior for
/// transient leadership changes).
pub fn publish(
    self: *JetStream,
    subject: []const u8,
    payload: []const u8,
) !Response(PubAck) {
    std.debug.assert(subject.len > 0);
    std.debug.assert(payload.len <= 1048576);
    return self.publishRetry(subject, payload, null);
}

fn publishRetry(
    self: *JetStream,
    subject: []const u8,
    payload: []const u8,
    hdrs: ?[]const headers.Entry,
) !Response(PubAck) {
    const max_retries: u32 = 2;
    const retry_wait_ns: u64 = 250_000_000;
    var attempt: u32 = 0;

    while (true) {
        const resp = if (hdrs) |h|
            self.client.requestWithHeaders(
                subject,
                h,
                payload,
                self.timeout_ms,
            ) catch |err| return err
        else
            self.client.request(
                subject,
                payload,
                self.timeout_ms,
            ) catch |err| return err;

        var msg = resp orelse
            return errors.Error.Timeout;

        if (msg.isNoResponders()) {
            msg.deinit();
            attempt += 1;
            if (attempt > max_retries)
                return errors.Error.NoResponders;
            sleepNs(retry_wait_ns);
            continue;
        }

        defer msg.deinit();
        return self.parsePubAckResponse(&msg);
    }
}

fn sleepNs(ns: u64) void {
    var ts: std.posix.timespec = .{
        .sec = @intCast(ns / 1_000_000_000),
        .nsec = @intCast(ns % 1_000_000_000),
    };
    _ = std.posix.system.nanosleep(&ts, &ts);
}

/// Publishes with header-based options (msg-id, expected
/// stream/seq) and waits for a PubAck.
pub fn publishWithOpts(
    self: *JetStream,
    subject: []const u8,
    payload: []const u8,
    opts: PublishOpts,
) !Response(PubAck) {
    std.debug.assert(subject.len > 0);
    std.debug.assert(payload.len <= 1048576);

    var hdr_entries: [5]headers.Entry = undefined;
    var hdr_count: usize = 0;

    if (opts.msg_id) |v| {
        hdr_entries[hdr_count] = .{
            .key = headers.HeaderName.msg_id,
            .value = v,
        };
        hdr_count += 1;
    }
    if (opts.expected_stream) |v| {
        hdr_entries[hdr_count] = .{
            .key = headers.HeaderName.expected_stream,
            .value = v,
        };
        hdr_count += 1;
    }
    if (opts.expected_last_msg_id) |v| {
        hdr_entries[hdr_count] = .{
            .key = headers.HeaderName.expected_last_msg_id,
            .value = v,
        };
        hdr_count += 1;
    }

    // Numeric headers need formatting
    var seq_buf: [20]u8 = undefined;
    if (opts.expected_last_seq) |v| {
        const s = std.fmt.bufPrint(
            &seq_buf,
            "{d}",
            .{v},
        ) catch unreachable;
        hdr_entries[hdr_count] = .{
            .key = headers.HeaderName.expected_last_seq,
            .value = s,
        };
        hdr_count += 1;
    }
    var subj_seq_buf: [20]u8 = undefined;
    if (opts.expected_last_subj_seq) |v| {
        const s = std.fmt.bufPrint(
            &subj_seq_buf,
            "{d}",
            .{v},
        ) catch unreachable;
        hdr_entries[hdr_count] = .{
            .key = headers.HeaderName.expected_last_subj_seq,
            .value = s,
        };
        hdr_count += 1;
    }

    return self.publishRetry(
        subject,
        payload,
        hdr_entries[0..hdr_count],
    );
}

// -- Internal helpers --

/// Builds the full API subject and sends a request with
/// JSON payload, parsing the response.
pub fn apiRequest(
    self: *JetStream,
    comptime T: type,
    api_subject: []const u8,
    payload: anytype,
) !Response(T) {
    std.debug.assert(api_subject.len > 0);
    const prefix = self.apiPrefix();
    std.debug.assert(prefix.len > 0);

    var full_buf: [512]u8 = undefined;
    const full_subj = std.fmt.bufPrint(
        &full_buf,
        "{s}{s}",
        .{ prefix, api_subject },
    ) catch return errors.Error.SubjectTooLong;

    const json_payload = try types.jsonStringify(
        self.allocator,
        payload,
    );
    defer self.allocator.free(json_payload);

    const resp = self.client.request(
        full_subj,
        json_payload,
        self.timeout_ms,
    ) catch |err| return err;
    var msg = resp orelse
        return errors.Error.Timeout;
    defer msg.deinit();

    if (msg.isNoResponders())
        return errors.Error.NoResponders;

    return self.parseResponse(T, msg.data);
}

/// Sends a request with no payload body.
fn apiRequestNoPayload(
    self: *JetStream,
    comptime T: type,
    api_subject: []const u8,
) !Response(T) {
    std.debug.assert(api_subject.len > 0);
    const prefix = self.apiPrefix();
    std.debug.assert(prefix.len > 0);

    var full_buf: [512]u8 = undefined;
    const full_subj = std.fmt.bufPrint(
        &full_buf,
        "{s}{s}",
        .{ prefix, api_subject },
    ) catch return errors.Error.SubjectTooLong;

    const resp = self.client.request(
        full_subj,
        "",
        self.timeout_ms,
    ) catch |err| return err;
    var msg = resp orelse
        return errors.Error.Timeout;
    defer msg.deinit();

    if (msg.isNoResponders())
        return errors.Error.NoResponders;

    return self.parseResponse(T, msg.data);
}

/// Parses JSON response, checks for API error envelope.
fn parseResponse(
    self: *JetStream,
    comptime T: type,
    data: []const u8,
) !Response(T) {
    std.debug.assert(data.len > 0);
    var parsed = types.jsonParse(
        T,
        self.allocator,
        data,
    ) catch return errors.Error.JsonParseError;

    if (checkApiError(T, &parsed.value)) |api_err| {
        self.last_api_err = ApiError.fromJson(api_err);
        parsed.deinit();
        return errors.Error.ApiError;
    }

    return Response(T){
        .value = parsed.value,
        ._parsed = parsed,
    };
}

/// Parses PubAck from a message response (publish goes
/// directly to stream subject, not through $JS.API).
fn parsePubAckResponse(
    self: *JetStream,
    msg: *Client.Message,
) !Response(PubAck) {
    if (msg.isNoResponders())
        return errors.Error.NoResponders;
    std.debug.assert(msg.data.len > 0);
    return self.parseResponse(PubAck, msg.data);
}

/// Checks if a parsed response contains an API error.
fn checkApiError(
    comptime T: type,
    value: *const T,
) ?ApiErrorJson {
    if (@hasField(T, "error")) {
        if (value.@"error") |err| {
            if (err.code > 0) return err;
        }
    }
    return null;
}

/// Returns the API prefix slice.
pub fn apiPrefix(self: *const JetStream) []const u8 {
    std.debug.assert(self.api_prefix_len > 0);
    return self.api_prefix_buf[0..self.api_prefix_len];
}

// -- Tests --

test "subject building" {
    // Test apiPrefix format for default
    var js = JetStream{
        .client = undefined,
        .allocator = std.testing.allocator,
    };
    const default_prefix = "$JS.API.";
    @memcpy(
        js.api_prefix_buf[0..default_prefix.len],
        default_prefix,
    );
    js.api_prefix_len = @intCast(default_prefix.len);
    try std.testing.expectEqualStrings(
        "$JS.API.",
        js.apiPrefix(),
    );
}
