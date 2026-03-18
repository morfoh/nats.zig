//! JetStream Key-Value Store.
//!
//! A key-value store backed by a JetStream stream. Keys are
//! NATS subjects under `$KV.{bucket}.{key}`, values are
//! message payloads. Supports history, delete markers, watch,
//! and optimistic concurrency via revision numbers.

const std = @import("std");
const Allocator = std.mem.Allocator;

const nats = @import("../nats.zig");
const Client = nats.Client;
const headers_mod = nats.protocol.headers;

const types = @import("types.zig");
const errors = @import("errors.zig");
const JetStream = @import("JetStream.zig");
const PullSubscription = @import(
    "pull.zig",
).PullSubscription;

var ephemeral_counter: std.atomic.Value(u32) =
    std.atomic.Value(u32).init(0);

/// Key-value store bound to a specific bucket.
/// Created via `JetStream.createKeyValue()` or
/// `JetStream.keyValue()`.
pub const KeyValue = struct {
    js: *JetStream,
    bucket_buf: [64]u8 = undefined,
    bucket_len: u8 = 0,
    stream_buf: [68]u8 = undefined,
    stream_len: u8 = 0,
    // Stable storage for ephemeral consumer names
    _eph_name_buf: [48]u8 = undefined,
    _eph_name_len: u8 = 0,

    /// Returns the bucket name.
    pub fn bucket(self: *const KeyValue) []const u8 {
        std.debug.assert(self.bucket_len > 0);
        return self.bucket_buf[0..self.bucket_len];
    }

    /// Returns the underlying stream name.
    fn streamName(
        self: *const KeyValue,
    ) []const u8 {
        std.debug.assert(self.stream_len > 0);
        return self.stream_buf[0..self.stream_len];
    }

    /// Builds the KV subject for a key. Validates key
    /// contains no wildcards or control characters.
    fn kvSubject(
        self: *const KeyValue,
        key: []const u8,
        buf: []u8,
    ) ![]const u8 {
        std.debug.assert(key.len > 0);
        // Reject wildcards and control chars in keys
        for (key) |c| {
            if (c == '*' or c == '>' or c < 0x20)
                return errors.Error.InvalidKey;
        }
        return std.fmt.bufPrint(
            buf,
            "$KV.{s}.{s}",
            .{ self.bucket(), key },
        ) catch return errors.Error.SubjectTooLong;
    }

    // -- Get --

    /// Gets the latest value for a key. Returns null
    /// if the key does not exist. Returns the entry
    /// even if it's a delete/purge marker (check
    /// entry.operation).
    pub fn get(
        self: *KeyValue,
        key: []const u8,
    ) !?types.KeyValueEntry {
        std.debug.assert(key.len > 0);
        std.debug.assert(self.bucket_len > 0);
        return self.getBySubject(key);
    }

    /// Gets a specific revision of a key.
    pub fn getRevision(
        self: *KeyValue,
        key: []const u8,
        revision: u64,
    ) !?types.KeyValueEntry {
        std.debug.assert(key.len > 0);
        std.debug.assert(revision > 0);
        return self.getBySeq(key, revision);
    }

    fn getBySubject(
        self: *KeyValue,
        key: []const u8,
    ) !?types.KeyValueEntry {
        var subj_buf: [256]u8 = undefined;
        const kv_subj = try self.kvSubject(
            key,
            &subj_buf,
        );

        var api_buf: [512]u8 = undefined;
        const api_subj = std.fmt.bufPrint(
            &api_buf,
            "STREAM.MSG.GET.{s}",
            .{self.streamName()},
        ) catch return errors.Error.SubjectTooLong;

        const req = types.MsgGetRequest{
            .last_by_subj = kv_subj,
        };
        return self.fetchAndParse(api_subj, req, key);
    }

    fn getBySeq(
        self: *KeyValue,
        key: []const u8,
        seq: u64,
    ) !?types.KeyValueEntry {
        var api_buf: [512]u8 = undefined;
        const api_subj = std.fmt.bufPrint(
            &api_buf,
            "STREAM.MSG.GET.{s}",
            .{self.streamName()},
        ) catch return errors.Error.SubjectTooLong;

        const req = types.MsgGetRequest{ .seq = seq };
        return self.fetchAndParse(api_subj, req, key);
    }

    fn fetchAndParse(
        self: *KeyValue,
        api_subj: []const u8,
        req: types.MsgGetRequest,
        key: []const u8,
    ) !?types.KeyValueEntry {
        var resp = self.js.apiRequest(
            types.MsgGetResponse,
            api_subj,
            req,
        ) catch |err| {
            if (err == error.ApiError) {
                if (self.js.lastApiError()) |ae| {
                    if (ae.err_code == 10037)
                        return null;
                }
            }
            return err;
        };
        defer resp.deinit();

        const msg = resp.value.message orelse
            return null;
        const seq = msg.seq;

        // Validate subject matches expected key
        if (msg.subject) |subj| {
            var exp_buf: [256]u8 = undefined;
            const expected = std.fmt.bufPrint(
                &exp_buf,
                "$KV.{s}.{s}",
                .{ self.bucket(), key },
            ) catch return errors.Error.SubjectTooLong;
            if (!std.mem.eql(u8, subj, expected))
                return null;
        }

        // Determine operation from stored headers
        var op: types.KeyValueOp = .put;
        if (msg.hdrs) |hdr_b64| {
            // hdrs is base64-encoded
            var decode_buf: [1024]u8 = undefined;
            const decoded = decodeBase64(
                hdr_b64,
                &decode_buf,
            ) orelse return types.KeyValueEntry{
                .bucket = self.bucket(),
                .key = key,
                .value = "",
                .revision = seq,
                .operation = .put,
            };
            op = parseKvOp(decoded);
        }

        // Data is base64-encoded in JSON response —
        // decode before returning to caller
        const allocator = self.js.allocator;
        var val: []const u8 = "";
        var val_alloc: ?Allocator = null;
        if (msg.data) |data_b64| {
            if (data_b64.len > 0 and op == .put) {
                // Decode base64 into allocated buffer
                const decoder = std.base64.standard
                    .Decoder;
                const dec_len = decoder
                    .calcSizeForSlice(data_b64) catch {
                    return error.InvalidData;
                };
                const decoded_val = try allocator.alloc(
                    u8,
                    dec_len,
                );
                decoder.decode(
                    decoded_val[0..dec_len],
                    data_b64,
                ) catch {
                    allocator.free(decoded_val);
                    return error.InvalidData;
                };
                val = decoded_val[0..dec_len];
                val_alloc = allocator;
            }
        }

        return types.KeyValueEntry{
            .bucket = self.bucket(),
            .key = key,
            .value = val,
            .revision = seq,
            .operation = op,
            .value_allocator = val_alloc,
        };
    }

    // -- Put --

    /// Puts a value for a key. Returns the revision
    /// (sequence number).
    pub fn put(
        self: *KeyValue,
        key: []const u8,
        value: []const u8,
    ) !u64 {
        std.debug.assert(key.len > 0);
        std.debug.assert(self.bucket_len > 0);
        var subj_buf: [256]u8 = undefined;
        const subj = try self.kvSubject(
            key,
            &subj_buf,
        );
        var resp = try self.js.publish(subj, value);
        defer resp.deinit();
        return resp.value.seq;
    }

    /// Creates a key only if it does not already exist.
    /// Returns the revision, or error.ApiError if the
    /// key already exists (check lastApiError for
    /// stream_wrong_last_seq).
    pub fn create(
        self: *KeyValue,
        key: []const u8,
        value: []const u8,
    ) !u64 {
        std.debug.assert(key.len > 0);
        var subj_buf: [256]u8 = undefined;
        const subj = try self.kvSubject(
            key,
            &subj_buf,
        );
        var resp = try self.js.publishWithOpts(
            subj,
            value,
            .{ .expected_last_subj_seq = 0 },
        );
        defer resp.deinit();
        return resp.value.seq;
    }

    /// Updates a key only if the current revision matches.
    /// Returns the new revision, or error.ApiError on
    /// revision mismatch.
    pub fn update(
        self: *KeyValue,
        key: []const u8,
        value: []const u8,
        revision: u64,
    ) !u64 {
        std.debug.assert(key.len > 0);
        std.debug.assert(revision > 0);
        var subj_buf: [256]u8 = undefined;
        const subj = try self.kvSubject(
            key,
            &subj_buf,
        );
        var resp = try self.js.publishWithOpts(
            subj,
            value,
            .{ .expected_last_subj_seq = revision },
        );
        defer resp.deinit();
        return resp.value.seq;
    }

    // -- Delete / Purge --

    /// Soft-deletes a key by publishing a delete marker.
    /// Returns the revision number. The key can still
    /// appear in history.
    pub fn delete(
        self: *KeyValue,
        key: []const u8,
    ) !u64 {
        std.debug.assert(key.len > 0);
        var subj_buf: [256]u8 = undefined;
        const subj = try self.kvSubject(
            key,
            &subj_buf,
        );
        const hdrs = [_]nats.protocol.headers.Entry{
            .{
                .key = "KV-Operation",
                .value = "DEL",
            },
        };
        var resp = try self.js.publishRetry(
            subj,
            "",
            &hdrs,
        );
        defer resp.deinit();
        return resp.value.seq;
    }

    /// Purges a key and all its history.
    /// Returns the revision number.
    pub fn purge(
        self: *KeyValue,
        key: []const u8,
    ) !u64 {
        std.debug.assert(key.len > 0);
        var subj_buf: [256]u8 = undefined;
        const subj = try self.kvSubject(
            key,
            &subj_buf,
        );
        const hdrs = [_]nats.protocol.headers.Entry{
            .{
                .key = "KV-Operation",
                .value = "PURGE",
            },
            .{
                .key = "Nats-Rollup",
                .value = "sub",
            },
        };
        var resp = try self.js.publishRetry(
            subj,
            "",
            &hdrs,
        );
        defer resp.deinit();
        return resp.value.seq;
    }

    // -- Keys --

    /// Returns all current (non-deleted) keys in the
    /// bucket. Creates an ephemeral consumer with
    /// last_per_subject deliver policy. Caller owns
    /// the slice; free each key + slice with allocator.
    pub fn keys(
        self: *KeyValue,
        allocator: Allocator,
    ) ![][]const u8 {
        std.debug.assert(self.bucket_len > 0);

        var subj_buf: [256]u8 = undefined;
        const filter = std.fmt.bufPrint(
            &subj_buf,
            "$KV.{s}.>",
            .{self.bucket()},
        ) catch return errors.Error.SubjectTooLong;

        var pull = try self.createEphemeralPull(
            filter,
            .last_per_subject,
            null,
        );
        defer self.deleteEphemeralPull(&pull);

        var result: std.ArrayList([]const u8) = .empty;
        errdefer {
            for (result.items) |k| allocator.free(k);
            result.deinit(allocator);
        }

        while (true) {
            var msg = (pull.next(3000) catch break) orelse break;
            defer msg.deinit();

            const subj = msg.subject();
            const plen: usize = 4 +
                @as(usize, self.bucket_len) + 1;
            if (subj.len > plen) {
                const k = subj[plen..];
                if (msg.headers()) |h| {
                    if (isDeleteOp(h)) continue;
                }
                const owned = try allocator.dupe(
                    u8,
                    k,
                );
                try result.append(allocator, owned);
            }
        }

        return result.toOwnedSlice(allocator);
    }

    // -- History --

    /// Returns all revisions for a key (up to max
    /// history). Caller owns the returned slice.
    pub fn history(
        self: *KeyValue,
        allocator: Allocator,
        key: []const u8,
    ) ![]types.KeyValueEntry {
        std.debug.assert(key.len > 0);

        var subj_buf: [256]u8 = undefined;
        const filter = try self.kvSubject(
            key,
            &subj_buf,
        );

        var pull = try self.createEphemeralPull(
            filter,
            .all,
            null,
        );
        defer self.deleteEphemeralPull(&pull);

        var result: std.ArrayList(
            types.KeyValueEntry,
        ) = .empty;
        errdefer result.deinit(allocator);

        while (true) {
            var msg = (pull.next(3000) catch break) orelse break;

            var op: types.KeyValueOp = .put;
            if (msg.headers()) |h| {
                op = parseKvOp(h);
            }

            const md = msg.metadata();
            const seq = if (md) |m|
                m.stream_seq
            else
                0;

            // Extract value before deinit frees msg
            const data = msg.data();
            var val: []const u8 = "";
            var val_alloc: ?Allocator = null;
            if (data.len > 0 and op == .put) {
                val = allocator.dupe(
                    u8,
                    data,
                ) catch break;
                val_alloc = allocator;
            }

            result.append(allocator, .{
                .bucket = self.bucket(),
                .key = key,
                .value = val,
                .revision = seq,
                .operation = op,
                .value_allocator = val_alloc,
            }) catch {
                if (val_alloc) |a| a.free(val);
                break;
            };

            msg.deinit();
        }

        return result.toOwnedSlice(allocator);
    }

    // -- Watch --

    /// Watches a key pattern for real-time updates.
    /// Delivers current values (last per subject)
    /// first, then continues with live updates.
    pub fn watch(
        self: *KeyValue,
        key_pattern: []const u8,
    ) !KvWatcher {
        std.debug.assert(key_pattern.len > 0);
        // watch() allows wildcards (*, >) for
        // pattern matching — skip kvSubject validation
        var subj_buf: [256]u8 = undefined;
        const filter = std.fmt.bufPrint(
            &subj_buf,
            "$KV.{s}.{s}",
            .{ self.bucket(), key_pattern },
        ) catch return errors.Error.SubjectTooLong;

        const pull = try self.createEphemeralPull(
            filter,
            .last_per_subject,
            null,
        );

        return KvWatcher{ .kv = self, .pull = pull };
    }

    /// Watches all keys in the bucket. Delivers
    /// current values first, then live updates.
    pub fn watchAll(self: *KeyValue) !KvWatcher {
        var subj_buf: [256]u8 = undefined;
        const filter = std.fmt.bufPrint(
            &subj_buf,
            "$KV.{s}.>",
            .{self.bucket()},
        ) catch return errors.Error.SubjectTooLong;

        const pull = try self.createEphemeralPull(
            filter,
            .last_per_subject,
            null,
        );

        return KvWatcher{ .kv = self, .pull = pull };
    }

    /// Creates an ephemeral consumer with the given
    /// deliver policy and returns a PullSubscription.
    fn createEphemeralPull(
        self: *KeyValue,
        filter: []const u8,
        deliver_policy: types.DeliverPolicy,
        opt_start_seq: ?u64,
    ) !PullSubscription {
        std.debug.assert(filter.len > 0);
        // Generate unique name into stable storage
        const seq = ephemeral_counter.fetchAdd(
            1,
            .monotonic,
        );
        const name = std.fmt.bufPrint(
            &self._eph_name_buf,
            "kv{d}x{d}",
            .{ seq, @intFromPtr(self) % 99999 },
        ) catch unreachable;
        self._eph_name_len = @intCast(name.len);

        var resp = try self.js.createConsumer(
            self.streamName(),
            .{
                .name = name,
                .ack_policy = .none,
                .deliver_policy = deliver_policy,
                .opt_start_seq = opt_start_seq,
                .filter_subject = filter,
                .mem_storage = true,
                .inactive_threshold = 60_000_000_000,
            },
        );
        resp.deinit();

        var pull = PullSubscription{
            .js = self.js,
            .stream = self.streamName(),
        };
        pull.setConsumer(name);
        return pull;
    }

    fn ephName(self: *const KeyValue) []const u8 {
        std.debug.assert(self._eph_name_len > 0);
        return self._eph_name_buf[0..self._eph_name_len];
    }

    fn deleteEphemeralPull(
        self: *KeyValue,
        pull: *PullSubscription,
    ) void {
        var resp = self.js.deleteConsumer(
            self.streamName(),
            pull.consumerName(),
        ) catch return;
        resp.deinit();
    }

    // -- Status --

    /// Returns bucket status information.
    pub fn status(
        self: *KeyValue,
    ) !types.Response(types.StreamInfo) {
        return self.js.streamInfo(self.streamName());
    }

    // -- Helpers --

    fn isDeleteOp(raw_headers: []const u8) bool {
        const op = parseKvOp(raw_headers);
        return op == .delete or op == .purge;
    }

    /// Matches exact KV-Operation header values to
    /// avoid substring false positives.
    fn parseKvOp(raw_headers: []const u8) types.KeyValueOp {
        if (std.mem.indexOf(
            u8,
            raw_headers,
            "KV-Operation: PURGE\r\n",
        ) != null) return .purge;
        if (std.mem.indexOf(
            u8,
            raw_headers,
            "KV-Operation: DEL\r\n",
        ) != null) return .delete;
        return .put;
    }

    fn decodeBase64(
        encoded: []const u8,
        buf: []u8,
    ) ?[]const u8 {
        if (encoded.len == 0) return null;
        const decoder = std.base64.standard.Decoder;
        const len = decoder.calcSizeForSlice(
            encoded,
        ) catch return null;
        if (len > buf.len) return null;
        decoder.decode(
            buf[0..len],
            encoded,
        ) catch return null;
        return buf[0..len];
    }
};

/// Watcher for real-time KV updates using an ephemeral
/// consumer with last_per_subject. Call `next()` to
/// receive entries.
pub const KvWatcher = struct {
    kv: *KeyValue,
    pull: PullSubscription,
    initial_done: bool = false,

    /// Returns the next entry update. Returns null
    /// when no more updates within timeout. First
    /// null after creation means all existing keys
    /// have been delivered.
    pub fn next(
        self: *KvWatcher,
        timeout_ms: u32,
    ) !?types.KeyValueEntry {
        std.debug.assert(timeout_ms > 0);
        var msg = (try self.pull.next(
            timeout_ms,
        )) orelse {
            if (!self.initial_done) {
                self.initial_done = true;
            }
            return null;
        };
        defer msg.deinit();

        var op: types.KeyValueOp = .put;
        if (msg.headers()) |h| {
            op = KeyValue.parseKvOp(h);
        }

        const subj = msg.subject();
        const plen: usize = 4 +
            @as(usize, self.kv.bucket_len) + 1;
        const key = if (subj.len > plen)
            subj[plen..]
        else
            "";

        const md = msg.metadata();
        const seq = if (md) |m|
            m.stream_seq
        else
            0;

        // Extract value before deinit frees msg
        const allocator = self.kv.js.allocator;
        const data = msg.data();
        var val: []const u8 = "";
        var val_alloc: ?Allocator = null;
        if (data.len > 0 and op == .put) {
            val = try allocator.dupe(u8, data);
            val_alloc = allocator;
        }

        return types.KeyValueEntry{
            .bucket = self.kv.bucket(),
            .key = key,
            .value = val,
            .revision = seq,
            .operation = op,
            .value_allocator = val_alloc,
        };
    }

    /// Cleans up the watcher and its consumer.
    pub fn deinit(self: *KvWatcher) void {
        self.kv.deleteEphemeralPull(&self.pull);
    }
};
