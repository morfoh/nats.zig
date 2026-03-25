//! JetStream push-based consumer subscription.
//!
//! Push consumers have a deliver_subject configured and the
//! server pushes messages to that subject. The client
//! subscribes and processes messages as they arrive.

const std = @import("std");
const Allocator = std.mem.Allocator;

const nats = @import("../nats.zig");
const Client = nats.Client;

const types = @import("types.zig");
const errors = @import("errors.zig");
const consumer_mod = @import("consumer.zig");
const JsMsg = @import("message.zig").JsMsg;
const JetStream = @import("JetStream.zig");

const JsMsgHandler = consumer_mod.JsMsgHandler;
const ConsumeContext = consumer_mod.ConsumeContext;
const HeartbeatMonitor = consumer_mod.HeartbeatMonitor;

/// Push-based consumer subscription. Created after a
/// push consumer exists on the server. Subscribe to
/// the deliver_subject and process messages via
/// consume().
pub const PushSubscription = struct {
    js: *JetStream,
    stream: []const u8,
    consumer_buf: [48]u8 = undefined,
    consumer_len: u8 = 0,
    deliver_buf: [256]u8 = undefined,
    deliver_len: u16 = 0,
    deliver_group_buf: [64]u8 = undefined,
    deliver_group_len: u8 = 0,

    /// Returns consumer name.
    pub fn consumerName(
        self: *const PushSubscription,
    ) []const u8 {
        std.debug.assert(self.consumer_len > 0);
        return self.consumer_buf[0..self.consumer_len];
    }

    /// Returns the deliver subject.
    pub fn deliverSubject(
        self: *const PushSubscription,
    ) []const u8 {
        std.debug.assert(self.deliver_len > 0);
        return self.deliver_buf[0..self.deliver_len];
    }

    /// Sets consumer name.
    pub fn setConsumer(
        self: *PushSubscription,
        name: []const u8,
    ) void {
        std.debug.assert(name.len > 0);
        std.debug.assert(name.len <= self.consumer_buf.len);
        @memcpy(
            self.consumer_buf[0..name.len],
            name,
        );
        self.consumer_len = @intCast(name.len);
    }

    /// Sets the deliver subject.
    pub fn setDeliverSubject(
        self: *PushSubscription,
        subj: []const u8,
    ) void {
        std.debug.assert(subj.len > 0);
        std.debug.assert(subj.len <= self.deliver_buf.len);
        @memcpy(
            self.deliver_buf[0..subj.len],
            subj,
        );
        self.deliver_len = @intCast(subj.len);
    }

    /// Sets the deliver group (queue group).
    pub fn setDeliverGroup(
        self: *PushSubscription,
        group: []const u8,
    ) void {
        std.debug.assert(group.len > 0);
        std.debug.assert(
            group.len <= self.deliver_group_buf.len,
        );
        @memcpy(
            self.deliver_group_buf[0..group.len],
            group,
        );
        self.deliver_group_len = @intCast(group.len);
    }

    /// Options for push consumption.
    pub const ConsumeOpts = struct {
        heartbeat_ms: u32 = 0,
        err_handler: ?consumer_mod.ErrHandler = null,
    };

    /// Starts callback-based consumption on the
    /// deliver subject. Messages are dispatched to
    /// the handler. Returns a ConsumeContext for
    /// stop/drain control.
    pub fn consume(
        self: *PushSubscription,
        handler: JsMsgHandler,
        opts: ConsumeOpts,
    ) !ConsumeContext {
        std.debug.assert(self.deliver_len > 0);
        std.debug.assert(self.consumer_len > 0);

        const client = self.js.client;
        const subj = self.deliverSubject();

        const qg: ?[]const u8 =
            if (self.deliver_group_len > 0)
                self.deliver_group_buf[0..self.deliver_group_len]
            else
                null;
        const sub = try client.queueSubscribeSync(
            subj,
            qg,
        );
        errdefer sub.deinit();

        // Flush to ensure SUB reaches the server
        // before the caller creates the push consumer.
        // Without this, the server may start pushing
        // before our subscription is registered.
        try client.flush(5_000_000_000);

        var ctx = ConsumeContext{};

        // Use OS thread instead of io.async because
        // nextMsgTimeout is a spin loop that never
        // yields to the IO runtime. io.async tasks
        // can't be canceled while spin-looping.
        ctx._thread = std.Thread.spawn(
            .{},
            pushConsumeTask,
            .{ client, sub, handler, opts, &ctx },
        ) catch return error.ThreadSpawnFailed;

        return ctx;
    }
};

/// Background task for push-based consumption.
fn pushConsumeTask(
    client: *Client,
    sub: *Client.Sub,
    handler: JsMsgHandler,
    opts: PushSubscription.ConsumeOpts,
    ctx: *ConsumeContext,
) void {
    defer {
        sub.deinit();
        ctx._state.store(.stopped, .release);
    }

    var hb: ?HeartbeatMonitor = if (opts.heartbeat_ms > 0)
        HeartbeatMonitor.init(opts.heartbeat_ms)
    else
        null;
    // Short poll interval so we check ctx.state often.
    // For heartbeat mode, use 2x heartbeat; otherwise
    // 1s keeps the loop responsive to stop/drain.
    const recv_ms: u32 = if (hb) |h|
        h.timeoutMs()
    else
        1000;

    while (ctx.state() == .running or
        ctx.state() == .draining)
    {
        const maybe = sub.nextMsgTimeout(
            recv_ms,
        ) catch |err| {
            if (opts.err_handler) |eh| eh(err);
            if (ctx.state() == .draining) break;
            continue;
        };
        const msg = maybe orelse {
            if (ctx.state() == .draining) break;
            if (hb) |*h| {
                if (h.recordTimeout()) {
                    if (opts.err_handler) |eh|
                        eh(errors.Error.NoHeartbeat);
                    break;
                }
            }
            continue;
        };

        if (hb) |*h| h.recordActivity();

        // Handle status messages
        if (msg.status()) |code| {
            if (code == 100) {
                // Heartbeat / flow control
                if (msg.reply_to) |reply| {
                    client.publish(
                        reply,
                        "",
                    ) catch {};
                }
                msg.deinit();
                continue;
            }
            msg.deinit();
            switch (code) {
                409 => break,
                else => continue,
            }
        }

        var js_msg = JsMsg{
            .msg = msg,
            .client = client,
        };
        handler.dispatch(&js_msg);
    }
}
