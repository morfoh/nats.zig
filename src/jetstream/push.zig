//! JetStream push-based consumer subscription.
//!
//! Push consumers have a deliver_subject configured and the
//! server pushes messages to that subject. The client
//! subscribes using a callback and processes messages as
//! they arrive on the IO thread.

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
        std.debug.assert(
            name.len <= self.consumer_buf.len,
        );
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
        std.debug.assert(
            subj.len <= self.deliver_buf.len,
        );
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
    /// deliver subject. Uses the client's native
    /// callback subscription (runs on IO thread).
    /// To stop: call the returned context's deinit().
    pub fn consume(
        self: *PushSubscription,
        handler: JsMsgHandler,
        opts: ConsumeOpts,
    ) !PushConsumeContext {
        _ = opts;
        std.debug.assert(self.deliver_len > 0);
        std.debug.assert(self.consumer_len > 0);

        const client = self.js.client;
        const subj = self.deliverSubject();

        // Allocate wrapper on heap so it outlives
        // this function. Stores the JsMsgHandler
        // and a pointer back to the client.
        const wrapper = try client.allocator.create(
            PushCallbackWrapper,
        );
        wrapper.* = .{
            .handler = handler,
            .client = client,
            .allocator = client.allocator,
        };

        const qg: ?[]const u8 =
            if (self.deliver_group_len > 0)
                self.deliver_group_buf[0..self.deliver_group_len]
            else
                null;

        // Use client.subscribe (callback mode).
        // This runs callbackDrainFn on the IO thread.
        // Messages are dispatched via the wrapper.
        const sub = try client.queueSubscribe(
            subj,
            qg,
            Client.MsgHandler.init(
                PushCallbackWrapper,
                wrapper,
            ),
        );

        // Flush to ensure SUB reaches the server
        // before the caller creates the push consumer.
        try client.flush(5_000_000_000);

        return PushConsumeContext{
            .sub = sub,
            .wrapper = wrapper,
        };
    }
};

/// Heap-allocated wrapper that bridges Client.MsgHandler
/// (receives *const Message) to JsMsgHandler (receives
/// *JsMsg). Lives on the heap because the callback
/// subscription outlives the consume() call.
const PushCallbackWrapper = struct {
    handler: JsMsgHandler,
    client: *Client,
    allocator: Allocator,

    pub fn onMessage(
        self: *PushCallbackWrapper,
        msg: *const Client.Message,
    ) void {
        // Create a mutable copy of the message for
        // JsMsg (ack needs to publish to reply_to).
        // The callback owns `msg` -- it will be
        // deinited by callbackDrainFn after we return.
        // JsMsg wraps the message but doesn't own it.
        var js_msg = JsMsg{
            .msg = msg.*,
            .client = self.client,
        };
        self.handler.dispatch(&js_msg);
    }
};

/// Context for controlling an active push consume.
/// Simpler than ConsumeContext -- just wraps the
/// subscription. Stopping = unsubscribing.
pub const PushConsumeContext = struct {
    sub: ?*Client.Sub,
    wrapper: *PushCallbackWrapper,

    /// Stops consumption. Safe to call before deinit.
    pub fn stop(self: *PushConsumeContext) void {
        if (self.sub) |s| {
            s.deinit();
            self.sub = null;
        }
    }

    /// Stops (if not already) and frees resources.
    pub fn deinit(self: *PushConsumeContext) void {
        self.stop();
        self.wrapper.allocator.destroy(
            self.wrapper,
        );
    }
};
