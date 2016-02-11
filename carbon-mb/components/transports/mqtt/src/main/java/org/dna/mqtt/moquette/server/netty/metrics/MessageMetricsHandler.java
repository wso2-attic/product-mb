package org.dna.mqtt.moquette.server.netty.metrics;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Handle message metrics
 */
public class MessageMetricsHandler extends ChannelDuplexHandler {

    private static final AttributeKey<MessageMetrics> ATTR_KEY_METRICS = new AttributeKey<MessageMetrics>
            ("MessageMetrics");

    private MessageMetricsCollector collector;

    private static Log log = LogFactory.getLog(org.dna.mqtt.moquette.server.netty.metrics.MessageMetricsHandler.class);

    public MessageMetricsHandler(MessageMetricsCollector collector) {
        this.collector = collector;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Attribute<MessageMetrics> attr = ctx.attr(ATTR_KEY_METRICS);
        attr.set(new MessageMetrics());

        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        MessageMetrics metrics = ctx.attr(ATTR_KEY_METRICS).get();
        metrics.incrementRead(1);
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        MessageMetrics metrics = ctx.attr(ATTR_KEY_METRICS).get();
        metrics.incrementWrote(1);
        ctx.write(msg, promise);
    }


    @Override
    public void close(ChannelHandlerContext ctx,
                      ChannelPromise promise) throws Exception {
        MessageMetrics metrics = ctx.attr(ATTR_KEY_METRICS).get();
        collector.addMetrics(metrics);
        super.close(ctx, promise);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // We log the error and close the connection at an event where and exception is caught
        log.error(cause.getMessage(), cause);
        ctx.close();
    }

}
