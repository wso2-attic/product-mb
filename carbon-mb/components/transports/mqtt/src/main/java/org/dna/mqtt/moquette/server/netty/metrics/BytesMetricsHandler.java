package org.dna.mqtt.moquette.server.netty.metrics;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

public class BytesMetricsHandler extends ChannelDuplexHandler {

    private static final AttributeKey<org.dna.mqtt.moquette.server.netty.metrics.BytesMetrics> ATTR_KEY_METRICS = new AttributeKey<org.dna.mqtt.moquette.server.netty.metrics.BytesMetrics>("BytesMetrics");

    private BytesMetricsCollector m_collector;

    public BytesMetricsHandler(BytesMetricsCollector collector) {
          m_collector = collector;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Attribute<org.dna.mqtt.moquette.server.netty.metrics.BytesMetrics> attr = ctx.attr(ATTR_KEY_METRICS);
        attr.set(new org.dna.mqtt.moquette.server.netty.metrics.BytesMetrics());

        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        org.dna.mqtt.moquette.server.netty.metrics.BytesMetrics metrics = ctx.attr(ATTR_KEY_METRICS).get();
        metrics.incrementRead(((ByteBuf)msg).readableBytes());
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        org.dna.mqtt.moquette.server.netty.metrics.BytesMetrics metrics = ctx.attr(ATTR_KEY_METRICS).get();
        metrics.incrementWrote(((ByteBuf)msg).writableBytes());
        ctx.write(msg, promise);
    }


    @Override
    public void close(ChannelHandlerContext ctx,
                      ChannelPromise promise) throws Exception {
        BytesMetrics metrics = ctx.attr(ATTR_KEY_METRICS).get();
        m_collector.addMetrics(metrics);
        super.close(ctx, promise);
    }
}
