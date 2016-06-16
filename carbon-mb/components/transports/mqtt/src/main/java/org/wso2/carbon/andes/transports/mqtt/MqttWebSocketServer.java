/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.andes.transports.mqtt;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.timeout.IdleStateHandler;
import org.wso2.carbon.andes.transports.mqtt.netty.handlers.IdleStateEventMqttHandler;
import org.wso2.carbon.andes.transports.mqtt.netty.handlers.MqttMessagingHandler;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.decoders.MQTTDecoder;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.encoders.MQTTEncoder;
import org.wso2.carbon.andes.transports.server.AbstractServer;

import java.util.List;

/**
 * Initializes websockets over MQTT transport
 */
public class MqttWebSocketServer extends AbstractServer {

    @Override
    public void createPipeline(ChannelPipeline pipeline) {
        //We define an additional pipeline to validate ssl certificates
        // pipeline.addLast("mqttSSL", getSSLConnectionEngine());

        //We would want to encode HTTP header portion of websockets
        pipeline.addLast("httpResponseEncoder", new HttpResponseEncoder());

        //We need to decode HTTP requests
        pipeline.addLast("httpRequestDecoder", new HttpRequestDecoder());

        pipeline.addLast("aggregator", new HttpObjectAggregator(65536));

        pipeline.addLast("WebsocketProtocolHandler", new WebSocketServerProtocolHandler("/mqtt", "mqttv3.1, mqttv3.1" +
                ".1"));

        pipeline.addLast("WebsocketToByteBufferDecoder", new WebSocketFrameToByteBufDecoder());

        pipeline.addLast("ByteToWebsocketEncoder", new ByteBufToWebSocketFrameEncoder());

        //Triggers when there's no activity for the channel (read/write) for DEFAULT_CONNECT_TIMEOUT
        pipeline.addFirst("idleStateMQTTHandler", new IdleStateHandler(MqttConstants.READER_IDLE_TIME,
                MqttConstants.WRITER_IDLE_TIME,
                MqttConstants.DEFAULT_CONNECT_TIMEOUT));

        //idleStateHandler would need to notify when its condition is fulfilled
        //Following would trigger the event generated from idleStateHandler
        //We need to also ensure this is triggered right after idle state handler
        pipeline.addAfter("idleStateMQTTHandler", "idleStateMQTTEventHandler", new IdleStateEventMqttHandler());

        //The MQTTDecoder extends ChannelInboundHandlerAdapter, the incoming bytes will be first decoded through this
        pipeline.addLast("decoder", new MQTTDecoder());
        //MQTTEncoder extends ChannelOutboundHandlerAdapter, the outbound messages will be converted to bytes
        pipeline.addLast("encoder", new MQTTEncoder());
        //Once decoded, we need to handle the decoded message and manage the brokering aspect
        pipeline.addLast("handler", new MqttMessagingHandler());

    }

    static class WebSocketFrameToByteBufDecoder extends MessageToMessageDecoder<BinaryWebSocketFrame> {

        @Override
        protected void decode(ChannelHandlerContext chc, BinaryWebSocketFrame frame, List<Object> out) throws
                Exception {
            //convert the frame to a ByteBuf
            ByteBuf frameBytes = frame.content();
            frameBytes.retain();
            out.add(frameBytes);
        }
    }

    static class ByteBufToWebSocketFrameEncoder extends MessageToMessageEncoder<ByteBuf> {

        @Override
        protected void encode(ChannelHandlerContext chc, ByteBuf bb, List<Object> out) throws Exception {
            //convert the ByteBuf to a WebSocketFrame
            BinaryWebSocketFrame result = new BinaryWebSocketFrame();
            result.content().writeBytes(bb);
            out.add(result);
        }
    }

}
