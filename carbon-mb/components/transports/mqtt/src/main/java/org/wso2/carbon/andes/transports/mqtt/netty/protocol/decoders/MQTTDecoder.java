/*
 * Copyright (c) 2012-2014 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package org.wso2.carbon.andes.transports.mqtt.netty.protocol.decoders;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.AttributeKey;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.Utils;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.AbstractMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author andrea
 */
public class MQTTDecoder extends ByteToMessageDecoder {

    //3 = 3.1, 4 = 3.1.1
    public static final AttributeKey<Integer> PROTOCOL_VERSION = AttributeKey.valueOf("version");

    private final Map<Byte, DemuxDecoder> decoderMap = new HashMap<>();

    public MQTTDecoder() {
        decoderMap.put(AbstractMessage.CONNECT, new ConnectDecoder());
        decoderMap.put(AbstractMessage.CONNACK, new ConnAckDecoder());
        decoderMap.put(AbstractMessage.PUBLISH, new PublishDecoder());
        decoderMap.put(AbstractMessage.PUBACK, new PubAckDecoder());
        decoderMap.put(AbstractMessage.SUBSCRIBE, new SubscribeDecoder());
        decoderMap.put(AbstractMessage.SUBACK, new SubAckDecoder());
        decoderMap.put(AbstractMessage.UNSUBSCRIBE, new UnsubscribeDecoder());
        decoderMap.put(AbstractMessage.DISCONNECT, new DisconnectDecoder());
        decoderMap.put(AbstractMessage.PINGREQ, new PingReqDecoder());
        decoderMap.put(AbstractMessage.PINGRESP, new PingRespDecoder());
        decoderMap.put(AbstractMessage.UNSUBACK, new UnsubAckDecoder());
        decoderMap.put(AbstractMessage.PUBCOMP, new PubCompDecoder());
        decoderMap.put(AbstractMessage.PUBREC, new PubRecDecoder());
        decoderMap.put(AbstractMessage.PUBREL, new PubRelDecoder());
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        in.markReaderIndex();
        if (!Utils.checkHeaderAvailability(in)) {
            in.resetReaderIndex();
            return;
        }
        in.resetReaderIndex();

        byte messageType = Utils.readMessageType(in);

        DemuxDecoder decoder = decoderMap.get(messageType);
        if (decoder == null) {
            throw new CorruptedFrameException("Can't find any suitable decoder for message type: " + messageType);
        }
        decoder.decode(ctx, in, out);
    }
}
