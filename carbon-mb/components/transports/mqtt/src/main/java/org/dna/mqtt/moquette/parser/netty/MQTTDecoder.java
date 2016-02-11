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
package org.dna.mqtt.moquette.parser.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.AttributeKey;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author andrea
 */
public class MQTTDecoder extends ByteToMessageDecoder {

    //3 = 3.1, 4 = 3.1.1
    static final AttributeKey<Integer> PROTOCOL_VERSION = AttributeKey.valueOf("version");

    private final Map<Byte, org.dna.mqtt.moquette.parser.netty.DemuxDecoder> decoderMap = new HashMap<>();

    public MQTTDecoder() {
        decoderMap.put(AbstractMessage.CONNECT, new ConnectDecoder());
        decoderMap.put(AbstractMessage.CONNACK, new org.dna.mqtt.moquette.parser.netty.ConnAckDecoder());
        decoderMap.put(AbstractMessage.PUBLISH, new PublishDecoder());
        decoderMap.put(AbstractMessage.PUBACK, new org.dna.mqtt.moquette.parser.netty.PubAckDecoder());
        decoderMap.put(AbstractMessage.SUBSCRIBE, new org.dna.mqtt.moquette.parser.netty.SubscribeDecoder());
        decoderMap.put(AbstractMessage.SUBACK, new org.dna.mqtt.moquette.parser.netty.SubAckDecoder());
        decoderMap.put(AbstractMessage.UNSUBSCRIBE, new org.dna.mqtt.moquette.parser.netty.UnsubscribeDecoder());
        decoderMap.put(AbstractMessage.DISCONNECT, new org.dna.mqtt.moquette.parser.netty.DisconnectDecoder());
        decoderMap.put(AbstractMessage.PINGREQ, new org.dna.mqtt.moquette.parser.netty.PingReqDecoder());
        decoderMap.put(AbstractMessage.PINGRESP, new org.dna.mqtt.moquette.parser.netty.PingRespDecoder());
        decoderMap.put(AbstractMessage.UNSUBACK, new org.dna.mqtt.moquette.parser.netty.UnsubAckDecoder());
        decoderMap.put(AbstractMessage.PUBCOMP, new org.dna.mqtt.moquette.parser.netty.PubCompDecoder());
        decoderMap.put(AbstractMessage.PUBREC, new org.dna.mqtt.moquette.parser.netty.PubRecDecoder());
        decoderMap.put(AbstractMessage.PUBREL, new org.dna.mqtt.moquette.parser.netty.PubRelDecoder());
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

        org.dna.mqtt.moquette.parser.netty.DemuxDecoder decoder = decoderMap.get(messageType);
        if (decoder == null) {
            throw new CorruptedFrameException("Can't find any suitable decoder for message type: " + messageType);
        }
        decoder.decode(ctx, in, out);
    }
}
