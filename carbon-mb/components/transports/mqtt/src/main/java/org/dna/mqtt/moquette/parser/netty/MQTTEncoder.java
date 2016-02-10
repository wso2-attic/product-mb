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
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.MessageToByteEncoder;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;

import java.util.HashMap;
import java.util.Map;

/**
 * @author andrea
 */
public class MQTTEncoder extends MessageToByteEncoder<AbstractMessage> {

    private Map<Byte, org.dna.mqtt.moquette.parser.netty.DemuxEncoder> m_encoderMap = new HashMap<Byte, org.dna.mqtt.moquette.parser.netty.DemuxEncoder>();

    public MQTTEncoder() {
        m_encoderMap.put(AbstractMessage.CONNECT, new ConnectEncoder());
        m_encoderMap.put(AbstractMessage.CONNACK, new org.dna.mqtt.moquette.parser.netty.ConnAckEncoder());
        m_encoderMap.put(AbstractMessage.PUBLISH, new org.dna.mqtt.moquette.parser.netty.PublishEncoder());
        m_encoderMap.put(AbstractMessage.PUBACK, new org.dna.mqtt.moquette.parser.netty.PubAckEncoder());
        m_encoderMap.put(AbstractMessage.SUBSCRIBE, new org.dna.mqtt.moquette.parser.netty.SubscribeEncoder());
        m_encoderMap.put(AbstractMessage.SUBACK, new org.dna.mqtt.moquette.parser.netty.SubAckEncoder());
        m_encoderMap.put(AbstractMessage.UNSUBSCRIBE, new org.dna.mqtt.moquette.parser.netty.UnsubscribeEncoder());
        m_encoderMap.put(AbstractMessage.DISCONNECT, new DisconnectEncoder());
        m_encoderMap.put(AbstractMessage.PINGREQ, new org.dna.mqtt.moquette.parser.netty.PingReqEncoder());
        m_encoderMap.put(AbstractMessage.PINGRESP, new PingRespEncoder());
        m_encoderMap.put(AbstractMessage.UNSUBACK, new org.dna.mqtt.moquette.parser.netty.UnsubAckEncoder());
        m_encoderMap.put(AbstractMessage.PUBCOMP, new PubCompEncoder());
        m_encoderMap.put(AbstractMessage.PUBREC, new org.dna.mqtt.moquette.parser.netty.PubRecEncoder());
        m_encoderMap.put(AbstractMessage.PUBREL, new org.dna.mqtt.moquette.parser.netty.PubRelEncoder());
    }

    @Override
    protected void encode(ChannelHandlerContext chc, AbstractMessage msg, ByteBuf bb) throws Exception {
        org.dna.mqtt.moquette.parser.netty.DemuxEncoder encoder = m_encoderMap.get(msg.getMessageType());
        if (encoder == null) {
            throw new CorruptedFrameException("Can't find any suitable decoder for message type: " + msg.getMessageType());
        }
        encoder.encode(chc, msg, bb);
    }
}
