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
package org.wso2.carbon.andes.transports.mqtt.netty.protocol.encoders;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.MessageToByteEncoder;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.AbstractMessage;

import java.util.HashMap;
import java.util.Map;

/**
 * @author andrea
 */
public class MQTTEncoder extends MessageToByteEncoder<AbstractMessage> {

    private Map<Byte , DemuxEncoder> encoderMap = new HashMap<>();

    public MQTTEncoder() {
        encoderMap.put(AbstractMessage.CONNECT, new ConnectEncoder());
        encoderMap.put(AbstractMessage.CONNACK, new ConnAckEncoder());
        encoderMap.put(AbstractMessage.PUBLISH, new PublishEncoder());
        encoderMap.put(AbstractMessage.PUBACK, new PubAckEncoder());
        encoderMap.put(AbstractMessage.SUBSCRIBE, new SubscribeEncoder());
        encoderMap.put(AbstractMessage.SUBACK, new SubAckEncoder());
        encoderMap.put(AbstractMessage.UNSUBSCRIBE, new UnsubscribeEncoder());
        encoderMap.put(AbstractMessage.DISCONNECT, new DisconnectEncoder());
        encoderMap.put(AbstractMessage.PINGREQ, new PingReqEncoder());
        encoderMap.put(AbstractMessage.PINGRESP, new PingRespEncoder());
        encoderMap.put(AbstractMessage.UNSUBACK, new UnsubAckEncoder());
        encoderMap.put(AbstractMessage.PUBCOMP, new PubCompEncoder());
        encoderMap.put(AbstractMessage.PUBREC, new PubRecEncoder());
        encoderMap.put(AbstractMessage.PUBREL, new PubRelEncoder());
    }

    @Override
    protected void encode(ChannelHandlerContext chc, AbstractMessage msg, ByteBuf bb) throws Exception {
        DemuxEncoder encoder = encoderMap.get(msg.getMessageType());
        if (encoder == null) {
            throw new CorruptedFrameException("Can't find any suitable decoder for message type: " + msg
                    .getMessageType());
        }
        encoder.encode(chc , msg , bb);
    }
}
