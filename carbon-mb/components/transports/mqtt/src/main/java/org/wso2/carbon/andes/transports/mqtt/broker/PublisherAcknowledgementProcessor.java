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

package org.wso2.carbon.andes.transports.mqtt.broker;

import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.AbstractMessage;


import java.util.TreeSet;

/**
 * Performs operations which involves writing acknowledgments to a given channel for a published message
 */
public class PublisherAcknowledgementProcessor {

    private static Log log = LogFactory.getLog(PublisherAcknowledgementProcessor.class);

    /**
     * TCP channel of the publisher
     */
    ChannelHandlerContext channel;

    /**
     * <p>
     * Contains the list of published keys for QoS 2 messages, the key will be inserted when PUBREC is sent to the
     * client. The key will be removed when the client sends the PUBCOMP
     * </p>
     * <p>
     * <b>Note : </b> the key will only be inserted after successfully inserting the messages to store, the
     * publisher key is a composition between clientId+messageId
     * </p>
     */
    private TreeSet<String> publishedKeys = new TreeSet<>();


    public PublisherAcknowledgementProcessor(ChannelHandlerContext channel) {
        this.channel = channel;
    }

    /**
     * <p>
     * Validates whether a QoS 2 message is already persisted
     * </p>
     * <p>
     * <b>Note : </b> this function should be called for QoS 2 messages as a validation whether the message is
     * already persisted to guarantee that the delivery is exactly once
     * </p>
     *
     * @param messageId the id of the message
     * @param clientId  the id of the client who published the message
     * @return true if the message is already persisted
     */
    public boolean isMessagePersisted(int messageId, String clientId) {
        String publisherKey = messageId + clientId;
        return publishedKeys.contains(publisherKey);
    }

    /**
     * <p>
     * Releases a QoS 2 message upon receiving an ack from the publisher
     * </p>
     *
     * @param messageId the id of the message
     * @param clientId  the id of the client
     * @return true if the release was successful, false if there was no element which was released
     */
    public boolean releaseMessage(Integer messageId, String clientId) {
        String publisherKey = messageId + clientId;
        return publishedKeys.remove(publisherKey);
    }

    /**
     * Will write the acknowledgment to the client channel
     *
     * @param message contains the acknowledgment message
     */
    public void acknowledge(AbstractMessage message) {
        if (null != channel) {
            channel.write(message);
        } else {
            log.warn("An ack was sent to a message which does not contain a valid publisher channel, probably for a " +
                    "QoS 0 message or the client disconnected when the ack was received");
        }
    }

    /**
     * Adds the publisher key which is messageId + clientId
     *
     * @param messageId the id of the message
     * @param clientId  the id of the client
     */
    public void addPublisherKey(Integer messageId, String clientId) {
        publishedKeys.add(messageId + clientId);
    }

/*    *//**
     * {@inheritDoc}
     *//*
    @Override
    public void ack(AndesMessageMetadata metadata) {
        int qos = (Integer) metadata.getTemporaryProperty(MqttUtils.QOSLEVEL);
        String clientId = (String) metadata.getTemporaryProperty(MqttUtils.CLIENT_ID);
        Integer messageId = (Integer) metadata.getTemporaryProperty(MqttUtils.MESSAGE_ID);

        if (qos == AbstractMessage.QOSType.EXACTLY_ONCE.ordinal()) {
          *//*  PubCompMessage pubCompMessage = new PubCompMessage();
            pubCompMessage.setMessageID(messageId);*//*
     *//*       String publisherKey = messageId + clientId;
            publishedKeys.add(publisherKey);*//*
            addPublisherKey(messageId, clientId);
            PubRecMessage pubRecMessage = new PubRecMessage();
            pubRecMessage.setMessageID(messageId);
            if (log.isDebugEnabled()) {
                log.debug("Publisher received will be dispatched to message " + messageId + " having the client id " +
                        clientId);
            }
            acknowledge(pubRecMessage);
        } else if (qos == AbstractMessage.QOSType.LEAST_ONE.ordinal()) {
            PubAckMessage pubAckMessage = new PubAckMessage();
            pubAckMessage.setMessageID(messageId);
            if (log.isDebugEnabled()) {
                log.debug("Publisher acknowledgment will be dispatched to message " + messageId + " having the client" +
                        " id " +
                        clientId);
            }
            acknowledge(pubAckMessage);
        }
    }

    @Override
    public void nack(AndesMessageMetadata metadata) {

    }*/
}
