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

package org.wso2.carbon.andes.transports.mqtt.adaptors.andes;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.andes.core.AndesMessageMetadata;
import org.wso2.carbon.andes.core.internal.inbound.PubAckHandler;
import org.wso2.carbon.andes.transports.mqtt.adaptors.andes.utils.MqttUtils;
import org.wso2.carbon.andes.transports.mqtt.broker.PublisherAcknowledgementProcessor;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.AbstractMessage;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.PubAckMessage;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.PubRecMessage;

/**
 * Handles acknowledgments sent/received by andes
 */
public class AndesPublisherAcknowledgementProcessor implements PubAckHandler {

    /**
     * Holds the protocol level publisher acknowledgment processing
     */
    private PublisherAcknowledgementProcessor protocolPublisherAckProcessor;

    private static Log log = LogFactory.getLog(AndesPublisherAcknowledgementProcessor.class);

    public AndesPublisherAcknowledgementProcessor(PublisherAcknowledgementProcessor protocolPublisherAckProcessor) {
        this.protocolPublisherAckProcessor = protocolPublisherAckProcessor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void ack(AndesMessageMetadata metadata) {
        int qos = (Integer) metadata.getTemporaryProperty(MqttUtils.QOSLEVEL);
        String clientId = (String) metadata.getTemporaryProperty(MqttUtils.CLIENT_ID);
        Integer messageId = (Integer) metadata.getTemporaryProperty(MqttUtils.MESSAGE_ID);

        if (qos == AbstractMessage.QOSType.EXACTLY_ONCE.getValue()) {
          /*  PubCompMessage pubCompMessage = new PubCompMessage();
            pubCompMessage.setMessageID(messageId);*/
     /*       String publisherKey = messageId + clientId;
            publishedKeys.add(publisherKey);*/
            protocolPublisherAckProcessor.addPublisherKey(messageId, clientId);
            PubRecMessage pubRecMessage = new PubRecMessage();
            pubRecMessage.setMessageID(messageId);
            if (log.isDebugEnabled()) {
                log.debug("Publisher received will be dispatched to message " + messageId + " having the client id " +
                        clientId);
            }
            protocolPublisherAckProcessor.acknowledge(pubRecMessage);
        } else if (qos == AbstractMessage.QOSType.LEAST_ONE.getValue()) {
            PubAckMessage pubAckMessage = new PubAckMessage();
            pubAckMessage.setMessageID(messageId);
            if (log.isDebugEnabled()) {
                log.debug("Publisher acknowledgment will be dispatched to message " + messageId + " having the client" +
                        " id " +
                        clientId);
            }
            protocolPublisherAckProcessor.acknowledge(pubAckMessage);
        }
    }

    @Override
    public void nack(AndesMessageMetadata metadata) {

    }
}
