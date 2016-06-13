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

package org.wso2.carbon.andes.transports.mqtt.broker.v311.commands;

import io.netty.channel.Channel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.andes.transports.mqtt.MqttConstants;
import org.wso2.carbon.andes.transports.mqtt.adaptors.MessagingAdaptor;
import org.wso2.carbon.andes.transports.mqtt.adaptors.andes.message.MqttMessageContext;
import org.wso2.carbon.andes.transports.mqtt.adaptors.common.QOSLevel;
import org.wso2.carbon.andes.transports.mqtt.adaptors.exceptions.AdaptorException;
import org.wso2.carbon.andes.transports.mqtt.broker.MqttChannel;
import org.wso2.carbon.andes.transports.mqtt.broker.PublisherAcknowledgementProcessor;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.PublishMessage;
import org.wso2.carbon.andes.transports.server.BrokerException;

import java.nio.ByteBuffer;

/**
 * Creates the flow describing the set of action for a publish message
 */
public class Publish {

    private static final Log log = LogFactory.getLog(Publish.class);

    /**
     * Notifies the message store on receiving a published message
     *
     * @param messageStore specifies the message store which will be informed about the publish message
     * @param channel      specifies the connection information of the client
     * @return Subscribe
     */
    public static boolean notifyStore(MessagingAdaptor messageStore, PublishMessage message, MqttChannel channel)
            throws
            BrokerException {
        String topic = message.getTopicName();
        int qosLevel = message.getQos().getValue();
        ByteBuffer msgPayload = message.getPayload();
        boolean retain = message.isRetainFlag();
        //We'll have to unbox the decoded value here message ID could be null
        //TODO do the change here
        int mqttLocalMessageID = message.getMessageID() == null ? -1 : message.getMessageID();
        String clientId = channel.getProperty(MqttConstants.CLIENT_ID_PROPERTY_NAME);
        Channel link = channel.getChannel().channel();
        PublisherAcknowledgementProcessor publisherAckWriter = channel.getPublisherAckWriter();

        MqttMessageContext messageContext = new MqttMessageContext(topic, QOSLevel.getQoSFromValue(qosLevel),
                msgPayload, retain, mqttLocalMessageID, clientId, publisherAckWriter, channel.getProtocolType(), link);
        try {
            if (qosLevel != QOSLevel.EXACTLY_ONCE.getValue()) {
                //QoS 1 and 2 messages will be stored
                messageStore.storePublishedMessage(messageContext);

            } else {
                //If its qos 2 message we need to validate whether the message is already persisted
                if (!channel.getPublisherAckWriter().isMessagePersisted(mqttLocalMessageID, clientId)) {
                    messageStore.storePublishedMessage(messageContext);
                } else {
                    log.warn("Message with id " + mqttLocalMessageID + " from client " + clientId + " published a QoS" +
                            " 2 message which has already being submitted to the store, its a possibly retry");
                }

            }
        } catch (AdaptorException e) {
            String error = "Error while storing the published message with id " + message.getMessageID();
            log.error(error, e);
            throw new BrokerException(error, e);
        }
        return true;
    }
}
