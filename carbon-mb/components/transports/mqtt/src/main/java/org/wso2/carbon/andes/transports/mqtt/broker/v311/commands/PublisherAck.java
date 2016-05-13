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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.andes.transports.mqtt.MqttConstants;
import org.wso2.carbon.andes.transports.mqtt.adaptors.MessagingAdaptor;
import org.wso2.carbon.andes.transports.mqtt.adaptors.common.MessageDeliveryTag;
import org.wso2.carbon.andes.transports.mqtt.adaptors.exceptions.AdaptorException;
import org.wso2.carbon.andes.transports.mqtt.broker.MqttChannel;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.PubAckMessage;
import org.wso2.carbon.andes.transports.server.BrokerException;

/**
 * Acknowledgment sent by the publisher to the broker upon accepting a message
 */
public class PublisherAck {

    private static final Log log = LogFactory.getLog(PublisherAck.class);

    /**
     * Notifies the message store on receiving an ack from QoS 1 subscription
     *
     * @param messageStore specifies the message store which will be informed about the publish message
     * @param channel      specifies the connection information of the client
     * @return Subscribe
     */
    public static boolean notifyStore(MessagingAdaptor messageStore, PubAckMessage message, MqttChannel channel) throws
            BrokerException {
        try {
            MessageDeliveryTag deliveryTag = new MessageDeliveryTag(message.getMessageID());
            Long messageId = channel.getMessageDeliveryTagMap().ackReceived(deliveryTag);
            messageStore.storeSubscriberAcknowledgment(messageId, channel);
        } catch (AdaptorException e) {
            String error = "Error occurred while processing the ack for message " + message.getMessageID
                    () + " originating from channel " + channel.getProperty(MqttConstants.CLIENT_ID_PROPERTY_NAME);
            throw new BrokerException(error, e);
        }
        return true;
    }

}
