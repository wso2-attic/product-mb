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

package org.wso2.carbon.andes.transports.mqtt.commands.v300;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.andes.transports.mqtt.MqttChannel;
import org.wso2.carbon.andes.transports.mqtt.MqttConstants;
import org.wso2.carbon.andes.transports.mqtt.SubscriberAcknowledgementProcessor;
import org.wso2.carbon.andes.transports.mqtt.connectors.IConnector;
import org.wso2.carbon.andes.transports.mqtt.distribution.bridge.MessageDeliveryTag;
import org.wso2.carbon.andes.transports.mqtt.exceptions.ConnectorException;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.PubRecMessage;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.PubRelMessage;
import org.wso2.carbon.andes.transports.server.BrokerException;


/**
 * Sent by the subscriber to broker upon receiving a QoS 2 message
 */
public class PublisherReceived {

    private static final Log log = LogFactory.getLog(PublisherReceived.class);

    /**
     * Notifies the message store on receiving a published message
     *
     * @param messageStore specifies the message store which will be informed about the publish message
     * @param channel      specifies the connection information of the client
     * @return Subscribe
     */
    public static boolean notifyStore(IConnector messageStore, PubRecMessage message, MqttChannel channel)
            throws
            BrokerException {

        try {
            //We would notify the store to avoid retries for the message at this point
            SubscriberAcknowledgementProcessor subscriberAck = channel.getSubscriberAck();
            if (!subscriberAck.hasAcknowledged(message.getMessageID())) {


                MessageDeliveryTag deliveryTag = new MessageDeliveryTag(message.getMessageID());
                Long messageId = channel.getMessageDeliveryTagMap().ackReceived(deliveryTag);
                messageStore.storeSubscriberAcknowledgment(messageId, channel);

                //We indicate the message has being acked so the if retried it will be be re-acknowledged
                subscriberAck.messageAcknowledged(message.getMessageID());
            }
            //We need to send the PUBREL at this point
            PubRelMessage pubRelMessage = new PubRelMessage();
            pubRelMessage.setMessageID(message.getMessageID());
            channel.write(pubRelMessage);
        } catch (ConnectorException e) {
            String error = "Error occurred while processing the ack for message " + message.getMessageID
                    () + " originating from channel " + channel.getProperty(MqttConstants.CLIENT_ID_PROPERTY_NAME);
            throw new BrokerException(error, e);
        }

        return true;
    }

}
