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


import org.wso2.carbon.andes.transports.mqtt.MqttConstants;
import org.wso2.carbon.andes.transports.mqtt.adaptors.MessagingAdaptor;
import org.wso2.carbon.andes.transports.mqtt.adaptors.common.QOSLevel;
import org.wso2.carbon.andes.transports.mqtt.adaptors.exceptions.AdaptorException;
import org.wso2.carbon.andes.transports.mqtt.broker.MqttChannel;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.AbstractMessage;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.SubAckMessage;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.SubscribeMessage;
import org.wso2.carbon.andes.transports.server.BrokerException;

import java.util.List;

/**
 * Creates the flow which describes the subscription command message actions
 */
public class Subscribe {

    /**
     * Notifies the message store on the subscription
     *
     * @param messageStore specifies the message store which will be informed about the subscription message
     * @param channel      specifies the connection information of the client
     * @return Subscribe
     */
    public static boolean notifyStore(MessagingAdaptor messageStore, SubscribeMessage message, MqttChannel channel)
            throws
            BrokerException {

        //There will be multiple topic subscriptions hence we need to create an Andes Local subscription per each topic
        List<SubscribeMessage.Couple> subscriptions = message.subscriptions();
        SubAckMessage ackMessage = new SubAckMessage();
        ackMessage.setMessageID(message.getMessageID());
        String cleanSessionState = channel.getProperty(MqttConstants.SESSION_DURABILITY_PROPERTY_NAME);
        boolean isCleanSession = Boolean.parseBoolean(cleanSessionState);

        for (SubscribeMessage.Couple subscription : subscriptions) {
            String topicFilter = subscription.getTopicFilter();
            String clientId = channel.getProperty(MqttConstants.CLIENT_ID_PROPERTY_NAME);
            //TODO eventually this API doesn't need to know the user name, authentication will be done
            // in a earlier stage
            String userName = "admin"; //WE HARD CODE THIS FOR NOW
            byte qos = subscription.getQos();
            AbstractMessage.QOSType qosLevel = AbstractMessage.QOSType.valueOf(qos);

            try {
                QOSLevel qoSFromValue = QOSLevel.getQoSFromValue(qosLevel.getValue());
                messageStore.storeSubscriptions(topicFilter, clientId, userName, isCleanSession,
                        qoSFromValue, channel);
                channel.addTopic(topicFilter, qoSFromValue.getValue());
                ackMessage.addType(AbstractMessage.QOSType.valueOf(qos));
            } catch (AdaptorException e) {
                String error = "Error occurred while creating the subscription";
                throw new BrokerException(error, e);
            }
        }

        //TODO add debug logging here
        channel.write(ackMessage);
        return true;
    }
}
