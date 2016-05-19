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
import org.wso2.carbon.andes.transports.mqtt.adaptors.andes.utils.MqttUtils;
import org.wso2.carbon.andes.transports.mqtt.adaptors.common.QOSLevel;
import org.wso2.carbon.andes.transports.mqtt.adaptors.exceptions.AdaptorException;
import org.wso2.carbon.andes.transports.mqtt.broker.MqttChannel;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.UnsubAckMessage;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.UnsubscribeMessage;
import org.wso2.carbon.andes.transports.server.BrokerException;

import java.util.Iterator;
import java.util.List;

/**
 * Unsubscribes from an event/connection
 */
public class UnSubscribe {
    private static final Log log = LogFactory.getLog(Disconnect.class);

    /**
     * Notifies the message store on receiving a published message
     *
     * @param messageStore specifies the message store which will be informed about the publish message
     * @param channel      specifies the connection information of the client
     * @return Subscribe
     */
    public static boolean notifyStore(MessagingAdaptor messageStore, UnsubscribeMessage message, MqttChannel channel)
            throws
            BrokerException {
        String clientId = channel.getProperty(MqttConstants.CLIENT_ID_PROPERTY_NAME);
        List<String> topics = message.topicFilters();
        Integer messageId = message.getMessageID();

        boolean isCleanSession = Boolean.parseBoolean(channel.getProperty(MqttConstants
                .SESSION_DURABILITY_PROPERTY_NAME));
        String subscriptionId = channel.getProperty(MqttUtils.CLUSTER_SUB_ID_PROPERTY_NAME);

        for (Iterator<String> topicItr = topics.iterator(); topicItr.hasNext(); ) {
            String topic = topicItr.next();
            String username = "admin";

            try {
                messageStore.storeUnsubscribeMessage(topic, username, clientId, isCleanSession, QOSLevel
                        .getQoSFromValue(channel.getTopic(topic)), subscriptionId);
                channel.removeTopic(topic);
            } catch (AdaptorException e) {
                String error = "Error while disconnecting the subscription";
                log.error(error, e);
                throw new BrokerException(error, e);
            }
        }

        UnsubAckMessage msg = new UnsubAckMessage();
        msg.setMessageID(messageId);
        channel.write(msg);

        log.info("Unsubscription message sent to " + clientId + " with message id " + messageId);
        return true;
    }

}
