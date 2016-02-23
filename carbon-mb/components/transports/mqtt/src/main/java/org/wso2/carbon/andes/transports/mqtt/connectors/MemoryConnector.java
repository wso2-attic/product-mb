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

package org.wso2.carbon.andes.transports.mqtt.connectors;

import org.wso2.carbon.andes.transports.mqtt.MqttChannel;
import org.wso2.carbon.andes.transports.mqtt.MqttConstants;
import org.wso2.carbon.andes.transports.mqtt.distribution.MqttMessageContext;
import org.wso2.carbon.andes.transports.mqtt.distribution.bridge.MessageDeliveryTag;
import org.wso2.carbon.andes.transports.mqtt.distribution.bridge.QOSLevel;
import org.wso2.carbon.andes.transports.mqtt.exceptions.ConnectorException;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.AbstractMessage;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.ConnectMessage;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.PublishMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This will bypass the andes message store and could be used for unit testing purposes
 */
public class MemoryConnector implements IConnector {

    /**
     * <p>
     * Holds the list of subscription against its topic
     * </p>
     * <p>
     * <b>Note:</b> in-memory mode will always send/receive from QoS 0
     * </p>
     */
    private Map<String, Map<String, MqttChannel>> subscriptions = new ConcurrentHashMap<>();

    @Override
    public void storeConnection(ConnectMessage message) throws ConnectorException {
        //We could bypass this step
    }

    @Override
    public void storeSubscriptions(String topic, String clientId, String username, boolean isCleanSession, QOSLevel
            qos, MqttChannel mqttChannel) throws ConnectorException {

        Map<String, MqttChannel> mqttChannels = subscriptions.get(topic);
        if (mqttChannels == null) {
            mqttChannels = new HashMap<>();
        }

        mqttChannels.put(mqttChannel.getProperty(MqttConstants.CLIENT_ID_PROPERTY_NAME), mqttChannel);
        subscriptions.put(topic, mqttChannels);
    }

    @Override
    public void storePublishedMessage(MqttMessageContext messageContext) throws ConnectorException {
        //We need to distribute the message to each subscriber in the list
        Map<String, MqttChannel> mqttChannels = subscriptions.get(messageContext.getTopic());

        if (null != mqttChannels) {
            for (MqttChannel channel : mqttChannels.values()) {
                PublishMessage pubMessage = new PublishMessage();
                pubMessage.setRetainFlag(false);
                pubMessage.setTopicName(messageContext.getTopic());
                pubMessage.setPayload(messageContext.getMessage());
                pubMessage.setMessageID(1);
                pubMessage.setQos(AbstractMessage.QOSType.MOST_ONE);
                //We're ready for the data to be written back to the channel
                //Re initialize the position
                messageContext.getMessage().flip();
                channel.write(pubMessage);
            }
        }


    }

    @Override
    public void storeDisconnectMessage(String topicName, String clientId, boolean isCleanSession, QOSLevel qosLevel)
            throws ConnectorException {
        Map<String, MqttChannel> stringMqttChannelMap = subscriptions.get(topicName);
        stringMqttChannelMap.remove(clientId);
    }

    @Override
    public void storeUnsubscribeMessage(String subscribedTopic, String username, String clientId, boolean
            isCleanSession, QOSLevel qosLevel) throws ConnectorException {
        Map<String, MqttChannel> stringMqttChannelMap = subscriptions.get(subscribedTopic);
        stringMqttChannelMap.remove(clientId);

    }

    @Override
    public void storeSubscriberAcknowledgment(long messageID, MqttChannel channel) throws ConnectorException {
        //This will not be applicable for in-memory mode
    }

    @Override
    public void storeRejection(MessageDeliveryTag deliveryTag, MqttChannel channel) throws ConnectorException {
        //This will not be applicable for in-memory mode
    }
}
