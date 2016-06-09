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

package org.wso2.carbon.andes.transports.mqtt.adaptors;

import org.wso2.carbon.andes.transports.mqtt.adaptors.andes.message.MqttMessageContext;
import org.wso2.carbon.andes.transports.mqtt.adaptors.common.MessageDeliveryTag;
import org.wso2.carbon.andes.transports.mqtt.adaptors.common.QOSLevel;
import org.wso2.carbon.andes.transports.mqtt.adaptors.exceptions.AdaptorException;
import org.wso2.carbon.andes.transports.mqtt.broker.MqttChannel;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.ConnectMessage;


/**
 * <P>
 * External store used to communicate with distribution i.e connecting with andes core (RDBMS), in memory, kafka
 * </P>
 * <p>
 * <b>Note:</b> the notifier should be a stateless model which will only be used to communicate with the underlying
 * persistence.
 * </p>
 * <p>
 * <b>Assumption:</b> it is assumed the storage engine would handle event ordering and synchronizations
 * </p>
 */
public interface MessagingAdaptor {
    /**
     * Will trigger an event from the protocol providing the information of connectivity
     *
     * @param message the message which holds connection details i.e last will message
     * @throws AdaptorException
     */
    void storeConnection(ConnectMessage message) throws AdaptorException;


    /**
     * <p>
     * Will add and indicate the subscription to the kernel the bridge will be provided as the channel
     * since per topic we will only be creating one channel with andes
     * </p>
     *
     * @param topic          the name of the topic which has subscriber/s
     * @param clientId       the id which will distinguish the topic channel (prefixed for cleanSession=false)
     * @param username       carbon username of logged user
     * @param isCleanSession should the connection be durable
     * @param qos            the subscriber specific qos this can be either 0,1 or 2
     * @param mqttChannel    the channel the request coming from
     * @throws AdaptorException
     */
    void storeSubscriptions(String topic, String clientId, String username, boolean isCleanSession, QOSLevel
            qos, MqttChannel mqttChannel) throws AdaptorException;


    /**
     * <p>
     * Will trigger when a message is published to a topic
     * </p>
     *
     * @param messageContext includes the message information to the relevant message connector
     * @throws AdaptorException
     */
    void storePublishedMessage(MqttMessageContext messageContext) throws AdaptorException;

    /**
     * <p>Stores a disconnect message in the store</p>
     *
     * @param topicName      the topic the subscription disconnection should be made
     * @param clientId       the channel id of the disconnected client
     * @param isCleanSession durability of the subscription
     * @param qosLevel       the quality of service level subscribed to
     * @param subscriptionId id of the subscriber the disconnection should be initiated
     * @param channel        Mqtt channel the disconnect request coming from
     * @throws AdaptorException
     */
    void storeDisconnectMessage(String topicName, String clientId, boolean isCleanSession, QOSLevel qosLevel, String
            subscriptionId, MqttChannel channel) throws AdaptorException;

    /**
     * <p>Notifies the store on an un subscription made by the client</p>
     *
     * @param message the un subscription message details
     * @param channel the link to TCP connection with the client
     * @throws AdaptorException
     */

    /**
     * Will trigger when subscriber sends a un subscription message
     *
     * @param subscribedTopic the topic the subscription disconnection should be made
     * @param username        carbon username of logged in user
     * @param clientId        the channel id of the disconnected client
     * @param isCleanSession  durability of the subscription
     * @param qosLevel        the quality of service level subscribed to
     * @param subscriptionId  id of the subscriber the disconnection should be initiated
     * @param channel         mqtt channel the request is coming from
     * @throws AdaptorException
     */
    void storeUnsubscribeMessage(String subscribedTopic, String username, String clientId, boolean
            isCleanSession, QOSLevel qosLevel, String subscriptionId, MqttChannel channel) throws AdaptorException;


    /**
     * <p>Stores subscriber acknowledgment PUBREC received from the subscriber for QoS 2 messages</p>
     *
     * @param messageID the id generated for the subscriber which is unique cluster wide
     * @param channel   the TCP channel for the subscriber
     * @throws AdaptorException
     */
    void storeSubscriberAcknowledgment(long messageID, MqttChannel channel) throws AdaptorException;


    /**
     * <p> Processors un-acked messages and notifies the store to retry</p>
     *
     * @param channel the channel which retries should be initiated
     * @throws AdaptorException
     */
    void storeRejection(MessageDeliveryTag deliveryTag, MqttChannel channel) throws AdaptorException;
}
