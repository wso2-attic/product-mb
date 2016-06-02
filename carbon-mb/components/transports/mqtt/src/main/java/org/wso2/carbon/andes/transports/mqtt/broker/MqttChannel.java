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
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.carbon.andes.transports.mqtt.adaptors.common.MessageDeliveryTag;
import org.wso2.carbon.andes.transports.mqtt.adaptors.common.MessageDeliveryTagMap;
import org.wso2.carbon.andes.transports.mqtt.internal.MqttTransport;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.AbstractMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Represents the channel/socket connection between the client
 */
public class MqttChannel {
    /**
     * Channel which will allow communicating with client
     */
    private ChannelHandlerContext channel;

    /**
     * Holds properties relevant to be persisted for the channel
     */
    private Properties connectionProps = new Properties();

    /**
     * In the case of a subscription, the channel will hold the list of topics and the relevant QoS levels the
     * subscription is bound to
     * Key - the topic filter
     * Value - the QoS level
     * <p>
     * This map will be be accessible when a new subscription is added or either its removed/un-subscribed.
     * This will not be concurrently accessed, hence we would not focused on introducing a synchronized data structure
     */
    private Map<String, Integer> topicQoSLevels = new HashMap<>();

    /**
     * responsible to generate delivery tags for messages sent to subscribers
     */
    private MessageDeliveryTagMap<MessageDeliveryTag, Long> deliveryTagMap;

    /**
     * Contains the protocol version
     */
    private BrokerVersion version;

    /**
     * Andes core protocol type.
     */
    private ProtocolType protocolType;

    /**
     * Handles publisher side acknowledgments relevant for the channel
     */
    private PublisherAcknowledgementProcessor publisherAck;

    /**
     * Handles subscriber side acknowledgements

     */
    private SubscriberAcknowledgementProcessor subscriberAck = new SubscriberAcknowledgementProcessor();

    public ChannelHandlerContext getChannel() {
        return this.channel;
    }

    public MqttChannel(ChannelHandlerContext ctx) {
        this.channel = ctx;

        this.publisherAck = new PublisherAcknowledgementProcessor(ctx);

        Queue<MessageDeliveryTag> messageIdList = new ConcurrentLinkedQueue<>();

        //TODO this algorithm should change, also we need to add a static initializer here
        //We start the message id with 2, message id 1 will be sent to qos 0 messages
        for (int messageCount = 2; messageCount != Short.MAX_VALUE; messageCount++) {
            MessageDeliveryTag messageDeliveryTag = new MessageDeliveryTag(messageCount);
            messageIdList.add(messageDeliveryTag);
        }

        //Messages which are fleeting will be kept in this
        Map<MessageDeliveryTag, Long> messageDeliveryTag = new TreeMap<>((o1, o2) -> {
            return o1.compareTo(o2);
        });

        this.deliveryTagMap =  new MessageDeliveryTagMap<>(messageIdList, messageDeliveryTag);

    }

    public SubscriberAcknowledgementProcessor getSubscriberAck() {
        return subscriberAck;
    }

    public PublisherAcknowledgementProcessor getPublisherAckWriter() {
        return publisherAck;
    }

    /**
     * For subscription channels we maintain the list of topics the channel is bound to
     *
     * @param topic the name of the topic
     * @param qos   the level of QoS
     */
    public void addTopic(String topic, Integer qos) {
        topicQoSLevels.put(topic, qos);
    }

    /**
     * Gets the qos of the topic
     *
     * @param topic the name of the topic bound to the channel
     * @return the qos of the subscribed topic either 0,1 or 2
     */
    public Integer getTopic(String topic) {
        return topicQoSLevels.get(topic);
    }


    /**
     * Removes a topic off the list during disconnection/un-subscription
     *
     * @param topic the name of the topic
     */
    public void removeTopic(String topic) {
        topicQoSLevels.remove(topic);
    }


    /**
     * List of topics a channel is bound to
     *
     * @return the list of topics
     */
    public Map<String, Integer> getTopicList() {
        return topicQoSLevels;
    }

    /**
     * Writes a given message back to the channel
     *
     * @param message message which should be written to the client
     */
    public void write(AbstractMessage message) {
        channel.writeAndFlush(message);
    }

    /**
     * Closes the channel
     */
    public void close() {
        this.channel.close();
    }

    /**
     * Adds the property to the list, the property will withstand for the lifetime of the channel
     *
     * @param propertyName  the name of the property
     * @param propertyValue the value of the property
     */
    public void addProperty(String propertyName, String propertyValue) {
        connectionProps.put(propertyName, propertyValue);
    }

    /**
     * Gets the property relevant to a channel by its name
     *
     * @param propertyName the name of the property
     * @return the value of the property
     */
    public String getProperty(String propertyName) {
        return connectionProps.getProperty(propertyName);
    }

    /**
     * Gets the message delivery factory
     *
     * @return the factory which contains message delivery
     */
    public MessageDeliveryTagMap<MessageDeliveryTag, Long> getMessageDeliveryTagMap() {
        return this.deliveryTagMap;
    }

    public BrokerVersion getVersion() {
        return version;
    }

    public void setVersion(BrokerVersion version) throws AndesException {
        this.version = version;
        protocolType = new ProtocolType(MqttTransport.PROTOCOL_NAME, version.toString());
    }

    /**
     * Return the {@link ProtocolType} of the MQTT connection
     * @return ProtocolType
     */
    public ProtocolType getProtocolType() {
        return protocolType;
    }
}
