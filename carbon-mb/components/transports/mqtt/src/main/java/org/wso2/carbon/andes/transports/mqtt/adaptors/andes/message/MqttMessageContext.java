/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.carbon.andes.transports.mqtt.adaptors.andes.message;

import io.netty.channel.Channel;
import org.wso2.carbon.andes.core.AndesContent;
import org.wso2.carbon.andes.core.AndesEncodingUtil;
import org.wso2.carbon.andes.core.AndesException;
import org.wso2.carbon.andes.core.AndesMessage;
import org.wso2.carbon.andes.core.AndesMessageMetadata;
import org.wso2.carbon.andes.core.AndesMessagePart;
import org.wso2.carbon.andes.core.AndesUtils;
import org.wso2.carbon.andes.core.ProtocolType;
import org.wso2.carbon.andes.transports.mqtt.adaptors.andes.utils.MqttUtils;
import org.wso2.carbon.andes.transports.mqtt.adaptors.common.QOSLevel;
import org.wso2.carbon.andes.transports.mqtt.broker.PublisherAcknowledgementProcessor;

import java.nio.ByteBuffer;

/**
 * Holds payload information and payload pertaining to a MQTT message
 */
public class MqttMessageContext {

    /**
     * Protocol type representation of message
     */
    private final ProtocolType protocolType;
    /**
     * The name of the topic a given payload is published
     */
    private String topic;
    /**
     * The level of QoS this will be either 0,1 or 2
     */
    private QOSLevel qosLevel;
    /**
     * The payload content wrapped as a ByteBuffer
     */
    private ByteBuffer payload;
    /**
     * Should this payload retained/persisted
     */
    private boolean retain;
    /**
     * MQTT local message ID
     */
    private int mqttLocalMessageID;
    /**
     * Holds the id of the publisher
     */
    private String publisherID;
    /**
     * The ack handler that will ack for QoS 1 and 2 messages
     */
    private PublisherAcknowledgementProcessor pubAckHandler;
    /**
     * The channel used to communicate with the publisher
     */
    private Channel channel;

    /**
     * Value to indicate, if the payload is a compressed one or not
     */
    private boolean isCompressed = false;

    /**
     * Message arrival time to the protocol level
     */
    private long arrivalTime;

    /**
     * Creates an MQTT message with metadata and the payload
     * @param topic              the name of the topic the message was sent
     * @param qosLevel           the level of QoS
     * @param msgPayload         the message in bytes
     * @param retain             should this message be persisted
     * @param mqttLocalMessageID the local id of the mqtt message
     * @param publisherID        the id of the publisher
     * @param pubAckHandler      the acknowledgment handler
     * @param socket             the channel in which the communication occurred
     */
    public MqttMessageContext(String topic, QOSLevel qosLevel, ByteBuffer msgPayload, boolean retain,
                              int mqttLocalMessageID, String publisherID,
                              PublisherAcknowledgementProcessor pubAckHandler,
                              ProtocolType protocolType, Channel socket) {
        this.protocolType = protocolType;
        setTopic(topic);
        setQosLevel(qosLevel);
        setPayload(msgPayload);
        setRetain(retain);
        setMqttLocalMessageID(mqttLocalMessageID);
        setPublisherID(publisherID);
        setPubAckHandler(pubAckHandler);
        setChannel(socket);
    }

    /**
     * Create a {@link MqttMessageContext} object from an {@link AndesMessage}
     * @param andesMessageMetadata {@link AndesMessageMetadata}
     * @param content              {@link AndesContent}
     * @throws AndesException
     */
    public MqttMessageContext(AndesMessageMetadata andesMessageMetadata, AndesContent content) throws AndesException {
        protocolType = andesMessageMetadata.getProtocolType();
        setTopic(andesMessageMetadata.getDestination());
        setRetain(andesMessageMetadata.isRetain());
        setArrivalTime(andesMessageMetadata.getArrivalTime());
        decodeProtocolMetadata(andesMessageMetadata.getProtocolMetadata());
        ByteBuffer buf = ByteBuffer.allocate(content.getContentLength());
        content.putContent(0, buf);
        setPayload(buf);

    }

    /**
     * Encode protocol related metadata
     * @return byte array representation of encoded protocol metadata
     */
    private byte[] encodeProtcolMetadata() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(AndesEncodingUtil.getEncodedIntLength());
        AndesEncodingUtil.putInt(byteBuffer, getQosLevel().getValue()); // encode qos level

        return byteBuffer.array();
    }

    /**
     * Decode protocol related metadata
     * @param protocolMetadata byte array of the encoded metadata
     */
    private void decodeProtocolMetadata(byte[] protocolMetadata) {
        ByteBuffer buffer = ByteBuffer.wrap(protocolMetadata);
        setQosLevel(QOSLevel.getQoSFromValue(AndesEncodingUtil.getEncodedInt(buffer))); // decode qos
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public QOSLevel getQosLevel() {
        return qosLevel;
    }

    public void setQosLevel(QOSLevel qosLevel) {
        this.qosLevel = qosLevel;
    }

    public ByteBuffer getPayload() {
        return payload;
    }

    public void setPayload(ByteBuffer message) {
        this.payload = message;
    }

    public boolean isRetain() {
        return retain;
    }

    public void setRetain(boolean retain) {
        this.retain = retain;
    }

    public int getMqttLocalMessageID() {
        return mqttLocalMessageID;
    }

    public void setMqttLocalMessageID(int mqttLocalMessageID) {
        this.mqttLocalMessageID = mqttLocalMessageID;
    }

    public String getPublisherID() {
        return publisherID;
    }

    public void setPublisherID(String publisherID) {
        this.publisherID = publisherID;
    }

    public PublisherAcknowledgementProcessor getPubAckHandler() {
        return pubAckHandler;
    }

    public void setPubAckHandler(PublisherAcknowledgementProcessor pubAckHandler) {
        this.pubAckHandler = pubAckHandler;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public boolean isCompressed() {
        return isCompressed;
    }

    /**
     * Get the {@link AndesMessage} representation of {@link MqttMessageContext}
     * @return AndesMessage
     */
    public AndesMessage toAndesMessage() {

        AndesMessageMetadata andesMetadata = new AndesMessageMetadata(getTopic(), protocolType);
        andesMetadata.setPersistent(true);
        andesMetadata.setRetain(isRetain());
        andesMetadata.setStorageDestination(getTopic());
        andesMetadata.setCompressed(isCompressed());
        andesMetadata.setArrivalTime(getArrivalTime());
        andesMetadata.setProtocolMetadata(encodeProtcolMetadata());
        andesMetadata.setMessageContentLength(payload.array().length);
        andesMetadata.setDeliveryStrategy(AndesUtils.TOPIC_DELIVERY_STRATEGY);
        // Add properties to be used for publisher acks
        andesMetadata.addTemporaryProperty(MqttUtils.CLIENT_ID, getPublisherID());
        andesMetadata.addTemporaryProperty(MqttUtils.MESSAGE_ID, getMqttLocalMessageID());
        andesMetadata.addTemporaryProperty(MqttUtils.QOSLEVEL, getQosLevel().getValue());

        AndesMessage andesMessage = new AndesMessage(andesMetadata);
        andesMessage.addMessagePart(toAndesMessagePart(payload.array()));

        return andesMessage;
    }

    @Override
    public String toString() {
        return "MqttMessageContext{" +
                "topic='" + topic + '\'' +
                ", qosLevel=" + qosLevel +
                ", retain=" + retain +
                ", mqttLocalMessageID=" + mqttLocalMessageID +
                ", publisherID='" + publisherID + '\'' +
                ", pubAckHandler=" + pubAckHandler +
                ", channel=" + channel +
                ", isCompressed=" + isCompressed +
                '}';
    }

    /**
     * The published messages will be taken in as a byte stream, the message will be transformed into
     * AndesMessagePart as
     * its required by the Andes kernal for processing
     *
     * @param message  the message contents
     * @return AndesMessagePart which wraps the message into a Andes kernal compliant object
     */
    private AndesMessagePart toAndesMessagePart(byte[] message) {
        AndesMessagePart messageBody = new AndesMessagePart();
        messageBody.setOffSet(0); //Here we set the offset to 0, but it will be a problem when large messages are sent
        messageBody.setData(message);
        messageBody.setDataLength(message.length);
        return messageBody;
    }

    /**
     * Message arrival time to the protocol level
     */
    public long getArrivalTime() {
        return arrivalTime;
    }

    public void setArrivalTime(long arrivalTime) {
        this.arrivalTime = arrivalTime;
    }
}
