package org.dna.mqtt.moquette.messaging.spi.impl.events;

import org.dna.mqtt.moquette.proto.messages.AbstractMessage.QOSType;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;
import org.dna.mqtt.moquette.server.ServerChannel;

import java.nio.ByteBuffer;

/**
 * This class is responsible of publishing events
 */
public class PublishEvent extends MessagingEvent {
    String topic;
    QOSType qos;
    ByteBuffer message;
    boolean retain;
    String clientID;
    //Optional attribute, available only fo QoS 1 and 2
    int msgID;

    transient ServerChannel session;

    public PublishEvent(PublishMessage pubMsg, String clientID, ServerChannel session) {
        topic = pubMsg.getTopicName();
        qos = pubMsg.getQos();
        message = pubMsg.getPayload();
        retain = pubMsg.isRetainFlag();
        this.clientID = clientID;
        this.session = session;
        if (pubMsg.getQos() != QOSType.MOST_ONE) {
            msgID = pubMsg.getMessageID();
        }
    }

    public PublishEvent(String topic, QOSType qos, ByteBuffer message, boolean retain,
                        String clientID, ServerChannel session) {
        this.topic = topic;
        this.qos = qos;
        this.message = message;
        this.retain = retain;
        this.clientID = clientID;
        this.session = session;
    }

    public PublishEvent(String topic, QOSType qos, ByteBuffer message, boolean retain,
                        String clientID, int msgID, ServerChannel session) {
        this(topic, qos, message, retain, clientID, session);
        this.msgID = msgID;
    }

    public String getTopic() {
        return topic;
    }

    public QOSType getQos() {
        return qos;
    }

    public ByteBuffer getMessage() {
        return message;
    }

    public boolean isRetain() {
        return retain;
    }

    public String getClientID() {
        return clientID;
    }

    public int getMessageID() {
        return msgID;
    }

    public ServerChannel getSession() {
        return session;
    }

    @Override
    public String toString() {
        return "PublishEvent{" +
               "m_msgID=" + msgID +
               ", m_clientID='" + clientID + '\'' +
               ", m_retain=" + retain +
               ", m_qos=" + qos +
               ", m_topic='" + topic + '\'' +
               '}';
    }
}
