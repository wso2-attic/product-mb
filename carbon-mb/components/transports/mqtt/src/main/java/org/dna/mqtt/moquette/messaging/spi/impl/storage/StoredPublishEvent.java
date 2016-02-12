package org.dna.mqtt.moquette.messaging.spi.impl.storage;

import org.dna.mqtt.moquette.messaging.spi.impl.events.PublishEvent;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage.QOSType;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * TODO: Comment to be added
 */
public class StoredPublishEvent implements Serializable {
    private static final long serialVersionUID = -7704103207007063922L;
    String topic;
    QOSType qos;
    byte[] message;
    boolean retain;
    String clientID;
    //Optional attribute, available only fo QoS 1 and 2
    int msgID;

    public StoredPublishEvent(PublishEvent wrapped) {
        topic = wrapped.getTopic();
        qos = wrapped.getQos();
        retain = wrapped.isRetain();
        clientID = wrapped.getClientID();
        msgID = wrapped.getMessageID();

        ByteBuffer buffer = wrapped.getMessage();
        message = new byte[buffer.remaining()];
        buffer.get(message);
        buffer.rewind();
    }

    public String getTopic() {
        return topic;
    }

    public QOSType getQos() {
        return qos;
    }

    // TODO : C5-Migration(findbugs related) - Remove clone
    public byte[] getMessage() {
        return message.clone();
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
}
