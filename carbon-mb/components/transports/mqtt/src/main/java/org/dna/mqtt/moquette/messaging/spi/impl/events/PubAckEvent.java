package org.dna.mqtt.moquette.messaging.spi.impl.events;

/**
 * Class to keep data of PubAckEvent
 */
public class PubAckEvent extends MessagingEvent {

    int messageId;

    String clientID;

    public PubAckEvent(int messageID, String clientID) {
        messageId = messageID;
        this.clientID = clientID;
    }

    public int getMessageId() {
        return messageId;
    }

    public String getClientID() {
        return clientID;
    }
}
