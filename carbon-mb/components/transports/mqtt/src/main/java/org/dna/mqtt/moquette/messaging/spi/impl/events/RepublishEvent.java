package org.dna.mqtt.moquette.messaging.spi.impl.events;

/**
 * Data class for republish event
 */
public class RepublishEvent extends MessagingEvent {
    private String clientID;

    public RepublishEvent(String clientID) {
        this.clientID = clientID;
    }

    public String getClientID() {
        return clientID;
    }
}
