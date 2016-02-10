package org.dna.mqtt.moquette.messaging.spi.impl.events;

/**
 * Used to model the connection lost event
 *
 */
public class LostConnectionEvent extends MessagingEvent {
    private String clientID;

    public LostConnectionEvent(String clienID) {
        clientID = clienID;
    }

    public String getClientID() {
        return clientID;
    }
    
}
