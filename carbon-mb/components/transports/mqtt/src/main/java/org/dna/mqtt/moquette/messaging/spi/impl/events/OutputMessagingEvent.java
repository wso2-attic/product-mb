package org.dna.mqtt.moquette.messaging.spi.impl.events;

import org.dna.mqtt.moquette.proto.messages.AbstractMessage;
import org.dna.mqtt.moquette.server.ServerChannel;

/**
 * Event class for output messages
 */
public class OutputMessagingEvent extends MessagingEvent {
    private ServerChannel channel;
    private AbstractMessage message;

    public OutputMessagingEvent(ServerChannel channel, AbstractMessage message) {
        this.channel = channel;
        this.message = message;
    }

    public ServerChannel getChannel() {
        return channel;
    }

    public AbstractMessage getMessage() {
        return message;
    }
    
    
}
