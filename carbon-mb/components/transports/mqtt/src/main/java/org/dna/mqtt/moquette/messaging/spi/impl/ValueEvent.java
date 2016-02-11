package org.dna.mqtt.moquette.messaging.spi.impl;

import com.lmax.disruptor.EventFactory;
import org.dna.mqtt.moquette.messaging.spi.impl.events.MessagingEvent;

/**
 *
 */
public final class ValueEvent {

    private MessagingEvent event;

    public MessagingEvent getEvent() {
        return event;
    }

    public void setEvent(MessagingEvent event) {
        this.event = event;
    }

    public static final EventFactory<org.dna.mqtt.moquette.messaging.spi.impl.ValueEvent> EVENT_FACTORY = new
            EventFactory<org.dna.mqtt.moquette.messaging.spi.impl.ValueEvent>() {

        public org.dna.mqtt.moquette.messaging.spi.impl.ValueEvent newInstance() {
            return new org.dna.mqtt.moquette.messaging.spi.impl.ValueEvent();
        }
    };
}
