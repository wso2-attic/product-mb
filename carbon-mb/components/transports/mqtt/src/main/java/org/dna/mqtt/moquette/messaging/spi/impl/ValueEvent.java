package org.dna.mqtt.moquette.messaging.spi.impl;

import com.lmax.disruptor.EventFactory;
import org.dna.mqtt.moquette.messaging.spi.impl.events.MessagingEvent;

public final class ValueEvent {

    private MessagingEvent m_event;

    public MessagingEvent getEvent() {
        return m_event;
    }

    public void setEvent(MessagingEvent event) {
        m_event = event;
    }
    
    public final static EventFactory<org.dna.mqtt.moquette.messaging.spi.impl.ValueEvent> EVENT_FACTORY = new EventFactory<org.dna.mqtt.moquette.messaging.spi.impl.ValueEvent>() {

        public org.dna.mqtt.moquette.messaging.spi.impl.ValueEvent newInstance() {
            return new org.dna.mqtt.moquette.messaging.spi.impl.ValueEvent();
        }
    };
}
