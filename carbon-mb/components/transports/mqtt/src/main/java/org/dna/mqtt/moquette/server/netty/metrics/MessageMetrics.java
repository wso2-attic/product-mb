package org.dna.mqtt.moquette.server.netty.metrics;

/**
 * Metrics in the form of message
 */
public class MessageMetrics {
    private long messagesRead = 0;
    private long messageWrote = 0;

    void incrementRead(long numMessages) {
        messagesRead += numMessages;
    }

    void incrementWrote(long numMessages) {
        messageWrote += numMessages;
    }

    public long messagesRead() {
        return messagesRead;
    }

    public long messagesWrote() {
        return messageWrote;
    }
}
