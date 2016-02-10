package org.dna.mqtt.moquette.server.netty.metrics;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Collects all the metrics from the various pipeline.
 */
public class MessageMetricsCollector {
    private Queue<MessageMetrics> allMetrics = new ConcurrentLinkedQueue<MessageMetrics>();

    void addMetrics(MessageMetrics metrics) {
        allMetrics.add(metrics);
    }

    public MessageMetrics computeMetrics() {
        MessageMetrics allMetrics = new MessageMetrics();
        for (MessageMetrics m : this.allMetrics) {
            allMetrics.incrementRead(m.messagesRead());
            allMetrics.incrementWrote(m.messagesWrote());
        }
        return allMetrics;
    }
}
