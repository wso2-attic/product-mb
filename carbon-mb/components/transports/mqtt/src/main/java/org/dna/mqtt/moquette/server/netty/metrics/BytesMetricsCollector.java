package org.dna.mqtt.moquette.server.netty.metrics;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Collects all the metrics from the various pipeline.
 */
public class BytesMetricsCollector {
    private Queue<org.dna.mqtt.moquette.server.netty.metrics.BytesMetrics> m_allMetrics = new ConcurrentLinkedQueue<org.dna.mqtt.moquette.server.netty.metrics.BytesMetrics>();

    void addMetrics(org.dna.mqtt.moquette.server.netty.metrics.BytesMetrics metrics) {
        m_allMetrics.add(metrics);
    }

    public org.dna.mqtt.moquette.server.netty.metrics.BytesMetrics computeMetrics() {
        org.dna.mqtt.moquette.server.netty.metrics.BytesMetrics allMetrics = new org.dna.mqtt.moquette.server.netty.metrics.BytesMetrics();
        for (BytesMetrics m : m_allMetrics) {
            allMetrics.incrementRead(m.readBytes());
            allMetrics.incrementWrote(m.wroteBytes());
        }
        return allMetrics;
    }
}
