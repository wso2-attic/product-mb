package org.wso2.mb.integration.common.clients;

import org.apache.log4j.Logger;
import org.wso2.mb.integration.common.clients.configurations.AndesClientConfiguration;

import javax.jms.JMSException;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AndesClient{
    protected AndesClientConfiguration config;
    private static Logger log = Logger.getLogger(AndesClient.class);

    protected AtomicLong sentMessageCount;
    protected AtomicLong receivedMessageCount;
    protected AtomicLong firstMessagePublishTimestamp;
    protected AtomicLong lastMessagePublishTimestamp;
    protected AtomicLong firstMessageConsumedTimestamp;
    protected AtomicLong lastMessageConsumedTimestamp;
    /**
     * Total latency in milliseconds
     */
    protected AtomicLong totalLatency;

    protected AndesClient(AndesClientConfiguration config) throws NamingException {
        this.config = config;
        this.initialize();
    }

    protected void initialize() throws NamingException{
        log.info("Initializing Andes client");
        sentMessageCount = new AtomicLong();
        receivedMessageCount = new AtomicLong();
        firstMessagePublishTimestamp = new AtomicLong();
        lastMessagePublishTimestamp = new AtomicLong();
        firstMessageConsumedTimestamp = new AtomicLong();
        lastMessageConsumedTimestamp = new AtomicLong();
        totalLatency = new AtomicLong();
    }

    public abstract void startClient() throws JMSException, NamingException, IOException;

    public abstract void stopClient() throws JMSException;

    public double getPublisherTPS() {
        if (0 == this.lastMessagePublishTimestamp.get() - this.firstMessagePublishTimestamp.get()) {
            return this.sentMessageCount.doubleValue() / (1D / 1000);
        } else {
            return this.sentMessageCount.doubleValue() / ((this.lastMessagePublishTimestamp.doubleValue() - this.firstMessagePublishTimestamp.doubleValue()) / 1000);
        }
    }

    public double getSubscriberTPS() {
        if (0 == this.lastMessageConsumedTimestamp.get() - this.firstMessageConsumedTimestamp.get()) {
            return this.receivedMessageCount.doubleValue() / (1D / 1000);
        } else {
            return this.receivedMessageCount.doubleValue() / ((this.lastMessageConsumedTimestamp.doubleValue() - this.firstMessageConsumedTimestamp.doubleValue()) / 1000);
        }
    }

    public double getAverageLatency() {
        if (0 == this.receivedMessageCount.get()) {
            log.warn("No messages were received");
            return 0D;
        } else {
            return (this.totalLatency.doubleValue() / 1000) / this.receivedMessageCount.doubleValue();
        }
    }

    public long getSentMessageCount() {
        return sentMessageCount.get();
    }

    public long getReceivedMessageCount() {
        return receivedMessageCount.get();
    }

    public AndesClientConfiguration getConfig(){
        return config;
    }
}
