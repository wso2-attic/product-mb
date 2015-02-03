package org.wso2.mb.integration.common.clients.configurations;

import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSMessageType;

public class AndesJMSPublisherClientConfiguration extends AndesJMSClientConfiguration {
    private final String readMessagesFromFilePath;
    private final JMSMessageType jmsMessageType;
    private final long messageLifetime;
    private final long numberOfMessagesToSend;
    private final int publisherCount;
    private final long JMSMessageExpiryTime;

    public static class AndesJMSPublisherClientConfigurationBuilder
            extends AndesJMSClientConfiguration.AndesJMSClientConfigurationBuilder {

        //optional
        private String readMessagesFromFilePath = null;
        private JMSMessageType jmsMessageType = JMSMessageType.TEXT;
        private long messageLifetime = 0L;
        private long numberOfMessagesToSend = 10;
        private int publisherCount = 1;
        private long jmsMessageExpiryTime = 0L;

        public AndesJMSPublisherClientConfigurationBuilder(String connectionString,
                                                           ExchangeType exchangeType,
                                                           String destinationName) {
            super(connectionString, exchangeType, destinationName);
        }

        public AndesJMSPublisherClientConfigurationBuilder(String userName, String password,
                                                           String hostName, int port,
                                                           ExchangeType exchangeType,
                                                           String destinationName) {
            super(userName, password, hostName, port, exchangeType, destinationName);
        }

        public AndesJMSPublisherClientConfigurationBuilder(AndesJMSClientConfiguration config) {
            super(config.getConnectionString(), config.getExchangeType(), config.getDestinationName());
        }

        public AndesJMSPublisherClientConfigurationBuilder setReadMessagesFromFilePath(
                String readMessagesFromFilePath) {
            this.readMessagesFromFilePath = readMessagesFromFilePath;
            return this;
        }


        public AndesJMSPublisherClientConfigurationBuilder setJmsMessageType(
                JMSMessageType jmsMessageType) {
            this.jmsMessageType = jmsMessageType;
            return this;
        }

        public AndesJMSPublisherClientConfigurationBuilder setMessageLifetime(
                long messageLifetime) {
            this.messageLifetime = messageLifetime;
            return this;
        }

        public AndesJMSPublisherClientConfigurationBuilder setNumberOfMessagesToSend(
                long numberOfMessagesToSend) {
            this.numberOfMessagesToSend = numberOfMessagesToSend;
            return this;
        }

        public AndesJMSPublisherClientConfigurationBuilder setPublisherCount(int publisherCount) {
            this.publisherCount = publisherCount;
            return this;
        }

        public AndesJMSPublisherClientConfigurationBuilder setJmsMessageExpiryTime(long jmsMessageExpiryTime) {
            this.jmsMessageExpiryTime = jmsMessageExpiryTime;
            return this;
        }

        public AndesJMSPublisherClientConfiguration build() {
            return new AndesJMSPublisherClientConfiguration(this);
        }
    }

    private AndesJMSPublisherClientConfiguration(
            AndesJMSPublisherClientConfigurationBuilder builder) {
        super(builder);
        this.readMessagesFromFilePath = builder.readMessagesFromFilePath;
        this.jmsMessageType = builder.jmsMessageType;
        this.messageLifetime = builder.messageLifetime;
        this.numberOfMessagesToSend = builder.numberOfMessagesToSend;
        this.publisherCount = builder.publisherCount;
        this.JMSMessageExpiryTime = builder.jmsMessageExpiryTime;
    }

    public String getReadMessagesFromFilePath() {
        return this.readMessagesFromFilePath;
    }

    public JMSMessageType getJMSMessageType() {
        return this.jmsMessageType;
    }

    public long getMessageLifetime() {
        return this.messageLifetime;
    }

    public long getNumberOfMessagesToSend() {
        return this.numberOfMessagesToSend;
    }

    public int getPublisherCount() {
        return publisherCount;
    }

    public long getJMSMessageExpiryTime() {
        return JMSMessageExpiryTime;
    }
}
