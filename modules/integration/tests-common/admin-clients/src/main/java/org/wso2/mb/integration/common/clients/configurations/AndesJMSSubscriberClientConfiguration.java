package org.wso2.mb.integration.common.clients.configurations;

import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;

import java.util.UUID;

public class AndesJMSSubscriberClientConfiguration extends AndesJMSClientConfiguration {
    private final long unSubscribeAfterEachMessageCount;
    private final long rollbackAfterEachMessageCount;
    private final long commitAfterEachMessageCount;
    private final long acknowledgeAfterEachMessageCount;
    private final String filePathToWriteReceivedMessages;
    private final long maximumMessagesToReceived;
    private final String subscriptionID;
    private final boolean durable;
    private final int acknowledgeMode;
    private final boolean async;
    private final int subscriberCount;




    public static class AndesJMSSubscriberClientConfigurationBuilder
            extends AndesJMSClientConfiguration.AndesJMSClientConfigurationBuilder {
        ////optional
        // un-subscribe only after a certain message count
        private long unSubscribeAfterEachMessageCount = Long.MAX_VALUE;

        //role back only after a certain message count
        private long rollbackAfterEachMessageCount = Long.MAX_VALUE;

        // commit only after a certain message count
        private long commitAfterEachMessageCount = Long.MAX_VALUE;

        // acknowledge only after a certain message count
        private long acknowledgeAfterEachMessageCount = Long.MAX_VALUE;

        // file path to print received messages
        private String filePathToWriteReceivedMessages = null;

        // maximum number of message received
        private long maximumMessagesToReceived = Long.MAX_VALUE;

        //generating subscription ID
        private String subscriptionID = UUID.randomUUID().toString().replace("-", "");

        //for topics. If its queue, keep it as false
        private boolean durable = false;

        //session.AUTO_ACKNOWLEDGE
        private int acknowledgeMode = 1;

        //asynchronous message receive. Using MessageListener of JMS
        private boolean async = false;

        // number of subscribers
        private int subscriberCount = 1;

        public AndesJMSSubscriberClientConfigurationBuilder(String connectionString,
                                                            ExchangeType exchangeType,
                                                            String destinationName) {
            super(connectionString, exchangeType, destinationName);
        }

        public AndesJMSSubscriberClientConfigurationBuilder(String userName, String password,
                                                            String hostName, int port,
                                                            ExchangeType exchangeType,
                                                            String destinationName) {
            super(userName, password, hostName, port, exchangeType, destinationName);
        }

        public AndesJMSSubscriberClientConfigurationBuilder(AndesJMSClientConfiguration config) {
            super(config.getConnectionString(), config.getExchangeType(), config.getDestinationName());
        }

        public AndesJMSSubscriberClientConfigurationBuilder setUnSubscribeAfterEachMessageCount(
                long unSubscribeAfterEachMessageCount) {
            this.unSubscribeAfterEachMessageCount = unSubscribeAfterEachMessageCount;
            return this;
        }

        public AndesJMSSubscriberClientConfigurationBuilder setRollbackAfterEachMessageCount(
                long rollbackAfterEachMessageCount) {
            this.rollbackAfterEachMessageCount = rollbackAfterEachMessageCount;
            return this;
        }

        public AndesJMSSubscriberClientConfigurationBuilder setCommitAfterEachMessageCount(
                long commitAfterEachMessageCount) {
            this.commitAfterEachMessageCount = commitAfterEachMessageCount;
            return this;
        }

        public AndesJMSSubscriberClientConfigurationBuilder setAcknowledgeAfterEachMessageCount(
                long acknowledgeAfterEachMessageCount) {
            this.acknowledgeAfterEachMessageCount = acknowledgeAfterEachMessageCount;
            return this;
        }

        public AndesJMSSubscriberClientConfigurationBuilder setFilePathToWriteReceivedMessages(
                String filePathToWriteReceivedMessages) {
            this.filePathToWriteReceivedMessages = filePathToWriteReceivedMessages;
            return this;
        }

        public AndesJMSSubscriberClientConfigurationBuilder setMaximumMessagesToReceived(
                long maximumMessagesToReceived) {
            this.maximumMessagesToReceived = maximumMessagesToReceived;
            return this;
        }

        public AndesJMSSubscriberClientConfigurationBuilder setSubscriptionID(
                String subscriptionID) {
            this.subscriptionID = subscriptionID;
            return this;
        }

        public AndesJMSSubscriberClientConfigurationBuilder setDurable(boolean durable) {
            this.durable = durable;
            return this;
        }

        public AndesJMSSubscriberClientConfigurationBuilder setAcknowledgeMode(
                int acknowledgeMode) {
            this.acknowledgeMode = acknowledgeMode;
            return this;
        }

        public AndesJMSSubscriberClientConfigurationBuilder setAsync(boolean async) {
            this.async = async;
            return this;
        }

        public AndesJMSSubscriberClientConfigurationBuilder setSubscriberCount(
                int subscriberCount) {
            this.subscriberCount = subscriberCount;
            return this;
        }

        public AndesJMSSubscriberClientConfiguration build() {
            return new AndesJMSSubscriberClientConfiguration(this);
        }
    }

    public AndesJMSSubscriberClientConfiguration(
            AndesJMSSubscriberClientConfigurationBuilder builder) {
        super(builder);
        this.unSubscribeAfterEachMessageCount = builder.unSubscribeAfterEachMessageCount;
        this.rollbackAfterEachMessageCount = builder.rollbackAfterEachMessageCount;
        this.commitAfterEachMessageCount = builder.commitAfterEachMessageCount;
        this.acknowledgeAfterEachMessageCount = builder.acknowledgeAfterEachMessageCount;
        this.filePathToWriteReceivedMessages = builder.filePathToWriteReceivedMessages;
        this.maximumMessagesToReceived = builder.maximumMessagesToReceived;
        this.subscriptionID = builder.subscriptionID;
        this.acknowledgeMode = builder.acknowledgeMode;
        this.durable = builder.durable;
        this.async = builder.async;
        this.subscriberCount = builder.subscriberCount;
    }

    public long getUnSubscribeAfterEachMessageCount() {
        return unSubscribeAfterEachMessageCount;
    }

    public long getRollbackAfterEachMessageCount() {
        return rollbackAfterEachMessageCount;
    }

    public long getCommitAfterEachMessageCount() {
        return commitAfterEachMessageCount;
    }

    public long getAcknowledgeAfterEachMessageCount() {
        return acknowledgeAfterEachMessageCount;
    }

    public String getFilePathToWriteReceivedMessages() {
        return filePathToWriteReceivedMessages;
    }

    public long getMaximumMessagesToReceived() {
        return maximumMessagesToReceived;
    }

    public String getSubscriptionID() {
        return subscriptionID;
    }

    public boolean isDurable() {
        return durable;
    }

    public int getAcknowledgeMode() {
        return acknowledgeMode;
    }

    public boolean isAsync() {
        return async;
    }

    public int getSubscriberCount() {
        return subscriberCount;
    }
}
