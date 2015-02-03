package org.wso2.mb.integration.common.clients.configurations;


import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;

public class AndesJMSClientConfiguration extends AndesClientConfiguration {
    private static final String CARBON_CLIENT_ID = "carbon";
    private static final String CARBON_VIRTUAL_HOST_NAME = "carbon";

    private final String connectionString;
    private final ExchangeType exchangeType;
    private final String destinationName;
    private final long printsPerMessageCount;
    private final long runningDelay;

    public static class AndesJMSClientConfigurationBuilder
            extends AndesClientConfiguration.AndesClientConfigurationBuilder {

        //required
        private final String connectionString;
        private final ExchangeType exchangeType;
        private final String destinationName;

        //optional
        private long printsPerMessageCount = -1;
        private long runningDelay = 0;

        public AndesJMSClientConfigurationBuilder(String connectionString,
                                                  ExchangeType exchangeType,
                                                  String destinationName) {
            this.connectionString = connectionString;
            this.exchangeType = exchangeType;
            this.destinationName = destinationName;
        }

        public AndesJMSClientConfigurationBuilder(String userName, String password, String hostName,
                                                  int port, ExchangeType exchangeType,
                                                  String destinationName) {
            this.connectionString = "amqp://" + userName + ":" + password + "@" + CARBON_CLIENT_ID + "/" +
                                    CARBON_VIRTUAL_HOST_NAME + "?brokerlist='tcp://" + hostName + ":" + port + "'";
            this.exchangeType = exchangeType;
            this.destinationName = destinationName;
        }

        public AndesJMSClientConfigurationBuilder setRunningDelay(long runningDelay) {
            this.runningDelay = runningDelay;
            return this;
        }

        public AndesJMSClientConfigurationBuilder setPrintsPerMessageCount(
                long printsPerMessageCount) {
            this.printsPerMessageCount = printsPerMessageCount;
            return this;
        }

        public AndesJMSClientConfiguration build() {
            return new AndesJMSClientConfiguration(this);
        }
    }

    public AndesJMSClientConfiguration(AndesJMSClientConfigurationBuilder builder) {
        super(builder);
        this.connectionString = builder.connectionString;
        this.exchangeType = builder.exchangeType;
        this.destinationName = builder.destinationName;
        this.printsPerMessageCount = builder.printsPerMessageCount;
        this.runningDelay = builder.runningDelay;
    }

    public String getConnectionString() {
        return this.connectionString;
    }

    public ExchangeType getExchangeType() {
        return exchangeType;
    }

    public String getDestinationName() {
        return destinationName;
    }

    public long getPrintsPerMessageCount() {
        return printsPerMessageCount;
    }

    public long getRunningDelay() {
        return runningDelay;
    }
}
