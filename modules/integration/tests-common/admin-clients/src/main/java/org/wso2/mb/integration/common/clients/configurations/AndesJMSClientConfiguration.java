package org.wso2.mb.integration.common.clients.configurations;


import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;

public class AndesJMSClientConfiguration extends AndesClientConfiguration {
    private String connectionString;
    private ExchangeType exchangeType;
    private String destinationName;
    private long printsPerMessageCount;
    private long runningDelay;

    public AndesJMSClientConfiguration(String connectionString,
                                              ExchangeType exchangeType,
                                              String destinationName) {
        this.connectionString = connectionString;
        this.exchangeType = exchangeType;
        this.destinationName = destinationName;
        this.initialize();
    }

    public AndesJMSClientConfiguration(String userName, String password, String hostName,
                                              int port, ExchangeType exchangeType,
                                              String destinationName) {
        this.connectionString = "amqp://" + userName + ":" + password + "@" + AndesClientConstants.CARBON_CLIENT_ID + "/" +
                                AndesClientConstants.CARBON_VIRTUAL_HOST_NAME + "?brokerlist='tcp://" + hostName + ":" + port + "'";
        this.exchangeType = exchangeType;
        this.destinationName = destinationName;
        this.initialize();
    }

    @Override
    public void initialize() {
        super.initialize();
        printsPerMessageCount = -1L;
        runningDelay = 0L;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public ExchangeType getExchangeType() {
        return exchangeType;
    }

    public void setExchangeType(ExchangeType exchangeType) {
        this.exchangeType = exchangeType;
    }

    public String getDestinationName() {
        return destinationName;
    }

    public void setDestinationName(String destinationName) {
        this.destinationName = destinationName;
    }

    public long getPrintsPerMessageCount() {
        return printsPerMessageCount;
    }

    public void setPrintsPerMessageCount(long printsPerMessageCount) throws AndesClientException {
        if (0 < printsPerMessageCount) {
            this.printsPerMessageCount = printsPerMessageCount;
        } else {
            throw new AndesClientException("Prints per message count cannot be less than one");
        }
    }

    public long getRunningDelay() {
        return runningDelay;
    }

    public void setRunningDelay(long runningDelay) throws AndesClientException {
        if (0 <= runningDelay) {
            this.runningDelay = runningDelay;
        } else {
            throw new AndesClientException("Running delay cannot be less than 0");
        }
    }
}
