package org.wso2.mb.integration.common.clients.configurations;


import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;

public class AndesJMSClientConfiguration implements Cloneable {
    private static final String TEMP_QUEUE_NAME = "temporaryClientQueue";
    private int port;
    private String hostName;
    private String userName;
    private String password;
    private String connectionString;
    private ExchangeType exchangeType;
    private String destinationName;
    private long printsPerMessageCount;
    private long runningDelay;
    private String filePathToWriteStatistics;

    public AndesJMSClientConfiguration() {
        this(ExchangeType.QUEUE, TEMP_QUEUE_NAME);
    }

    public AndesJMSClientConfiguration(ExchangeType exchangeType, String destinationName) {
        this("admin", "admin", "127.0.0.1", 5672, exchangeType, destinationName);
    }

    public AndesJMSClientConfiguration(String userName, String password, String hostName,
                                       int port, ExchangeType exchangeType,
                                       String destinationName) {
        this.exchangeType = exchangeType;
        this.destinationName = destinationName;
        this.userName = userName;
        this.password = password;
        this.hostName = hostName;
        this.port = port;
        this.createConnectionString();

        this.printsPerMessageCount = 1L;
        this.runningDelay = 0L;
    }

    // TODO : set username, etc
    public AndesJMSClientConfiguration(String connectionString,
                                              ExchangeType exchangeType,
                                              String destinationName) {
        this.connectionString = connectionString;
        this.exchangeType = exchangeType;
        this.destinationName = destinationName;
    }

    // TODO : implement
    public AndesJMSClientConfiguration(String xmlConfigFilePath){

    }

    public AndesJMSClientConfiguration(
            AndesJMSClientConfiguration config) {
        this.connectionString = config.getConnectionString();
        this.exchangeType = config.getExchangeType();
        this.destinationName = config.destinationName;
        this.printsPerMessageCount = config.getPrintsPerMessageCount();
        this.runningDelay = config.getRunningDelay();
    }

    private void createConnectionString() {
        this.connectionString = "amqp://" + this.userName + ":" + this.password + "@" + AndesClientConstants.CARBON_CLIENT_ID +
                                "/" + AndesClientConstants.CARBON_VIRTUAL_HOST_NAME + "?brokerlist='tcp://" +
                                this.hostName + ":" + this.port + "'";
    }

    public void setUserName(String userName) {
        this.userName = userName;
        this.createConnectionString();
    }

    public void setPassword(String password) {
        this.password = password;
        this.createConnectionString();
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
        this.createConnectionString();
    }

    public void setPort(int port) {
        this.port = port;
        this.createConnectionString();
    }

    public String getUserName() {
        return this.userName;
    }

    public String getPassword() {
        return this.password;
    }

    public String getHostName() {
        return this.hostName;
    }

    public int getPort() {
        return this.port;
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
        return this.printsPerMessageCount;
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

    public String getFilePathToWriteStatistics() {
        return filePathToWriteStatistics;
    }

    public void setFilePathToWriteStatistics(String filePathToPrintStatistics) {
        this.filePathToWriteStatistics = filePathToPrintStatistics;
    }

    @Override
    public String toString() {
        StringBuilder toStringVal = new StringBuilder();
        toStringVal.append("ConnectionString=").append(this.connectionString).append("\n");
        toStringVal.append("ExchangeType=").append(this.exchangeType).append("\n");
        toStringVal.append("PrintsPerMessageCount=").append(this.printsPerMessageCount).append("\n");
        toStringVal.append("RunningDelay=").append(this.runningDelay).append("\n");
        return toStringVal.toString();
    }
}
