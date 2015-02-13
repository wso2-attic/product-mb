package org.wso2.mb.integration.common.clients.configurations;

import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSMessageType;

import java.io.File;
import java.io.FileNotFoundException;

public class AndesJMSPublisherClientConfiguration extends AndesJMSClientConfiguration{
    private String readMessagesFromFilePath;
    private JMSMessageType jmsMessageType;
    private long numberOfMessagesToSend;
    private int publisherCount;
    private long jmsMessageExpiryTime;

    public AndesJMSPublisherClientConfiguration() {
        super();
    }

    public AndesJMSPublisherClientConfiguration(
            ExchangeType exchangeType, String destinationName) {
        super(exchangeType, destinationName);
    }

    public AndesJMSPublisherClientConfiguration(String connectionString,
                                                ExchangeType exchangeType,
                                                String destinationName) {
        super(connectionString, exchangeType, destinationName);
        this.initialize();
    }

    public AndesJMSPublisherClientConfiguration(String userName, String password,
                                                String hostName, int port,
                                                ExchangeType exchangeType,
                                                String destinationName) {
        super(userName, password, hostName, port, exchangeType, destinationName);
        this.initialize();
    }

    // TODO : implement
    public AndesJMSPublisherClientConfiguration(String xmlConfigFilePath) {
        super(xmlConfigFilePath);
    }

    public AndesJMSPublisherClientConfiguration(
            AndesJMSClientConfiguration config) {
        super(config);
        this.initialize();
    }

    @Override
    public void initialize() {
        super.initialize();
        readMessagesFromFilePath = null;
        jmsMessageType = JMSMessageType.TEXT;
        numberOfMessagesToSend = 10;
        publisherCount = 1;
        jmsMessageExpiryTime = 0L;
    }

    public String getReadMessagesFromFilePath() {
        return readMessagesFromFilePath;
    }

    public void setReadMessagesFromFilePath(String readMessagesFromFilePath)
            throws AndesClientException, FileNotFoundException {
        File messagesFilePath = new File(readMessagesFromFilePath);
        if (messagesFilePath.exists() && !messagesFilePath.isDirectory()) {
            this.readMessagesFromFilePath = readMessagesFromFilePath;
        }else{
            throw new FileNotFoundException("File is missing : " + messagesFilePath);
        }
    }

    public JMSMessageType getJMSMessageType() {
        return jmsMessageType;
    }

    public void setJMSMessageType(JMSMessageType jmsMessageType) {
        this.jmsMessageType = jmsMessageType;
    }

    public long getNumberOfMessagesToSend() {
        return numberOfMessagesToSend;
    }

    public void setNumberOfMessagesToSend(long numberOfMessagesToSend) throws AndesClientException {
        if (0 < numberOfMessagesToSend) {
            this.numberOfMessagesToSend = numberOfMessagesToSend;
        }else{
            throw new AndesClientException("The number of messages to send cannot be less than 1");
        }
    }

    public int getPublisherCount() {
        return publisherCount;
    }

    public void setPublisherCount(int publisherCount) throws AndesClientException {
        if (0 < publisherCount) {
            this.publisherCount = publisherCount;
        }else{
            throw new AndesClientException("The amount of publishers cannot be less than 1");
        }
    }

    public long getJMSMessageExpiryTime() {
        return jmsMessageExpiryTime;
    }

    public void setJMSMessageExpiryTime(long jmsMessageExpiryTime) throws AndesClientException {
        if (0 <= jmsMessageExpiryTime) {
            this.jmsMessageExpiryTime = jmsMessageExpiryTime;
        }else{
            throw new AndesClientException("Message expiry time cannot be less than 0");
        }
    }

    @Override
    public AndesJMSPublisherClientConfiguration clone() throws CloneNotSupportedException {
        return (AndesJMSPublisherClientConfiguration)super.clone();
    }
}
