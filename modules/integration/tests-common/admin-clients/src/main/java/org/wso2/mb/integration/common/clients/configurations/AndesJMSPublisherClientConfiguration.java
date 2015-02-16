package org.wso2.mb.integration.common.clients.configurations;

import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSMessageHeader;
import org.wso2.mb.integration.common.clients.operations.utils.JMSMessageType;

import javax.jms.Message;
import java.io.File;
import java.io.FileNotFoundException;

public class AndesJMSPublisherClientConfiguration extends AndesJMSClientConfiguration{
    private String readMessagesFromFilePath = null;
    private JMSMessageType jmsMessageType = JMSMessageType.TEXT;
    private long numberOfMessagesToSend = 10L;
    private long jmsMessageExpiryTime = 0L;
    private JMSMessageHeader messageHeader = null;

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
    }

    public AndesJMSPublisherClientConfiguration(String hostName, int port,
                                                ExchangeType exchangeType,
                                                String destinationName) {
        super(hostName, port, exchangeType, destinationName);
    }

    public AndesJMSPublisherClientConfiguration(String userName, String password,
                                                String hostName, int port,
                                                ExchangeType exchangeType,
                                                String destinationName) {
        super(userName, password, hostName, port, exchangeType, destinationName);
    }

    // TODO : implement
    public AndesJMSPublisherClientConfiguration(String xmlConfigFilePath) {
        super(xmlConfigFilePath);
    }

    public AndesJMSPublisherClientConfiguration(
            AndesJMSClientConfiguration config) {
        super(config);
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

    public JMSMessageHeader getMessageHeader() {
        return messageHeader;
    }

    public void setMessageHeader(JMSMessageHeader messageHeader) {
        this.messageHeader = messageHeader;
    }

    @Override
    public AndesJMSPublisherClientConfiguration clone() throws CloneNotSupportedException {
        return (AndesJMSPublisherClientConfiguration)super.clone();
    }
}
