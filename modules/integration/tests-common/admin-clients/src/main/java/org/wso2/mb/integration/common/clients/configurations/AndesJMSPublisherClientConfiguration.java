/*
*  Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.mb.integration.common.clients.configurations;

import org.wso2.mb.integration.common.clients.operations.utils.ClientConfigurationException;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSMessageHeader;
import org.wso2.mb.integration.common.clients.operations.utils.JMSMessageType;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * This class represents the Andes client publisher configuration. The class contains properties
 * related to JMS message publishing/sending.
 */
public class AndesJMSPublisherClientConfiguration extends AndesJMSClientConfiguration {
    /**
     * File path to read a string content which would be used to as message content when publishing.
     */
    private String readMessagesFromFilePath = null;

    /**
     * JMS message type. Text will be used as default message type.
     */
    private JMSMessageType jmsMessageType = JMSMessageType.TEXT;

    /**
     * Number of messages to be sent by the publisher.
     */
    private long numberOfMessagesToSend = 10L;

    /**
     * The message expiry time.
     */
    private long jmsMessageExpiryTime = 0L;

    /**
     * Message headers which would contain properties for the message to be sent. This would be
     * used to filter messages using selectors functionality.
     */
    private JMSMessageHeader messageHeader = null;

    /**
     * Creates a connection string with default properties.
     */
    public AndesJMSPublisherClientConfiguration() {
        super();
    }

    /**
     * Creates a publisher with a given exchange type and destination with default connection
     * string.
     *
     * @param exchangeType    The exchange type.
     * @param destinationName The destination name.
     */
    public AndesJMSPublisherClientConfiguration(
            ExchangeType exchangeType, String destinationName) {
        super(exchangeType, destinationName);
    }

    /**
     * Creates a consumer with a given host name, port for connection string and exchange type and
     * destination name.
     *
     * @param hostName        The host name for connection string.
     * @param port            The port for the connection string.
     * @param exchangeType    The exchange type.
     * @param destinationName The destination name.
     */
    public AndesJMSPublisherClientConfiguration(String hostName, int port,
                                                ExchangeType exchangeType,
                                                String destinationName) {
        super(hostName, port, exchangeType, destinationName);
    }

    /**
     * Creates a consumer with a given user name, password, host name, password for connection
     * string and exchange type and destination name.
     *
     * @param userName        The user name for the connection string.
     * @param password        The password for the connection string.
     * @param hostName        The host name for the connection string.
     * @param port            The port for the connection string.
     * @param exchangeType    The exchange type.
     * @param destinationName The destination name.
     */
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

    /**
     * Creates a consumer with a given AMQP transport connection string for SSL and a given
     * exchange type and destination.
     *
     * @param userName        The user name for the connection string.
     * @param password        The password for the connection string.
     * @param hostName        The host name for the connection string.
     * @param port            The port for the connection string.
     * @param exchangeType    The exchange type.
     * @param destinationName The destination name.
     * @param sslAlias
     * @param trustStorePath
     * @param trustStorePassword
     * @param keyStorePath
     * @param keyStorePassword
     */
    public AndesJMSPublisherClientConfiguration(String userName, String password, String hostName,
                                                int port,
                                                ExchangeType exchangeType, String destinationName,
                                                String sslAlias, String trustStorePath,
                                                String trustStorePassword, String keyStorePath,
                                                String keyStorePassword) {
        super(userName, password, hostName, port, exchangeType, destinationName, sslAlias,
              trustStorePath, trustStorePassword, keyStorePath, keyStorePassword);
    }

    /**
     * Gets the file path to read a string content which would be used to as message content when
     * publishing.
     *
     * @return The file path.
     */
    public String getReadMessagesFromFilePath() {
        return readMessagesFromFilePath;
    }

    /**
     * Sets the file path to read a string content which would be used to as message content when
     * publishing.
     *
     * @param readMessagesFromFilePath The file path.
     * @throws org.wso2.mb.integration.common.clients.operations.utils.ClientConfigurationException
     * @throws FileNotFoundException
     */
    public void setReadMessagesFromFilePath(String readMessagesFromFilePath)
            throws ClientConfigurationException, FileNotFoundException {
        File messagesFilePath = new File(readMessagesFromFilePath);
        if (messagesFilePath.exists() && !messagesFilePath.isDirectory()) {
            this.readMessagesFromFilePath = readMessagesFromFilePath;
        } else {
            throw new FileNotFoundException("File is missing : " + messagesFilePath);
        }
    }

    /**
     * Gets JMS message type.
     *
     * @return JMS message type.
     */
    public JMSMessageType getJMSMessageType() {
        return jmsMessageType;
    }

    /**
     * Sets JMS message type.
     *
     * @param jmsMessageType JMS message type
     */
    public void setJMSMessageType(JMSMessageType jmsMessageType) {
        this.jmsMessageType = jmsMessageType;
    }

    /**
     * Gets the number of messages to be sent by the publisher.
     *
     * @return The number of messages.
     */
    public long getNumberOfMessagesToSend() {
        return numberOfMessagesToSend;
    }

    /**
     * Sets the number of messages to be sent by the publisher
     *
     * @param numberOfMessagesToSend The number of messages.
     * @throws org.wso2.mb.integration.common.clients.operations.utils.ClientConfigurationException
     */
    public void setNumberOfMessagesToSend(long numberOfMessagesToSend)
            throws ClientConfigurationException {
        if (0 < numberOfMessagesToSend) {
            this.numberOfMessagesToSend = numberOfMessagesToSend;
        } else {
            throw new ClientConfigurationException("The number of messages to send cannot be less" +
                                                   " than 1");
        }
    }

    /**
     * Gets the messages expiry time.
     *
     * @return The message expiry time.
     */
    public long getJMSMessageExpiryTime() {
        return jmsMessageExpiryTime;
    }

    /**
     * Sets the message expiry time.
     *
     * @param jmsMessageExpiryTime The message expiry time.
     * @throws org.wso2.mb.integration.common.clients.operations.utils.ClientConfigurationException
     */
    public void setJMSMessageExpiryTime(long jmsMessageExpiryTime) throws
                                                                   ClientConfigurationException {
        if (0 <= jmsMessageExpiryTime) {
            this.jmsMessageExpiryTime = jmsMessageExpiryTime;
        } else {
            throw new ClientConfigurationException("Message expiry time cannot be less than 0");
        }
    }

    /**
     * Gets the message header which can be used by selectors.
     *
     * @return The message header properties.
     */
    public JMSMessageHeader getMessageHeader() {
        return messageHeader;
    }

    /**
     * Sets the message header which can be used by selectors.
     *
     * @param messageHeader The message header properties..
     */
    public void setMessageHeader(JMSMessageHeader messageHeader) {
        this.messageHeader = messageHeader;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return super.toString() +
               "ReadMessagesFromFilePath=" + this.readMessagesFromFilePath + "\n" +
               "JmsMessageType=" + this.jmsMessageType + "\n" +
               "NumberOfMessagesToSend=" + this.numberOfMessagesToSend + "\n" +
               "JmsMessageExpiryTime=" + this.jmsMessageExpiryTime + "\n";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesJMSPublisherClientConfiguration clone() throws CloneNotSupportedException {
        return (AndesJMSPublisherClientConfiguration) super.clone();
    }
}
