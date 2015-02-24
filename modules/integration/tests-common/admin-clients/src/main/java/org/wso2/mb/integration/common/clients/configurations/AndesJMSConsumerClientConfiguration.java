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

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.wso2.mb.integration.common.clients.operations.utils.ClientConfigurationException;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSAcknowledgeMode;

/**
 * This class represents the Andes client consumer configuration. The class contains properties
 * related to JMS message consuming.
 */
public class AndesJMSConsumerClientConfiguration extends AndesJMSClientConfiguration {
    /**
     * The logger used in logging information, warnings, errors and etc.
     */
    private static Logger log = Logger.getLogger(AndesJMSConsumerClientConfiguration.class);

    /**
     * Message count at which the consumer un-subscribes.
     */
    private long unSubscribeAfterEachMessageCount = Long.MAX_VALUE;

    /**
     * Message count at which the session is rolled back.
     */
    private long rollbackAfterEachMessageCount = Long.MAX_VALUE;

    /**
     * Message count at which the session is committed.
     */
    private long commitAfterEachMessageCount = Long.MAX_VALUE;

    /**
     * Message count at which a message is acknowledge.
     */
    private long acknowledgeAfterEachMessageCount = Long.MAX_VALUE;

    /**
     * The file path to write received messages.
     */
    private String filePathToWriteReceivedMessages = null;

    /**
     * Maximum messages to receiver.
     */
    private long maximumMessagesToReceived = Long.MAX_VALUE;

    /**
     * Subscription ID for durable topics.
     */
    private String subscriptionID = null;

    /**
     * Whether the subscriber is durable.
     */
    private boolean durable = false;

    /**
     * The acknowledge mode for messages.
     */
    private JMSAcknowledgeMode acknowledgeMode = JMSAcknowledgeMode.AUTO_ACKNOWLEDGE;

    /**
     * Whether the consumer is asynchronously reading messages. Asynchronous message reading implies
     * that it uses {@link javax.jms.MessageListener} to listen to receiving messages. Synchronous
     * message reading will use a while loop inside a thread.
     */
    private boolean async = true;

    /**
     * JMS selectors string for filtering.
     */
    private String selectors = null;

    /**
     * Creates a consumer configuration with default values.
     */
    public AndesJMSConsumerClientConfiguration() {
        super();
    }

    /**
     * Creates a consumer with a given exchange type and destination with default connection string.
     *
     * @param exchangeType    The exchange type.
     * @param destinationName The destination name.
     */
    public AndesJMSConsumerClientConfiguration(
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
    public AndesJMSConsumerClientConfiguration(String hostName, int port,
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
    public AndesJMSConsumerClientConfiguration(String userName, String password,
                                               String hostName, int port,
                                               ExchangeType exchangeType,
                                               String destinationName) {
        super(userName, password, hostName, port, exchangeType, destinationName);
    }

    // TODO : implement
    public AndesJMSConsumerClientConfiguration(String xmlConfigFilePath) {
        super(xmlConfigFilePath);
    }

    public AndesJMSConsumerClientConfiguration(
            AndesJMSClientConfiguration config) {
        super(config);
    }

    public AndesJMSConsumerClientConfiguration(String userName, String password, String hostName,
                                               int port,
                                               ExchangeType exchangeType, String destinationName,
                                               String sslAlias, String trustStorePath,
                                               String trustStorePassword, String keyStorePath,
                                               String keyStorePassword) {
        super(userName, password, hostName, port, exchangeType, destinationName, sslAlias,
              trustStorePath, trustStorePassword, keyStorePath, keyStorePassword);

    }

    /**
     * Gets the message count to un-subscribe the consumer.
     *
     * @return The message count to un-subscribe the consumer.
     */
    public long getUnSubscribeAfterEachMessageCount() {
        return unSubscribeAfterEachMessageCount;
    }

    /**
     * Sets message count to un-subscribe the consumer.
     *
     * @param unSubscribeAfterEachMessageCount The message count to un-subscribe the consumer.
     * @throws org.wso2.mb.integration.common.clients.operations.utils.ClientConfigurationException
     */
    public void setUnSubscribeAfterEachMessageCount(long unSubscribeAfterEachMessageCount)
            throws ClientConfigurationException {
        if (0 < unSubscribeAfterEachMessageCount) {
            this.unSubscribeAfterEachMessageCount = unSubscribeAfterEachMessageCount;
        } else {
            throw new ClientConfigurationException("Value cannot be less than 0");
        }
    }

    /**
     * Gets the message count at which the session should be rolled-back.
     *
     * @return The message count at which the session should be rolled-back.
     */
    public long getRollbackAfterEachMessageCount() {
        return rollbackAfterEachMessageCount;
    }

    /**
     * Sets message count at which the session should be rolled-back.
     *
     * @param rollbackAfterEachMessageCount The message count at which the session should be
     *                                      rolled-back.
     * @throws org.wso2.mb.integration.common.clients.operations.utils.ClientConfigurationException
     */
    public void setRollbackAfterEachMessageCount(long rollbackAfterEachMessageCount)
            throws ClientConfigurationException {
        if (0 < rollbackAfterEachMessageCount) {
            this.rollbackAfterEachMessageCount = rollbackAfterEachMessageCount;
        } else {
            throw new ClientConfigurationException("Value cannot be less than 0");
        }
    }

    /**
     * Gets the message count at which the session should be committed.
     *
     * @return The message count at which the session should be committed.
     */
    public long getCommitAfterEachMessageCount() {
        return commitAfterEachMessageCount;
    }

    /**
     * Sets the message count at which the session should be committed.
     *
     * @param commitAfterEachMessageCount The message count at which the session should be
     *                                    committed.
     * @throws org.wso2.mb.integration.common.clients.operations.utils.ClientConfigurationException
     */
    public void setCommitAfterEachMessageCount(long commitAfterEachMessageCount)
            throws ClientConfigurationException {
        if (0 < commitAfterEachMessageCount) {
            this.commitAfterEachMessageCount = commitAfterEachMessageCount;
        } else {
            throw new ClientConfigurationException("Value cannot be less than 0");
        }
    }

    /**
     * Gets the message count at which a message should be acknowledged after.
     *
     * @return The the message count at which a message should be acknowledged after.
     */
    public long getAcknowledgeAfterEachMessageCount() {
        return acknowledgeAfterEachMessageCount;
    }

    /**
     * Sets the message count at which a message should be acknowledged after.
     *
     * @param acknowledgeAfterEachMessageCount The the message count at which a message should be
     *                                         acknowledged after.
     * @throws org.wso2.mb.integration.common.clients.operations.utils.ClientConfigurationException
     */
    public void setAcknowledgeAfterEachMessageCount(long acknowledgeAfterEachMessageCount)
            throws ClientConfigurationException {
        if (0 < acknowledgeAfterEachMessageCount) {
            this.acknowledgeAfterEachMessageCount = acknowledgeAfterEachMessageCount;
        } else {
            throw new ClientConfigurationException("Value cannot be less than 0");
        }
    }

    /**
     * Gets the file path where the received messages should be written to,
     *
     * @return The file path where the received messages should be written to,
     */
    public String getFilePathToWriteReceivedMessages() {
        return filePathToWriteReceivedMessages;
    }

    /**
     * Sets the file path where the received messages should be written to,
     *
     * @param filePathToWriteReceivedMessages The file path where the received messages should be
     *                                        written to,
     */
    public void setFilePathToWriteReceivedMessages(String filePathToWriteReceivedMessages) {
        this.filePathToWriteReceivedMessages = filePathToWriteReceivedMessages;
    }

    /**
     * Gets the maximum number of messages to received.
     *
     * @return The maximum number of messages to received.
     */
    public long getMaximumMessagesToReceived() {
        return this.maximumMessagesToReceived;
    }

    /**
     * Sets the maximum number of messages to received.
     *
     * @param maximumMessagesToReceived The maximum number of messages to received.
     * @throws org.wso2.mb.integration.common.clients.operations.utils.ClientConfigurationException
     */
    public void setMaximumMessagesToReceived(long maximumMessagesToReceived)
            throws ClientConfigurationException {
        if (0 < maximumMessagesToReceived) {
            this.maximumMessagesToReceived = maximumMessagesToReceived;
        } else {
            throw new ClientConfigurationException("The maximum number of messages to receive " +
                                                   "cannot be less than 1");
        }
    }

    /**
     * Gets the subscription ID.
     *
     * @return The subscription ID.
     */
    public String getSubscriptionID() {
        return subscriptionID;
    }

    /**
     * Sets the subscription ID
     *
     * @param subscriptionID The subscription ID
     * @throws org.wso2.mb.integration.common.clients.operations.utils.ClientConfigurationException
     */
    public void setSubscriptionID(String subscriptionID) throws ClientConfigurationException {
        if (this.durable) {
            if (StringUtils.isNotEmpty(subscriptionID)) {
                this.subscriptionID = subscriptionID;
            } else {
                throw new ClientConfigurationException("Subscription ID cannot be null or empty " +
                                                       "for an durable topic");
            }
        } else {
            this.subscriptionID = subscriptionID;
            log.warn("Setting subscription ID for non-durable topics. Subscription ID is not " +
                     "necessary for non-durable topics or queues");
        }
    }

    /**
     * Checks whether the subscriber/consumer is durable.
     *
     * @return true if subscriber/consumer is durable, false otherwise.
     */
    public boolean isDurable() {
        return durable;
    }

    /**
     * Sets values for a durable subscription
     *
     * @param durable        True if subscription is durable, false otherwise.
     * @param subscriptionID The subscription ID.
     * @throws org.wso2.mb.integration.common.clients.operations.utils.ClientConfigurationException
     */
    public void setDurable(boolean durable, String subscriptionID) throws
                                                                   ClientConfigurationException {
        if (durable) {
            if (StringUtils.isNotEmpty(subscriptionID)) {
                this.subscriptionID = subscriptionID;
            } else {
                throw new ClientConfigurationException("Subscription ID cannot be null or empty " +
                                                       "for an durable topic");
            }
        }

        this.durable = durable;
    }

    /**
     * Gets acknowledge mode for messages.
     *
     * @return The acknowledge mode for messages.
     */
    public JMSAcknowledgeMode getAcknowledgeMode() {
        return acknowledgeMode;
    }

    /**
     * Sets acknowledge mode for messages.
     *
     * @param acknowledgeMode The acknowledge mode for messages.
     * @throws org.wso2.mb.integration.common.clients.operations.utils.ClientConfigurationException
     */
    public void setAcknowledgeMode(JMSAcknowledgeMode acknowledgeMode)
            throws ClientConfigurationException {
        this.acknowledgeMode = acknowledgeMode;
    }

    /**
     * Checks whether consumer is asynchronously reading messages.
     *
     * @return true if messages are read asynchronously, false otherwise. Asynchronously message
     * reading implies that it uses {@link javax.jms.MessageListener} to listen to receiving
     * messages.
     */
    public boolean isAsync() {
        return async;
    }

    /**
     * Sets the consumer to read message asynchronously. Asynchronously message
     * reading implies that it uses {@link javax.jms.MessageListener} to listen to receiving
     * messages.
     *
     * @param async true if messages should be read asynchronously, false otherwise.
     */
    public void setAsync(boolean async) {
        this.async = async;
    }

    /**
     * Gets the selectors query used by the consumer for filtering.
     *
     * @return The selectors query used by the consumer for filtering.
     */
    public String getSelectors() {
        return selectors;
    }

    /**
     * Sets the selectors query used by the consumer for filtering.
     *
     * @param selectors The selectors query used by the consumer for filtering.
     */
    public void setSelectors(String selectors) {
        this.selectors = selectors;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return super.toString() +
               "UnSubscribeAfterEachMessageCount=" + this.unSubscribeAfterEachMessageCount + "\n" +
               "RollbackAfterEachMessageCount=" + this.rollbackAfterEachMessageCount + "\n" +
               "CommitAfterEachMessageCount=" + this.commitAfterEachMessageCount + "\n" +
               "AcknowledgeAfterEachMessageCount=" + this.acknowledgeAfterEachMessageCount + "\n" +
               "FilePathToWriteReceivedMessages=" + this.filePathToWriteReceivedMessages + "\n" +
               "MaximumMessagesToReceived=" + this.maximumMessagesToReceived + "\n" +
               "SubscriptionID=" + this.subscriptionID + "\n" +
               "Durable=" + this.durable + "\n" +
               "AcknowledgeMode=" + this.acknowledgeMode + "\n" +
               "Async=" + this.async + "\n" +
               "Selectors=" + this.selectors + "\n";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesJMSConsumerClientConfiguration clone() throws CloneNotSupportedException {
        return (AndesJMSConsumerClientConfiguration) super.clone();
    }
}
