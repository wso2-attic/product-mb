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

import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;

/**
 * This class acts as a configuration class for the Andes Client. The class contains configuration
 * that is common for both JMS publishers and consumer. The configuration mentioned are related to
 * JMS only.
 */
public class AndesJMSClientConfiguration implements Cloneable {
    /**
     * The destination name to be used when a configuration is not passed to the client.
     */
    private static final String TEMP_DESTINATION_NAME = "temporaryDestination";

    /**
     * The AMQP transport port.
     */
    private int port;

    /**
     * The host name for the AMQP transport.
     */
    private String hostName;

    /**
     * The user name used when creating the AMQP connection string. Publishing/Consuming of JMS will
     * be done under this username.
     */
    private String userName;

    /**
     * The password used when creating the AMQP connection string.
     */
    private String password;

    /**
     * The connection string used for publishing/consuming jms messages
     */
    private String connectionString;

    /**
     * The exchange type used to describe the destination type
     */
    private ExchangeType exchangeType;

    /**
     * The destination name for which messages are published/consumed
     */
    private String destinationName;

    /**
     * The amount of console logging for a certain message count
     */
    private long printsPerMessageCount;

    /**
     * A delay value which can be used to delay of publishing/consuming jms messages
     */
    private long runningDelay;

    /**
     * The file path to write statistics. Statistics include TPS, latency, etc.
     */
    private String filePathToWriteStatistics;

    /**
     * XML file path for configuration
     */
    private String xmlConfigFilePath;

    /**
     * The empty constructor which will create a queue related test case.
     */
    public AndesJMSClientConfiguration() {
        this(ExchangeType.QUEUE, TEMP_DESTINATION_NAME);
    }

    /**
     * Creates a connection string with the default username, password, hostname, port. Also sets
     * the exchange type and destination name for publishing/consuming jms messages.
     *
     * @param exchangeType    The exchange type.
     * @param destinationName The destination name.
     */
    public AndesJMSClientConfiguration(ExchangeType exchangeType, String destinationName) {
        this(AndesClientConstants.DEFAULT_USERNAME, AndesClientConstants.DEFAULT_PASSWORD,
             AndesClientConstants.DEFAULT_HOST_NAME, AndesClientConstants.DEFAULT_PORT,
             exchangeType, destinationName);
    }

    /**
     * Creates a connection string with default username, password and a given host name, port.
     * Also sets exchangeType and destination name used for publishing/consuming jms messages.
     *
     * @param hostName        The host name used in the AMQP transport connection string.
     * @param port            The port used in the AMQP transport connection string.
     * @param exchangeType    The exchange type used for publishing/consuming jms messages.
     * @param destinationName The destination name used for publishing/consuming jms messages.
     */
    public AndesJMSClientConfiguration(String hostName, int port, ExchangeType exchangeType,
                                       String destinationName) {
        this(AndesClientConstants.DEFAULT_USERNAME, AndesClientConstants.DEFAULT_PASSWORD, hostName,
             port, exchangeType, destinationName);
    }

    /**
     * Creates a connection string with a given username, password, hostname, port, exchange type
     * and destination name.
     *
     * @param userName        The username to be used in creating the connection string.
     * @param password        The password to be used in creating the connection string.
     * @param hostName        The host name to be used in creating the connection string.
     * @param port            The AMQP transport port used in creating the connection string
     * @param exchangeType    The exchange type.
     * @param destinationName The destination name.
     */
    public AndesJMSClientConfiguration(String userName, String password, String hostName,
                                       int port, ExchangeType exchangeType,
                                       String destinationName) {
        // Setting values for exchange type and destination name
        this.exchangeType = exchangeType;
        this.destinationName = destinationName;

        // Creating connection string
        this.userName = userName;
        this.password = password;
        this.hostName = hostName;
        this.port = port;
        this.createConnectionString();

        // Setting default values
        this.printsPerMessageCount = 1L;
        this.runningDelay = 0L;
    }

    public AndesJMSClientConfiguration(String connectionString,
                                       ExchangeType exchangeType,
                                       String destinationName) {
        this.connectionString = connectionString;
        this.exchangeType = exchangeType;
        this.destinationName = destinationName;
    }

    public AndesJMSClientConfiguration(String xmlConfigFilePath) {
        this.xmlConfigFilePath = xmlConfigFilePath;
    }

    public AndesJMSClientConfiguration(
            AndesJMSClientConfiguration config) {
        this.connectionString = config.getConnectionString();
        this.exchangeType = config.getExchangeType();
        this.destinationName = config.destinationName;
        this.printsPerMessageCount = config.getPrintsPerMessageCount();
        this.runningDelay = config.getRunningDelay();
    }

    /**
     * Creates an AMQP connection string.
     */
    private void createConnectionString() {
        this.connectionString = "amqp://" + this.userName + ":" + this.password + "@" +
                                AndesClientConstants.CARBON_CLIENT_ID + "/" +
                                AndesClientConstants.CARBON_VIRTUAL_HOST_NAME +
                                "?brokerlist='tcp://" +
                                this.hostName + ":" + this.port + "'";
    }

    /**
     * Sets user name for the AMQP connection string
     *
     * @param userName The user name.
     */
    public void setUserName(String userName) {
        this.userName = userName;
        this.createConnectionString();
    }

    /**
     * Sets password for the AMQP connection string
     *
     * @param password The password
     */
    public void setPassword(String password) {
        this.password = password;
        this.createConnectionString();
    }

    /**
     * Sets host name for the AMQP connection string.
     *
     * @param hostName The host name
     */
    public void setHostName(String hostName) {
        this.hostName = hostName;
        this.createConnectionString();
    }

    /**
     * Sets the AMQP connection string port.
     *
     * @param port the port.
     */
    public void setPort(int port) {
        this.port = port;
        this.createConnectionString();
    }

    /**
     * Gets the user name used in the AMQP connection string.
     *
     * @return The user name.
     */
    public String getUserName() {
        return this.userName;
    }

    /**
     * Gets the password used in the AMQP connection string.
     *
     * @return The password.
     */
    public String getPassword() {
        return this.password;
    }

    /**
     * Gets the host name used in the AMQP connection string.
     *
     * @return The host name.
     */
    public String getHostName() {
        return this.hostName;
    }

    /**
     * Gets the port used in the AMQP connection string.
     *
     * @return The port name.
     */
    public int getPort() {
        return this.port;
    }

    /**
     * Gets the AMQP connection string.
     *
     * @return The connection string.
     */
    public String getConnectionString() {
        return connectionString;
    }

    /**
     * Sets the AMQP transport connection string.
     *
     * @param connectionString The connection string.
     */
    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    /**
     * Gets the exchange type used for publishing/consuming jms messages.
     *
     * @return The exchange type
     */
    public ExchangeType getExchangeType() {
        return exchangeType;
    }

    /**
     * Sets the exchange type used for publishing/consuming jms messages
     *
     * @param exchangeType The exchange type
     */
    public void setExchangeType(ExchangeType exchangeType) {
        this.exchangeType = exchangeType;
    }

    /**
     * Gets the destination name for publishing/consuming jms messages.
     *
     * @return The destination name.
     */
    public String getDestinationName() {
        return destinationName;
    }

    /**
     * Sets the destination name for publishing/consuming jms messages.
     *
     * @param destinationName The destination name
     */
    public void setDestinationName(String destinationName) {
        this.destinationName = destinationName;
    }

    /**
     * Gets the number of console logging per message count
     *
     * @return The number of console logging per message count
     */
    public long getPrintsPerMessageCount() {
        return this.printsPerMessageCount;
    }

    /**
     * Sets the number of console logging per message count
     *
     * @param printsPerMessageCount The number of console logging per message count
     * @throws AndesClientException
     */
    public void setPrintsPerMessageCount(long printsPerMessageCount) throws AndesClientException {
        if (0 < printsPerMessageCount) {
            this.printsPerMessageCount = printsPerMessageCount;
        } else {
            throw new AndesClientException("Prints per message count cannot be less than one");
        }
    }

    /**
     * Gets the delay used in publishing/consuming messages in milliseconds.
     *
     * @return the delay used in publishing/consuming messages in milliseconds.
     */
    public long getRunningDelay() {
        return runningDelay;
    }

    /**
     * Sets the delay used in publishing/consuming messages in milliseconds.
     *
     * @param runningDelay The delay used in publishing/consuming messages in milliseconds.
     * @throws AndesClientException
     */
    public void setRunningDelay(long runningDelay) throws AndesClientException {
        if (0 <= runningDelay) {
            this.runningDelay = runningDelay;
        } else {
            throw new AndesClientException("Running delay cannot be less than 0");
        }
    }

    /**
     * Gets the file path to write statistics such as TPS, Average latency against Timestamp.
     *
     * @return The file path used for writing statistics.
     */
    public String getFilePathToWriteStatistics() {
        return filePathToWriteStatistics;
    }

    /**
     * Sets the file path to write statistics such as TPS, Average latency against Timestamp
     *
     * @param filePathToPrintStatistics The file path used for writing statistics.
     */
    public void setFilePathToWriteStatistics(String filePathToPrintStatistics) {
        this.filePathToWriteStatistics = filePathToPrintStatistics;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "ConnectionString=" + this.connectionString + "\n" + "ExchangeType=" +
               this.exchangeType + "\n" + "PrintsPerMessageCount=" + this.printsPerMessageCount +
               "\n" + "RunningDelay=" + this.runningDelay + "\n";
    }
}
