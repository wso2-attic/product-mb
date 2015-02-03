package org.wso2.mb.integration.common.clients.configurations;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSMessageType;

/**
 * The following class contains properties related to the andes client. Properties common to AMQP, MQTT, etc should be kept here.
 * Properties common to AMQP publisher sdfsd
 */
public class AndesClientConfiguration {
    private static Logger log = Logger.getLogger(AndesClientConfiguration.class);

    public AndesClientConfiguration(
            AndesClientConfigurationBuilder andesClientConfigurationBuilder) {
        //assign values
    }

//    protected final String destinationName;
//    protected  final String connectionString;
//    protected  final long runningDelay;
//    protected  final long printsPerMessageCount;
//    private final ExchangeType exchangeType;
//    private final String readMessagesFromFile;
//    private final JMSMessageType jmsMessageType;
//    private final long messageLifetime;
//    private final long numberOfMessagesToSend;
//    private final long unSubscribeAfterEachMessageCount;
//    private final long rollbackAfterEachMessageCount;
//    private final long commitAfterEachMessageCount;
//    private final long acknowledgeAfterEachMessageCount;
//    private final String filePathToWriteReceivedMessages;
//    private final long maximumMessagesToReceived;
//    private final String subscriptionID;
//    private final boolean durable;
//    private final int acknowledgeMode;

    public static class AndesClientConfigurationBuilder {
        //required

        //optional

        //build method
        public AndesClientConfiguration build() {
            return new AndesClientConfiguration(this);
        }
    }


//
//    public AndesClientConfiguration(String xmlFilePath) {
//
//    }
//
//    public String getUserName() {
//        return userName;
//    }
//
//    public void setUserName(String userName) {
//        this.userName = userName;
//    }
//
//    public String getPassword() {
//        return password;
//    }
//
//    public void setPassword(String password) {
//        this.password = password;
//    }
//
//    public String getDestinationName() {
//        return destinationName;
//    }
//
//    public void setDestinationName(String destinationName) {
//        this.destinationName = destinationName;
//    }
//
//
//    public String getConnectionString() {
//        return connectionString;
//    }
//
//    public void setConnectionString(String connectionString) {
//        this.connectionString = connectionString;
//    }
//
//    public String getHostName() {
//        return hostName;
//    }
//
//    public void setHostName(String hostName) {
//        this.hostName = hostName;
//    }
//
//    public int getPort() {
//        return port;
//    }
//
//    public void setPort(int port) {
//        this.port = port;
//    }
//
//    public String getTCPConnectionURL() {
//        if (this.connectionString != null && !this.connectionString.equals("")) {
//            log.info("Using connection string : " + this.connectionString);
//            return this.connectionString;
//        } else {
//            String builtConnectionString = "amqp://" + this.userName + ":" + this.password + "@" + CARBON_CLIENT_ID + "/" + CARBON_VIRTUAL_HOST_NAME + "?brokerlist='tcp://" + this.hostName + ":" + this.port + "'";
//            log.info("Building connection string : " + builtConnectionString);
//            return builtConnectionString;
//        }
//    }
//
//    public long getRunningDelay() {
//        return runningDelay;
//    }
//
//    public void setRunningDelay(long runningDelay) {
//        this.runningDelay = runningDelay;
//    }
//
//
//    public long getPrintsPerMessageCount() {
//        return printsPerMessageCount;
//    }
//
//    /**
//     * if printsPerMessageCount is -1, message details are printed at each message.
//     * if printsPerMessageCount is 0, message details are not printed.
//     * if printsPerMessageCount > 0, message details are printed once at mentioned value
//     *
//     * @param printsPerMessageCount
//     */
//    public void setPrintsPerMessageCount(long printsPerMessageCount) {
//        if (printsPerMessageCount < -1) {
//            throw new IllegalArgumentException("Value cannot be less than -1");
//        }
//
//        this.printsPerMessageCount = printsPerMessageCount;
//    }
//

}
