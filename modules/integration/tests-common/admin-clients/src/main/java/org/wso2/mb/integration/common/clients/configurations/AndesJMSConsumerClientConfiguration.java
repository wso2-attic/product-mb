package org.wso2.mb.integration.common.clients.configurations;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSAcknowledgeMode;

import java.io.File;
import java.io.FileNotFoundException;

public class AndesJMSConsumerClientConfiguration extends AndesJMSClientConfiguration {
    private static Logger log = Logger.getLogger(AndesJMSConsumerClientConfiguration.class);
    private long unSubscribeAfterEachMessageCount;
    private long rollbackAfterEachMessageCount;
    private long commitAfterEachMessageCount;
    private long acknowledgeAfterEachMessageCount;
    private String filePathToWriteReceivedMessages;
    private long maximumMessagesToReceived;
    private String subscriptionID;
    private boolean durable;
    private JMSAcknowledgeMode acknowledgeMode;
    private boolean async;
    private int subscriberCount;

    public AndesJMSConsumerClientConfiguration() {
        super();
    }

    public AndesJMSConsumerClientConfiguration(
            ExchangeType exchangeType, String destinationName) {
        super(exchangeType, destinationName);
        this.initialize();
    }

    public AndesJMSConsumerClientConfiguration(String connectionString,
                                               ExchangeType exchangeType,
                                               String destinationName) {
        super(connectionString, exchangeType, destinationName);
        this.initialize();
    }

    public AndesJMSConsumerClientConfiguration(String userName, String password,
                                               String hostName, int port,
                                               ExchangeType exchangeType,
                                               String destinationName) {
        super(userName, password, hostName, port, exchangeType, destinationName);
        this.initialize();
    }

    // TODO : implement
    public AndesJMSConsumerClientConfiguration(String xmlConfigFilePath) {
        super(xmlConfigFilePath);
    }

    public AndesJMSConsumerClientConfiguration(
            AndesJMSClientConfiguration config) {
        super(config);
    }

    @Override
    protected void initialize() {
        unSubscribeAfterEachMessageCount = Long.MAX_VALUE;

        //role back only after a certain message count
        rollbackAfterEachMessageCount = Long.MAX_VALUE;

        // commit only after a certain message count
        commitAfterEachMessageCount = Long.MAX_VALUE;

        // acknowledge only after a certain message count
        acknowledgeAfterEachMessageCount = Long.MAX_VALUE;

        // file path to print received messages
        filePathToWriteReceivedMessages = null;

        // maximum number of message received
        maximumMessagesToReceived = Long.MAX_VALUE;

        //generating subscription ID
        subscriptionID = "";

        //for topics. If its queue, keep it as false
        durable = false;

        //session.AUTO_ACKNOWLEDGE
        acknowledgeMode = JMSAcknowledgeMode.AUTO_ACKNOWLEDGE;

        //asynchronous message receive. Using MessageListener of JMS.
        async = true;

        // number of subscribers
        subscriberCount = 1;
    }

    public long getUnSubscribeAfterEachMessageCount() {
        return unSubscribeAfterEachMessageCount;
    }

    public void setUnSubscribeAfterEachMessageCount(long unSubscribeAfterEachMessageCount)
            throws AndesClientException {
        if (0 < unSubscribeAfterEachMessageCount) {
            this.unSubscribeAfterEachMessageCount = unSubscribeAfterEachMessageCount;
        } else {
            throw new AndesClientException("Value cannot be less than 0");
        }
    }

    public long getRollbackAfterEachMessageCount() {
        return rollbackAfterEachMessageCount;
    }

    public void setRollbackAfterEachMessageCount(long rollbackAfterEachMessageCount)
            throws AndesClientException {
        if (0 < rollbackAfterEachMessageCount) {
            this.rollbackAfterEachMessageCount = rollbackAfterEachMessageCount;
        } else {
            throw new AndesClientException("Value cannot be less than 0");
        }
    }

    public long getCommitAfterEachMessageCount() {
        return commitAfterEachMessageCount;
    }

    public void setCommitAfterEachMessageCount(long commitAfterEachMessageCount)
            throws AndesClientException {
        if (0 < commitAfterEachMessageCount) {
            this.commitAfterEachMessageCount = commitAfterEachMessageCount;
        } else {
            throw new AndesClientException("Value cannot be less than 0");
        }
    }

    public long getAcknowledgeAfterEachMessageCount() {
        return acknowledgeAfterEachMessageCount;
    }

    public void setAcknowledgeAfterEachMessageCount(long acknowledgeAfterEachMessageCount)
            throws AndesClientException {
        if (0 < acknowledgeAfterEachMessageCount) {
            this.acknowledgeAfterEachMessageCount = acknowledgeAfterEachMessageCount;
        } else {
            throw new AndesClientException("Value cannot be less than 0");
        }
    }

    public String getFilePathToWriteReceivedMessages() {
        return filePathToWriteReceivedMessages;
    }

    public void setFilePathToWriteReceivedMessages(String filePathToWriteReceivedMessages)
            throws FileNotFoundException {
        File messagesFilePath = new File(filePathToWriteReceivedMessages);
        if (messagesFilePath.exists() && !messagesFilePath.isDirectory()) {
            this.filePathToWriteReceivedMessages = filePathToWriteReceivedMessages;
        } else {
            throw new FileNotFoundException("File is missing : " + messagesFilePath);
        }
    }

    public long getMaximumMessagesToReceived() {
        return this.maximumMessagesToReceived;
    }

    public void setMaximumMessagesToReceived(long maximumMessagesToReceived)
            throws AndesClientException {
        if (0 < maximumMessagesToReceived) {
            this.maximumMessagesToReceived = maximumMessagesToReceived;
        } else {
            throw new AndesClientException("The maximum number of messages to receive cannot be less than 1");
        }
    }

    public String getSubscriptionID() {
        return subscriptionID;
    }

    public void setSubscriptionID(String subscriptionID) throws AndesClientException {
        if(this.durable){
            if(StringUtils.isNotEmpty(subscriptionID)){
                this.subscriptionID = subscriptionID;
            }else{
                throw new AndesClientException("Subscription ID cannot be null or empty for an durable topic");
            }
        }else{
            this.subscriptionID = subscriptionID;
            log.warn("Setting subscription ID is not necessary for non-durable topics or queues");
        }
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable, String subscriptionID) throws AndesClientException {

        if(durable){
            if(StringUtils.isNotEmpty(subscriptionID)){
                this.subscriptionID = subscriptionID;
            }else{
                throw new AndesClientException("Subscription ID cannot be null or empty for an durable topic");
            }
        }

        this.durable = durable;
    }

    public JMSAcknowledgeMode getAcknowledgeMode() {
        return acknowledgeMode;
    }

    /**
     * int AUTO_ACKNOWLEDGE = 1;
     * int CLIENT_ACKNOWLEDGE = 2;
     * int DUPS_OK_ACKNOWLEDGE = 3;
     * int SESSION_TRANSACTED = 0;
     *
     * @param acknowledgeMode
     * @throws AndesClientException
     */
    public void setAcknowledgeMode(JMSAcknowledgeMode acknowledgeMode) throws AndesClientException {
        this.acknowledgeMode = acknowledgeMode;
    }

    public boolean isAsync() {
        return async;
    }

    public void setAsync(boolean async) {
        this.async = async;
    }

    public int getSubscriberCount() {
        return subscriberCount;
    }

    public void setSubscriberCount(int subscriberCount) throws AndesClientException {
        if (0 < subscriberCount) {
            this.subscriberCount = subscriberCount;
        } else {
            throw new AndesClientException("The amount of subscribers cannot be less than 1");

        }
    }

    @Override
    public String toString() {
        StringBuilder toStringVal = new StringBuilder();
        toStringVal.append(super.toString());
        toStringVal.append("UnSubscribeAfterEachMessageCount=").append(this.unSubscribeAfterEachMessageCount).append("\n");
        toStringVal.append("RollbackAfterEachMessageCount=").append(this.rollbackAfterEachMessageCount).append("\n");
        toStringVal.append("CommitAfterEachMessageCount=").append(this.commitAfterEachMessageCount).append("\n");
        toStringVal.append("AcknowledgeAfterEachMessageCount=").append(this.acknowledgeAfterEachMessageCount).append("\n");
        toStringVal.append("FilePathToWriteReceivedMessages=").append(this.filePathToWriteReceivedMessages).append("\n");
        toStringVal.append("MaximumMessagesToReceived=").append(this.maximumMessagesToReceived).append("\n");
        toStringVal.append("SubscriptionID=").append(this.subscriptionID).append("\n");
        toStringVal.append("Durable=").append(this.durable).append("\n");
        toStringVal.append("AcknowledgeMode=").append(this.acknowledgeMode).append("\n");
        toStringVal.append("Async=").append(this.async).append("\n");
        toStringVal.append("SubscriberCount=").append(this.subscriberCount).append("\n");
        return toStringVal.toString();
    }

    @Override
    public AndesJMSConsumerClientConfiguration clone() throws CloneNotSupportedException {
        return (AndesJMSConsumerClientConfiguration) super.clone();
    }
}
