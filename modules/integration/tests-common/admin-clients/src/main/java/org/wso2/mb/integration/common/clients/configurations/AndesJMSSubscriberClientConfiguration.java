package org.wso2.mb.integration.common.clients.configurations;

import org.apache.commons.lang.StringUtils;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.UUID;

public class AndesJMSSubscriberClientConfiguration extends AndesJMSClientConfiguration {
    private long unSubscribeAfterEachMessageCount;
    private  long rollbackAfterEachMessageCount;
    private  long commitAfterEachMessageCount;
    private  long acknowledgeAfterEachMessageCount;
    private  String filePathToWriteReceivedMessages;
    private  long maximumMessagesToReceived;
    private  String subscriptionID;
    private  boolean durable;
    private  int acknowledgeMode;
    private  boolean async;
    private  int subscriberCount;

    public AndesJMSSubscriberClientConfiguration(String connectionString,
                                                 ExchangeType exchangeType,
                                                 String destinationName) {
        super(connectionString, exchangeType, destinationName);
        this.initialize();
    }

    public AndesJMSSubscriberClientConfiguration(String userName, String password,
                                                 String hostName, int port,
                                                 ExchangeType exchangeType,
                                                 String destinationName) {
        super(userName, password, hostName, port, exchangeType, destinationName);
        this.initialize();
    }

    public AndesJMSSubscriberClientConfiguration(AndesJMSClientConfiguration config) {
        super(config.getConnectionString(), config.getExchangeType(), config.getDestinationName());
        this.initialize();
    }

    @Override
    public void initialize() {
        super.initialize();
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
        subscriptionID = UUID.randomUUID().toString().replace("-", "");

        //for topics. If its queue, keep it as false
        durable = false;

        //session.AUTO_ACKNOWLEDGE
        acknowledgeMode = 1;

        //asynchronous message receive. Using MessageListener of JMS
        async = false;

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
        }else{
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
        }else{
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
        }else{
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
        }else{
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
        }else{
            throw new FileNotFoundException("File is missing : " + messagesFilePath);
        }
    }

    public long getMaximumMessagesToReceived() {
        return maximumMessagesToReceived;
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
        if (null != subscriptionID && StringUtils.isNotEmpty(subscriptionID)) {
            this.subscriptionID = subscriptionID;
        } else {
            throw new AndesClientException("Subscription ID cannot be empty or null");
        }
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public int getAcknowledgeMode() {
        return acknowledgeMode;
    }

    public void setAcknowledgeMode(int acknowledgeMode) throws AndesClientException {
        if (0 <= acknowledgeMode && 3 >= acknowledgeMode) {
            this.acknowledgeMode = acknowledgeMode;
        }else{
            throw new AndesClientException("Invalid acknowledge mode");
        }
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
        }else{
            throw new AndesClientException("The amount of subscribers cannot be less than 1");

        }
    }
}
