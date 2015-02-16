package org.wso2.mb.integration.common.clients;


import org.apache.log4j.Logger;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientOutputParser;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;

import javax.jms.JMSException;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AndesClient {
    private static Logger log = Logger.getLogger(AndesClient.class);
    private long startDelay = 0L;
    List<AndesJMSConsumer> consumers = new ArrayList<AndesJMSConsumer>();
    List<AndesJMSPublisher> publishers = new ArrayList<AndesJMSPublisher>();

    public AndesClient(AndesJMSClientConfiguration config, int numberOfThreads)
            throws JMSException, NamingException, AndesClientException {
        if (0 < numberOfThreads) {
            for (int i = 0; i < numberOfThreads; i++) {
                if (config instanceof AndesJMSConsumerClientConfiguration) {
                    consumers.add(new AndesJMSConsumer((AndesJMSConsumerClientConfiguration) config));
                    AndesClientUtils.initializeReceivedMessagesPrintWriter(((AndesJMSConsumerClientConfiguration) config).getFilePathToWriteReceivedMessages());
                } else if (config instanceof AndesJMSPublisherClientConfiguration) {
                    publishers.add(new AndesJMSPublisher((AndesJMSPublisherClientConfiguration) config));
                }
            }
        } else {
            throw new AndesClientException("The amount of subscribers cannot be less than 1");
        }


    }

    public AndesClient(AndesJMSClientConfiguration config) throws JMSException, NamingException {
        if (config instanceof AndesJMSConsumerClientConfiguration) {
            consumers.add(new AndesJMSConsumer((AndesJMSConsumerClientConfiguration) config));
            AndesClientUtils.initializeReceivedMessagesPrintWriter(((AndesJMSConsumerClientConfiguration) config).getFilePathToWriteReceivedMessages());
        } else if (config instanceof AndesJMSPublisherClientConfiguration) {
            publishers.add(new AndesJMSPublisher((AndesJMSPublisherClientConfiguration) config));
        }
    }

    public void startClient() throws NamingException, JMSException, IOException {
        for (AndesJMSConsumer consumer : consumers) {
            consumer.startClient();
            if (this.startDelay > 0L) {
                AndesClientUtils.sleepForInterval(this.startDelay);
            }
        }
        for (AndesJMSPublisher publisher : publishers) {
            publisher.startClient();
            if (this.startDelay > 0L) {
                AndesClientUtils.sleepForInterval(this.startDelay);
            }
        }
    }

    public void stopClient() throws JMSException {
        for (AndesJMSConsumer consumer : consumers) {
            consumer.stopClient();
        }
        for (AndesJMSPublisher publisher : publishers) {
            publisher.stopClient();
        }
    }

    public long getReceivedMessageCount() {
        long allReceivedMessageCount = 0L;
        for (AndesJMSConsumer consumer : consumers) {
            allReceivedMessageCount = allReceivedMessageCount + consumer.getReceivedMessageCount();
        }
        return allReceivedMessageCount;
    }

    public double getConsumerTPS() {
        double tps = 0L;
        for (AndesJMSConsumer consumer : consumers) {
            tps = tps + consumer.getConsumerTPS();
        }
        return tps / consumers.size();
    }

    public double getAverageLatency() {
        double averageLatency = 0L;
        for (AndesJMSConsumer consumer : consumers) {
            averageLatency = averageLatency + consumer.getAverageLatency();
        }
        return averageLatency / consumers.size();
    }

    public long getSentMessageCount() {
        long allSentMessageCount = 0L;
        for (AndesJMSPublisher publisher : publishers) {
            allSentMessageCount = allSentMessageCount + publisher.getSentMessageCount();
        }
        return allSentMessageCount;
    }

    public double getPublisherTPS() {
        double tps = 0L;
        for (AndesJMSPublisher publisher : publishers) {
            tps = tps + publisher.getPublisherTPS();
        }
        return tps / publishers.size();
    }

    public Map<Long, Integer> checkIfMessagesAreDuplicated()
            throws IOException {
        if (0 < consumers.size()) {
            AndesClientUtils.flushPrintWriters();
            AndesClientOutputParser andesClientOutputParser = new AndesClientOutputParser(consumers.get(0).getConfig().getFilePathToWriteReceivedMessages());
            return andesClientOutputParser.checkIfMessagesAreDuplicated();
        } else {
            return null;
        }
    }

    public boolean checkIfMessagesAreInOrder()
            throws IOException {
        if (0 < consumers.size()) {
            AndesClientOutputParser andesClientOutputParser = new AndesClientOutputParser(consumers.get(0).getConfig().getFilePathToWriteReceivedMessages());
            return andesClientOutputParser.checkIfMessagesAreInOrder();
        } else {
            return false;
        }
    }

    /**
     * This method return whether received messages are transacted
     *
     * @param operationOccurredIndex Index of the operated message most of the time last message
     * @return
     */
    public boolean transactedOperation(long operationOccurredIndex)
            throws IOException {
        if (0 < consumers.size()) {
            AndesClientOutputParser andesClientOutputParser = new AndesClientOutputParser(consumers.get(0).getConfig().getFilePathToWriteReceivedMessages());
            return andesClientOutputParser.transactedOperations(operationOccurredIndex);
        } else {
            return false;
        }
    }

    /**
     * This method returns number of duplicate received messages
     *
     * @return duplicate message count
     */
    public long getTotalNumberOfDuplicates()
            throws IOException {
        if (0 < consumers.size()) {
            AndesClientOutputParser andesClientOutputParser = new AndesClientOutputParser(consumers.get(0).getConfig().getFilePathToWriteReceivedMessages());
            return andesClientOutputParser.numberDuplicatedMessages();
        } else {
            return -1L;
        }
    }

    public long getStartDelay() {
        return startDelay;
    }

    public void setStartDelay(long startDelay) {
        this.startDelay = startDelay;
    }

    public AndesJMSClientConfiguration getConfig() {
        return this.consumers.get(0).getConfig();
    }
}
