package org.wso2.mb.integration.common.clients.operations.utils;

import org.apache.log4j.Logger;
import org.wso2.mb.integration.common.clients.AndesJMSConsumerClient;

import javax.jms.JMSException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AndesClientUtils {
    private static PrintWriter receivedMessagePrintWriter;
    private static PrintWriter statisticsPrintWriter;
    private static Logger log = Logger.getLogger(AndesClientUtils.class);

    // TODO : check for missing flushers
    public static void waitUntilAllMessageReceivedAndShutdownClients(AndesJMSConsumerClient client, long waitTimeTillMessageCounterChanges) throws JMSException {
        long previousMessageCount = 0;
        long currentMessageCount = -1;

        // Check each 10 second if new messages have been received, if not shutdown clients.
        // If no message are received this will wait for 20 seconds before shutting down clients.
        while (currentMessageCount != previousMessageCount) {
            try {
                TimeUnit.MILLISECONDS.sleep(waitTimeTillMessageCounterChanges);
            } catch (InterruptedException e) {
                log.error("Error waiting for receiving messages.", e);
            }
            previousMessageCount = currentMessageCount;
            currentMessageCount = client.getReceivedMessageCount();
        }

        log.info("Message count received by consumer : " + Long.toString(client.getReceivedMessageCount()));
        client.stopClient();
        flushPrintWriters();
    }

    public static void sleepForInterval(long milliseconds) {
        if (1 < milliseconds) {
            try {
                Thread.sleep(milliseconds);
            } catch (InterruptedException ignore) {
                //ignore
            }
        }
    }


    public static void createTestFileToSend(String filePathToRead, String filePathToCreate,
                                            int sizeInKB) {
        String sampleKB10StringToWrite = "";
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(filePathToRead));
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append('\n');
                line = br.readLine();
            }
            sampleKB10StringToWrite = sb.toString();
        } catch (FileNotFoundException e) {
            log.error("File to read sample string to create text file to send is not found", e);
        } catch (IOException e) {
            log.error("Error in reading sample file to create text file to send", e);
        } finally {

            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException e) {
                log.error("Error while closing buffered reader", e);
            }

        }
        try {

            File fileToCreate = new File(filePathToCreate);

            //no need to recreate if exists
            if (fileToCreate.exists()) {
                log.info("File requested to create already exists. Skipping file creation... " + filePathToCreate);
                return;
            } else {
                boolean createFileSuccess = fileToCreate.createNewFile();
                if (createFileSuccess) {
                    log.info("Successfully created a file to append content for sending at " + filePathToCreate);
                }
            }

            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePathToCreate));
            PrintWriter printWriter = new PrintWriter(bufferedWriter);

            for (int count = 0; count < sizeInKB / 10; count++) {
                printWriter.append(sampleKB10StringToWrite);
            }

        } catch (IOException e) {
            log.error("Error. File to print received messages is not provided", e);
        }
    }

    public static void writeReceivedMessagesToFile(String content, String filePath) {
        if (receivedMessagePrintWriter == null) {
            initializeReceivedMessagesPrintWriter(filePath);
        }

        receivedMessagePrintWriter.println(content);

    }

    public static void writeStatisticsToFile(String content, String filePath) {
        if (statisticsPrintWriter == null) {
            initializeStatisticsPrintWriter(filePath);
        }

        statisticsPrintWriter.println(content);

    }

    /**
     * Initialize the print writer. This needs to be invoked before each test case.
     *
     * @param filePath The file path to write to.
     */
    public static void initializeReceivedMessagesPrintWriter(String filePath) {
        try {
            File writerFile = new File(filePath);
            if (writerFile.exists() || writerFile.createNewFile()) {
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath));
                receivedMessagePrintWriter = new PrintWriter(bufferedWriter);
            }
        } catch (IOException e) {
            log.error("Error initializing Print Writer.", e);
        }
    }

    /**
     * Initialize the print writer. This needs to be invoked before each test case.
     *
     * @param filePath The file path to write to.
     */
    public static void initializeStatisticsPrintWriter(String filePath) {
        try {
            File writerFile = new File(filePath);
            if (writerFile.exists() || writerFile.createNewFile()) {
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath));
                statisticsPrintWriter = new PrintWriter(bufferedWriter);
                statisticsPrintWriter.println("TIMESTAMP,CONSUMER_TPS,AVERAGE_LATENCY,TIMESTAMP,PUBLISHER_TPS");
            }
        } catch (IOException e) {
            log.error("Error initializing Print Writer.", e);
        }
    }

    public static void flushPrintWriters() {
        if (receivedMessagePrintWriter != null) {
            receivedMessagePrintWriter.flush();
        }

        if (statisticsPrintWriter != null) {
            statisticsPrintWriter.flush();
        }
    }

    public static Map<Long, Integer> checkIfMessagesAreDuplicated(AndesJMSConsumerClient consumer)
            throws IOException {
        AndesClientUtils.flushPrintWriters();
        AndesClientOutputParser andesClientOutputParser = new AndesClientOutputParser(consumer.getConfig().getFilePathToWriteReceivedMessages());
        return andesClientOutputParser.checkIfMessagesAreDuplicated();
    }

    public static boolean checkIfMessagesAreInOrder(AndesJMSConsumerClient consumer)
            throws IOException {
        AndesClientOutputParser andesClientOutputParser = new AndesClientOutputParser(consumer.getConfig().getFilePathToWriteReceivedMessages());
        return andesClientOutputParser.checkIfMessagesAreInOrder();
    }

    /**
     * This method return whether received messages are transacted
     *
     * @param operationOccurredIndex Index of the operated message most of the time last message
     * @return
     */
    public static boolean transactedOperation(AndesJMSConsumerClient consumer, long operationOccurredIndex)
            throws IOException {
        AndesClientOutputParser andesClientOutputParser = new AndesClientOutputParser(consumer.getConfig().getFilePathToWriteReceivedMessages());
        return andesClientOutputParser.transactedOperations(operationOccurredIndex);
    }

    /**
     * This method returns number of duplicate received messages
     *
     * @return duplicate message count
     */
    public static long getTotalNumberOfDuplicates(AndesJMSConsumerClient consumer)
            throws IOException {
        AndesClientOutputParser andesClientOutputParser = new AndesClientOutputParser(consumer.getConfig().getFilePathToWriteReceivedMessages());
        return andesClientOutputParser.numberDuplicatedMessages();
    }
}
