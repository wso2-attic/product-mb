package org.wso2.mb.integration.common.clients.operations.utils;

import org.apache.log4j.Logger;
import org.wso2.mb.integration.common.clients.AndesClient;

import javax.jms.JMSException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class AndesClientUtils {

    private static PrintWriter printWriterGlobal;
    private static Logger log = Logger.getLogger(AndesClientUtils.class);

    public static void writeToFile(String whatToWrite, String filePath) {
        if (printWriterGlobal == null) {
            initializePrintWriter(filePath);
        }

        printWriterGlobal.println(whatToWrite);

    }

    /**
     * Initialize the print writer. This needs to be invoked before each test case.
     *
     * @param filePath The file path to write to.
     */
    public static void initializePrintWriter(String filePath) {
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath));
            printWriterGlobal = new PrintWriter(bufferedWriter);
        } catch (IOException e) {
            log.error("Error initializing Print Writer.", e);
        }
    }

    public static void flushPrintWriter() {
        if (printWriterGlobal != null) {
            printWriterGlobal.flush();
        }
    }

    /**
     * Wait until messages are received. When expected count is received (within numberOfSecondsToWaitForMessages)
     *
     * @param client               client to evaluate message count from
     * @param messageCountExpected expected message count
     * @param waitTimePerMessage   number of seconds to wait for messages
     * @return success of the receive
     */
    public static boolean waitUntilMessagesAreReceived(AndesClient client,
                                                       long messageCountExpected,
                                                       long waitTimePerMessage)
            throws JMSException {
        try {
            if (messageCountExpected < 0) {
                throw new IllegalArgumentException("Expected message count cannot be less than 0");
            }
            if (waitTimePerMessage < 0) {
                throw new IllegalArgumentException("Wait time per message cannot be less than 0");
            }

            for (int count = 0; count < messageCountExpected; count++) {
                sleepForInterval(waitTimePerMessage);
                log.info(">>>>Total received message count=" + client.getReceivedMessageCount());
                if (client.getReceivedMessageCount() == messageCountExpected) {
                    log.info("SUCCESS: Received expected " + messageCountExpected + ". Received message count=" + client.getReceivedMessageCount());
                    return true;
                } else if (client.getReceivedMessageCount() > messageCountExpected) {
                    log.info("FAILED: Received more messages than expected " + messageCountExpected + ". Received message count=" + client.getReceivedMessageCount());
                    return false;
                }

                // if no messages are received yet, keep on checking for messages with a thread sleep.
            }

            log.info("FAILED. Did not receive messages expected " + messageCountExpected + ". Received message count=" + client.getReceivedMessageCount());
            return false;
        } finally {
            flushPrintWriter();
            client.stopClient();
        }
    }

    /**
     * Wait specified time and count number of messages of all subscribers
     *
     * @param client
     * @param messageCountExpected
     * @param waitTimePerMessage
     * @throws InterruptedException
     * @throws JMSException
     */
    public static void waitUntilAllMessagesReceived(AndesClient client,
                                                    int messageCountExpected,
                                                    long waitTimePerMessage)
            throws InterruptedException, JMSException {
        long lastCount = 0;
        for (int count = 0; count < messageCountExpected; count++) {
            long thisCount = client.getReceivedMessageCount();
            if (thisCount == lastCount) {
                break;
            }
            lastCount = thisCount;
            sleepForInterval(waitTimePerMessage);

            log.info("Total messages received : " + thisCount);
        }
        flushPrintWriter();
        client.stopClient();
    }

    /**
     * Wait specified time and count number of exact messages received by subscribers
     *
     * @param client
     * @param messageCountExpected
     * @param waitTimePerMessage
     * @throws JMSException
     */
    public static void waitUntilExactOrMoreNumberOfMessagesReceived(AndesClient client,
                                                              int messageCountExpected,
                                                              long waitTimePerMessage) throws JMSException {
        long lastCount = 0;
        for (int count = 0; count < messageCountExpected; count++) {

            long thisCount = client.getReceivedMessageCount();
            if (thisCount >= messageCountExpected || thisCount == lastCount) {
                log.info("Exact number of messages received : " + thisCount);
                break;
            }
            lastCount = thisCount;

            sleepForInterval(waitTimePerMessage);
        }

        flushPrintWriter();
        client.stopClient();
    }

    public static long waitAndGetNumberOfMessagesReceived(AndesClient client, long waitTime, boolean closeClient)
            throws JMSException {

        sleepForInterval(waitTime);
        long receivedMessageCount = client.getReceivedMessageCount();
        if (closeClient) {
            flushPrintWriter();
            client.stopClient();
        }

        log.info("Number of messages received : " + receivedMessageCount);
        return receivedMessageCount;
    }

    public static boolean getIfSenderIsSuccess(AndesClient sendingClient, int expectedSentMessageCount) {
        boolean sendingSuccess = false;
        long sentMessageCount = sendingClient.getSentMessageCount();
        if (expectedSentMessageCount == sentMessageCount) {
            sendingSuccess = true;
        }
        log.info("Message count sent by client : " + Long.toString(sentMessageCount));
        return sendingSuccess;
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
}
