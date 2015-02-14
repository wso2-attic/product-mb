/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
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

package org.wso2.mb.integration.common.clients.operations.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.mb.integration.common.clients.AndesClientTemp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

public class AndesClientUtilsTemp {

    private static PrintWriter printWriterGlobal;
    private static final Log log = LogFactory.getLog(AndesClientTemp.class);

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
            PrintWriter printWriter = new PrintWriter(bufferedWriter);
            printWriterGlobal = printWriter;
        } catch (IOException e) {
            log.error("Error initializing Print Writer.", e);
        }
    }

    public static void flushPrintWriter() {
        log.info(Thread.currentThread().getStackTrace());
        if (printWriterGlobal != null) {
            printWriterGlobal.flush();
        }
    }

    /**
     * Wait until messages are received. When expected count is received (within numberOfSecondsToWaitForMessages)
     *
     * @param client                           client to evaluate message count from
     * @param messageCountExpected             expected message count
     * @param numberOfSecondsToWaitForMessages number of seconds to wait for messages
     * @return success of the receive
     */
    public static boolean waitUntilMessagesAreReceived(AndesClientTemp client, int messageCountExpected,
                                                       int numberOfSecondsToWaitForMessages) {
        int tenSecondIterationsToWait = numberOfSecondsToWaitForMessages / 10;
        boolean success = false;
        for (int count = 0; count < tenSecondIterationsToWait; count++) {
            try {
                Thread.sleep(1000 * 10);
            } catch (InterruptedException ignore) {
                //silently ignore
            }
            log.info(">>>>total q count=" + client.getReceivedqueueMessagecount() + " total t count=" + client
                    .getReceivedTopicMessagecount());
            if (client.getReceivedqueueMessagecount() == messageCountExpected && client.getReceivedTopicMessagecount
                    () == messageCountExpected) {

                //wait for a small time to until clients does their work (eg: onMessage)
                AndesClientUtilsTemp.sleepForInterval(500);
                log.info("SUCCESS: Received expected " + messageCountExpected + ". Received q=" + client
                        .getReceivedqueueMessagecount() + " t=" + client.getReceivedTopicMessagecount());
                flushPrintWriter();
                log.info("SHUTTING DOWN AT UTILSTEMP");
                client.shutDownClient();
                return true;
            } else if (client.getReceivedqueueMessagecount() > messageCountExpected || client
                    .getReceivedTopicMessagecount() > messageCountExpected) {
                //wait for a small time to until clients does their work (eg: onMessage)
                AndesClientUtilsTemp.sleepForInterval(500);
                log.info("FAILED: Received more messages than expected " + messageCountExpected + ". Received q=" +
                         client.getReceivedqueueMessagecount() + " t=" + client.getReceivedTopicMessagecount());
                flushPrintWriter();
                log.info("SHUTTING DOWN AT UTILSTEMP");
                client.shutDownClient();
                return false;
            }
        }

        log.info("FAILED. Did not receive messages expected " + messageCountExpected + ". Received q=" + client
                .getReceivedqueueMessagecount() + " t=" + client.getReceivedTopicMessagecount());
        flushPrintWriter();
        log.info("SHUTTING DOWN AT UTILSTEMP");
        client.shutDownClient();
        return success;
    }

    /**
     * Wait specified time and count number of messages of all subscribers
     *
     * @param client
     * @param queueName
     * @param messageCountExpected
     * @param numberOfSecondsToWaitForMessages
     * @return true if total message count equal to expected message count
     */
    public static void waitUntilAllMessagesReceived(AndesClientTemp client, String queueName, int messageCountExpected,
                                                    int numberOfSecondsToWaitForMessages) {
        int lastCount = 0;
        int tenSecondIterationsToWait = numberOfSecondsToWaitForMessages / 10;
        for (int count = 0; count < tenSecondIterationsToWait; count++) {
            int thisCount = client.getReceivedqueueMessagecount();
            if (thisCount == lastCount) {
                break;
            }
            lastCount = thisCount;
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException ignore) {
            }
            log.info("Total messages in " + queueName + " [" + thisCount + "] ");
        }
        flushPrintWriter();
        client.shutDownClient();
    }

    /**
     * Wait specified time and count number of exact messages received by subscribers
     *
     * @param client
     * @param queueName
     * @param messageCountExpected
     * @param numberOfSecondsToWaitForMessages
     * @return
     */
    public static void waitUntilExactNumberOfMessagesReceived(AndesClientTemp client, String queueName,
                                                              int messageCountExpected,
                                                              int numberOfSecondsToWaitForMessages) {
        int lastCount = 0;
        int tenSecondIterationsToWait = numberOfSecondsToWaitForMessages / 10;
        for (int count = 0; count < tenSecondIterationsToWait; count++) {
            int thisCount = client.getReceivedqueueMessagecount();
            if (thisCount >= messageCountExpected || thisCount == lastCount) {
                flushPrintWriter();
                client.shutDownClient();
                log.info("Total exact messages received to " + queueName + " [" + thisCount + "] ");
                break;
            }

            lastCount = thisCount;

            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException ignore) {
            }
        }
    }

    public static int getNoOfMessagesReceived(AndesClientTemp client, int messageCountExpected,
                                              int numberOfSecondsToWaitForMessages) {
        int tenSecondIterationsToWait = numberOfSecondsToWaitForMessages / 10;
        int noOfMessagesReceived = 0;
        for (int count = 0; count < tenSecondIterationsToWait; count++) {
            try {
                Thread.sleep(1000 * 10);
            } catch (InterruptedException ignore) {
                //silently ignore
            }
            if (client.getReceivedqueueMessagecount() >= messageCountExpected) {
                //wait for a small time to until clients does their work (eg: onMessage)
                AndesClientUtilsTemp.sleepForInterval(500);
                flushPrintWriter();
                client.shutDownClient();
                return client.getReceivedqueueMessagecount();
            }

        }
        noOfMessagesReceived = client.getReceivedqueueMessagecount();
        log.info("Number of messages received " + noOfMessagesReceived);
        flushPrintWriter();
        client.shutDownClient();
        return noOfMessagesReceived;
    }


    public static boolean getIfSenderIsSuccess(AndesClientTemp sendingClient, int expectedMsgCount) {
        boolean sendingSuccess = false;
        if (expectedMsgCount == sendingClient.getReceivedqueueMessagecount()) {
            sendingSuccess = true;
        }
        return sendingSuccess;
    }

    public static void sleepForInterval(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException ignore) {
            //ignore
        }
    }

    public static void createTestFileToSend(String filePathToRead, String filePathToCreate, int sizeInKB) {
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
