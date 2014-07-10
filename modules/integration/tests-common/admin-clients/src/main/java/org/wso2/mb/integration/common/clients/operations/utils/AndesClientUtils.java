/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.mb.integration.common.clients.operations.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.mb.integration.common.clients.AndesClient;

import java.io.*;

public class AndesClientUtils {

    private  static PrintWriter printWriterGlobal;
    private static final Log log = LogFactory.getLog(AndesClient.class);
    public static void writeToFile(String whatToWrite, String filePath) {
        try {
            if(printWriterGlobal == null) {
                BufferedWriter bufferedWriter = new BufferedWriter( new FileWriter(filePath));
                PrintWriter printWriter=new PrintWriter(bufferedWriter);
                printWriterGlobal =  printWriter;
            }

            printWriterGlobal.println(whatToWrite);

        } catch (IOException e) {
            System.out.println("Error. File to print received messages is not provided" + e);
        }

    }

    public static void flushPrintWriter() {
        if(printWriterGlobal != null) {
            printWriterGlobal.flush();
        }
    }

    /**
     * Wait until messages are received. When expected count is received (within numberOfSecondsToWaitForMessages)
     * @param client client to evaluate message count from
     * @param messageCountExpected expected message count
     * @param numberOfSecondsToWaitForMessages  number of seconds to wait for messages
     * @return success of the receive
     */
    public static boolean waitUntilMessagesAreReceived(AndesClient client, int messageCountExpected, int numberOfSecondsToWaitForMessages) {
        int tenSecondIterationsToWait = numberOfSecondsToWaitForMessages/10;
        boolean success = false;
        for (int count = 0; count < tenSecondIterationsToWait; count++) {
            try {
                Thread.sleep(1000 * 10);
            } catch (InterruptedException e) {
                //silently ignore
            }
            System.out.println(">>>>total q count=" + client.getReceivedqueueMessagecount() + " total t count=" + client.getReceivedTopicMessagecount());
            if (client.getReceivedqueueMessagecount() == messageCountExpected && client.getReceivedTopicMessagecount() == messageCountExpected) {

                //wait for a small time to until clients does their work (eg: onMessage)
                AndesClientUtils.sleepForInterval(500);
                System.out.println("SUCCESS: Received expected "+ messageCountExpected+". Received q=" + client.getReceivedqueueMessagecount() + " t=" + client.getReceivedTopicMessagecount());
                flushPrintWriter();
                client.shutDownClient();
                return true;
            }  else if(client.getReceivedqueueMessagecount() > messageCountExpected || client.getReceivedTopicMessagecount() > messageCountExpected) {
                //wait for a small time to until clients does their work (eg: onMessage)
                AndesClientUtils.sleepForInterval(500);
                System.out.println("FAILED: Received more messages than expected "+ messageCountExpected+". Received q=" + client.getReceivedqueueMessagecount() + " t=" + client.getReceivedTopicMessagecount());
                flushPrintWriter();
                client.shutDownClient();
                return false;
            }
        }

        System.out.println("FAILED. Did not receive messages expected "+ messageCountExpected+". Received q=" + client.getReceivedqueueMessagecount() + " t=" + client.getReceivedTopicMessagecount());
        flushPrintWriter();
        client.shutDownClient();
        return success;
    }


    public static int getNoOfMessagesReceived(AndesClient client, int messageCountExpected,int numberOfSecondsToWaitForMessages) {
        int tenSecondIterationsToWait = numberOfSecondsToWaitForMessages/10;
        int noOfMessagesReceived = 0;
        for (int count = 0; count < tenSecondIterationsToWait; count++) {
            try {
                Thread.sleep(1000 * 10);
            } catch (InterruptedException e) {
                //silently ignore
            }
            if (client.getReceivedqueueMessagecount() == messageCountExpected){
                //wait for a small time to until clients does their work (eg: onMessage)
                AndesClientUtils.sleepForInterval(500);
                flushPrintWriter();
                client.shutDownClient();
                return client.getReceivedqueueMessagecount() ;
            }

        }
        noOfMessagesReceived = client.getReceivedqueueMessagecount();
        log.info("Number of messages received "+ noOfMessagesReceived);
        flushPrintWriter();
        client.shutDownClient();
        return noOfMessagesReceived;
    }





    public static boolean getIfSenderIsSuccess(AndesClient sendingClient, int expectedMsgCount) {
        boolean sendingSuccess = false;
        if(expectedMsgCount == sendingClient.getReceivedqueueMessagecount()) {
            System.out.println("SENDING: SUCCESS");
            sendingSuccess = true;
        } else {
            System.out.println("SENDING: FAILED");
            sendingSuccess = false;
        }
        return sendingSuccess;
    }

    public static void sleepForInterval(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
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
            System.out.println("File to read sample string to create text file to send is not found");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("Error in reading sample file to create text file to send");
            e.printStackTrace();
        } finally {

            try {
                if(br != null) {
                    br.close();
                }
            } catch (IOException e) {
                System.out.println("Error while closing buffered reader" + e);
            }

        }
        try {

                File fileToCreate = new File(filePathToCreate);

                //no need to recreate if exists
                if (fileToCreate.exists()) {
                    System.out.println("File requested to create already exists. Skipping file creation... " + filePathToCreate);
                    return;
                } else {
                    boolean createFileSuccess = fileToCreate.createNewFile();
                    if(createFileSuccess) {
                        System.out.println("Successfully created a file to append content for sending at " + filePathToCreate);
                    }
                }

            BufferedWriter bufferedWriter = new BufferedWriter( new FileWriter(filePathToCreate));
            PrintWriter printWriter = new PrintWriter(bufferedWriter);

            for(int count = 0; count < sizeInKB/10 ; count++) {
                printWriter.append(sampleKB10StringToWrite);
            }

        } catch (IOException e) {
            System.out.println("Error. File to print received messages is not provided" + e);
        }

    }

}
