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

package org.wso2.mb.integration.common.clients.operations.queue;

import org.wso2.mb.integration.common.clients.operations.utils.*;
import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class QueueMessageReceiver implements Runnable {

    public static final String QPID_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    private static final String CF_NAME = "andesConnectionfactory";
    private static final String CARBON_CLIENT_ID = "carbon";
    private static final String CARBON_VIRTUAL_HOST_NAME = "carbon";

    private String hostName = "localhost";
    private String port = "5672";
    private String connectionString = "";

    private QueueConnection queueConnection = null;
    private QueueSession queueSession = null;

    private QueueReceiver queueReceiver = null;
    private boolean useMessageListener = true;
    private int delayBetweenMessages = 0;
    private AtomicInteger messageCounter;
    private int stopAfter = Integer.MAX_VALUE;
    private int ackAfterEach = Integer.MAX_VALUE;
    private int commitAfterEach = Integer.MAX_VALUE;
    private int rollbackAfterEach = Integer.MAX_VALUE;
    private String queueName;
    private  int printNumberOfMessagesPer = 1;
    private  boolean isToPrintEachMessage = false;
    private String fileToWriteReceivedMessages = "";

    //private static final Logger log = Logger.getLogger(queue.QueueMessageReceiver.class);

    public QueueMessageReceiver(String connectionString, String hostName, String port, String userName, String password, String queueName, int ackMode,
                                boolean useMessageListener, AtomicInteger messageCounter, int delayBetweenMessages, int printNumberOfMessagesPer, boolean isToPrintEachMessage, String fileToWriteReceivedMessages, int stopAfter, int ackAfterEach, int commitAfterEach, int rollbackAfterEach) {

        this.hostName = hostName;
        this.port = port;
        this.connectionString = connectionString;
        this.useMessageListener = useMessageListener;
        this.delayBetweenMessages = delayBetweenMessages;
        this.messageCounter = messageCounter;
        this.queueName = queueName;
        this.printNumberOfMessagesPer = printNumberOfMessagesPer;
        this.isToPrintEachMessage = isToPrintEachMessage;
        this.fileToWriteReceivedMessages = fileToWriteReceivedMessages;
        this.stopAfter = stopAfter;
        this.ackAfterEach = ackAfterEach;
        this.commitAfterEach = commitAfterEach;
        this.rollbackAfterEach = rollbackAfterEach;

        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        properties.put("queue." + queueName, queueName);

        System.out.println("getTCPConnectionURL(userName,password) = " + getTCPConnectionURL(userName, password));

        try {
            InitialContext ctx = new InitialContext(properties);
            // Lookup connection factory
            QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx.lookup(CF_NAME);
            queueConnection = connFactory.createQueueConnection();
            queueConnection.start();
            if (ackMode == QueueSession.SESSION_TRANSACTED) {
                queueSession = queueConnection.createQueueSession(true, ackMode);
            } else {
                queueSession = queueConnection.createQueueSession(false, ackMode);
            }
            Queue queue = (Queue) ctx.lookup(queueName);
            queueReceiver = queueSession.createReceiver(queue);

        } catch (NamingException e) {
            System.out.println("Error while looking up for queue" + e);
        } catch (JMSException ex) {
            System.out.println("Error while initializing queue connection" + ex);
        }

    }

    private String getTCPConnectionURL(String username, String password) {
        if(connectionString != null && !connectionString.equals("")) {
            return connectionString;
        } else {
            return new StringBuffer()
                    .append("amqp://").append(username).append(":").append(password)
                    .append("@").append(CARBON_CLIENT_ID)
                    .append("/").append(CARBON_VIRTUAL_HOST_NAME)
                    .append("?brokerlist='tcp://").append(hostName).append(":").append(port).append("'")
                    .toString();
        }
    }

    public void stopListening() {
        try {

            System.out.println("closing Subscriber");
            queueReceiver.close();
            queueSession.close();
            queueConnection.close();
            System.out.println("done closing Subscriber");

        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public QueueReceiver getQueueReceiver() {
        return this.queueReceiver;
    }

    public void run() {
        try {
            if (useMessageListener) {
                QueueMessageListener messageListener = new QueueMessageListener(queueConnection, queueSession, queueReceiver, queueName, messageCounter,
                        delayBetweenMessages, printNumberOfMessagesPer, isToPrintEachMessage, fileToWriteReceivedMessages,
                        stopAfter, ackAfterEach, commitAfterEach, rollbackAfterEach);

                queueReceiver.setMessageListener(messageListener);
            } else {
                int localMessageCount =0;
                while (true) {
                    Message message = queueReceiver.receive();
                    if (message!= null && message instanceof TextMessage) {
                        messageCounter.incrementAndGet();
                        localMessageCount++;

                        String redelivery;
                        TextMessage textMessage = (TextMessage) message;
                        if (message.getJMSRedelivered()) {
                            redelivery = "REDELIVERED";
                        } else {
                            redelivery = "ORIGINAL";
                        }
                        if(messageCounter.get() % printNumberOfMessagesPer == 0) {
                            System.out.println("[QUEUE RECEIVE] ThreadID:"+Thread.currentThread().getId()+" queue:"+queueName+" localMessageCount:"+localMessageCount+
                                    " totalMessageCount:" + messageCounter.get() + " max count:" + stopAfter );
                        }
                        if(isToPrintEachMessage) {
                            System.out.println("(count:"+messageCounter.get()+"/threadID:"+Thread.currentThread().getId()+"/queue:"+queueName+") "+ redelivery + " >> " + textMessage.getText());
                            AndesClientUtils.writeToFile(textMessage.getText(), fileToWriteReceivedMessages);
                        }
                    }

                    if(messageCounter.get() % ackAfterEach == 0) {
                        if(queueSession.getAcknowledgeMode() == QueueSession.CLIENT_ACKNOWLEDGE) {
                            if(message != null) {
                                message.acknowledge();
                                System.out.println("****Acked message***");
                            }
                        }
                    }

                    //commit get priority
                    if (messageCounter.get() % commitAfterEach == 0) {
                        queueSession.commit();
                        System.out.println("Committed Queue Session");
                    } else if (messageCounter.get() % rollbackAfterEach == 0) {
                        queueSession.rollback();
                        System.out.println("Rollbacked Queue Session");
                    }

                    if (messageCounter.get() == stopAfter) {
                        stopListening();
                        break;
                    }
                    if (delayBetweenMessages != 0) {
                        try {
                            Thread.sleep(delayBetweenMessages);
                        } catch (InterruptedException e) {
                            //silently ignore
                        }
                    }
                }
            }
        } catch (JMSException e) {
            System.out.println("Error while listening for messages" + e);
        }

    }
}
