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

package org.wso2.mb.integration.common.clients.operations.queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class QueueMessageSender implements Runnable {

    private static Log log = LogFactory.getLog(QueueMessageSender.class);

    public static final String QPID_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    private static final String CF_NAME = "andesConnectionfactory";
    private static final String CARBON_CLIENT_ID = "carbon";
    private static final String CARBON_VIRTUAL_HOST_NAME = "carbon";

    private String hostName = "localhost";
    private String port = "5672";
    private String connectionString = "";
    private AtomicInteger messageCounter;
    private int numOfMessagesToSend;
    private int delay;
    private boolean readFromFile = false;
    private String filePath = "";

    private QueueConnection queueConnection = null;
    private QueueSession queueSession = null;
    private QueueSender queueSender = null;
    private String queueName = null;
    private int printNumberOfMessagesPer = 1;
    private boolean isToPrintEachMessage = false;

    private String typeOfMessage = "text";

    /**
     * By default, according to JMS 1.1, message expiration is only activated if this value is larger than 0.
     */
    private Long jmsExpiration = 0L;

    public QueueMessageSender(String connectionString, String hostName, String port,
                              String userName,
                              String password, String queueName,
                              AtomicInteger messageCounter, int numOfMessagesToSend,
                              int delayBetweenMessages,
                              String filePath, int printNumberOfMessagesPer,
                              boolean isToPrintEachMessage,
                              Long jmsExpiration, String jmsType) {

        this.hostName = hostName;
        this.port = port;
        this.connectionString = connectionString;
        this.messageCounter = messageCounter;
        this.queueName = queueName;
        this.numOfMessagesToSend = numOfMessagesToSend;
        this.delay = delayBetweenMessages;
        this.filePath = filePath;
        if (filePath != null && !filePath.equals("")) {
            readFromFile = true;
        }
        this.printNumberOfMessagesPer = printNumberOfMessagesPer;
        this.isToPrintEachMessage = isToPrintEachMessage;
        this.jmsExpiration = jmsExpiration;

        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        properties.put("queue." + queueName, queueName);

        log.info("getTCPConnectionURL(userName,password) = " + getTCPConnectionURL(userName, password));

        try {
            InitialContext ctx = new InitialContext(properties);
            // Lookup connection factory
            QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx.lookup(CF_NAME);
            queueConnection = connFactory.createQueueConnection();
            queueConnection.start();
            queueSession = queueConnection.createQueueSession(false, QueueSession.AUTO_ACKNOWLEDGE);

            // Send message
            Queue queue = (Queue) ctx.lookup(queueName);
            queueSender = queueSession.createSender(queue);

        } catch (NamingException e) {
            log.error("Error while looking up for queue", e);
        } catch (JMSException e) {
            log.error("Error while initializing queue connection", e);
        }

    }

    private String getTCPConnectionURL(String username, String password) {
        if (connectionString != null && !connectionString.equals("")) {
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

    public void run() {
        try {
            Message message = null;
            String everything = "";
            if (readFromFile) {
                BufferedReader br = new BufferedReader(new FileReader(filePath));
                try {
                    StringBuilder sb = new StringBuilder();
                    String line = br.readLine();

                    while (line != null) {
                        sb.append(line);
                        sb.append('\n');
                        line = br.readLine();
                    }

                    // Remove the last appended next line since there is no next line.
                    sb.replace(sb.length() - 1, sb.length() + 1, "");

                    everything = sb.toString();
                } finally {

                    br.close();

                }
            }

            long threadID = Thread.currentThread().getId();
            int localMessageCount = 0;
            while (messageCounter.get() < numOfMessagesToSend) {
                if (typeOfMessage.equals("text")) {
                    if (!readFromFile) {
                        message = queueSession.createTextMessage("sending Message:-" + messageCounter.get() + "- " +
                                "ThreadID:" + threadID);
                    } else {
                        message = queueSession.createTextMessage(everything);
                    }
                } else if (typeOfMessage.equals("byte")) {
                    message = queueSession.createBytesMessage();
                } else if (typeOfMessage.equals("map")) {
                    message = queueSession.createMapMessage();
                } else if (typeOfMessage.equals("object")) {
                    message = queueSession.createObjectMessage();
                } else if (typeOfMessage.equals("stream")) {
                    message = queueSession.createStreamMessage();
                }
                message.setStringProperty("msgID", Integer.toString(messageCounter.get()));
                synchronized (messageCounter.getClass()) {
                    if (messageCounter.get() >= numOfMessagesToSend) {
                        break;
                    }
                    queueSender.send(message, DeliveryMode.PERSISTENT, 0, jmsExpiration);
                    messageCounter.incrementAndGet();
                }
                localMessageCount++;
                if (messageCounter.get() % printNumberOfMessagesPer == 0) {

                    log.info((readFromFile ? "(FROM FILE)" : "(SIMPLE MESSAGE) ") + "[QUEUE SEND] ThreadID:" +
                            threadID + " queueName:" +
                            queueName + " localMessageCount:" + localMessageCount + " totalMessageCount:-" +
                            messageCounter.get() + "- count to send:" +
                            numOfMessagesToSend);
                }
                if (isToPrintEachMessage) {
                    log.info("(count:" + messageCounter.get() + "/threadID:" + threadID + ") " + message);
                }
                if (delay != 0) {
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        //silently ignore
                    }
                }
            }

            stopSending();

        } catch (JMSException e) {
            log.error("Error while publishing messages", e);
        } catch (IOException e) {
            log.error("Error while reading file", e);
        }
    }

    public synchronized void stopSending() {
        try {
            if (queueSender != null) {
                queueSender.close();
                queueSender = null;
            }
            if (queueSession != null) {
                queueSession.close();
                queueSession = null;
            }
            if (queueConnection != null) {
                queueConnection.close();
                queueConnection = null;
            }
        } catch (JMSException e) {
            log.error("Error while stopping the sender.", e);
        }
    }

    public String getTypeOfMessage() {
        return typeOfMessage;
    }

    public void setTypeOfMessage(String typeOfMessage) {
        this.typeOfMessage = typeOfMessage;
    }
}
