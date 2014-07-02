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

package org.wso2.mb.integration.common.clients.operations.topic;

import org.wso2.mb.integration.common.clients.operations.utils.*;
import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class TopicMessageReceiver  implements Runnable{

    public static final String QPID_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    private static final String CF_NAME = "andesConnectionfactory";
    private static final String CARBON_CLIENT_ID = "carbon";
    private static final String CARBON_VIRTUAL_HOST_NAME = "carbon";

    private String hostName = "localhost";
    private String port = "5672";
    private String connectionString = "";

    private TopicConnection topicConnection = null;
    private TopicSession topicSession = null;

    private String subscriptionId = null;
    private TopicSubscriber topicSubscriber = null;
    private boolean useMessageListener = true;
    private int delayBetweenMessages = 0;
    private AtomicInteger messageCounter;
    private int stopAfter = Integer.MAX_VALUE;
    private int ackAfterEach = Integer.MAX_VALUE;
    private int commitAfterEach = Integer.MAX_VALUE;
    private int rollbackAfterEach = Integer.MAX_VALUE;
    private int unSubscribeAfter = Integer.MAX_VALUE;
    private String topicName;
    private int printNumberOfMessagesPer = 1;
    private boolean isToPrintEachMessage = false;
    private String fileToWriteReceivedMessages = "";

    //private static final Logger log = Logger.getLogger(topic.TopicMessageReceiver.class);

    public TopicMessageReceiver(String connectionString, String hostName, String port, String userName, String password, String topicName, boolean isDurable, String subscriptionID, int ackMode,
                                boolean useMessageListener, AtomicInteger messageCounter, int delayBetweenMessages, int printNumberOfMessagesPer, boolean isToPrintEachMessage, String fileToWriteReceivedMessages, int stopAfter, int unsubscrbeAfter, int ackAfterEach, int commitAfterEach, int rollbackAfterEach) {

        this.hostName = hostName;
        this.port = port;
        this.connectionString = connectionString;
        this.useMessageListener = useMessageListener;
        this.delayBetweenMessages = delayBetweenMessages;
        this.messageCounter = messageCounter;
        this.topicName = topicName;
        this.printNumberOfMessagesPer = printNumberOfMessagesPer;
        this.isToPrintEachMessage = isToPrintEachMessage;
        this.fileToWriteReceivedMessages = fileToWriteReceivedMessages;
        this.stopAfter = stopAfter;
        this.unSubscribeAfter = unsubscrbeAfter;
        this.ackAfterEach = ackAfterEach;
        this.commitAfterEach = commitAfterEach;
        this.rollbackAfterEach = rollbackAfterEach;

        this.subscriptionId = subscriptionID;

        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        properties.put("topic." + topicName, topicName);

        System.out.println("getTCPConnectionURL(userName,password) = " + getTCPConnectionURL(userName, password));

        try {
            InitialContext ctx = new InitialContext(properties);
            // Lookup connection factory
            TopicConnectionFactory connFactory = (TopicConnectionFactory) ctx.lookup(CF_NAME);
            topicConnection = connFactory.createTopicConnection();
            topicConnection.setClientID(subscriptionId);
            topicConnection.start();
            if(ackMode == TopicSession.SESSION_TRANSACTED) {
                topicSession = topicConnection.createTopicSession(true, QueueSession.SESSION_TRANSACTED);
            }   else {
                topicSession = topicConnection.createTopicSession(false, QueueSession.AUTO_ACKNOWLEDGE);
            }

            // Send message
            Topic topic = (Topic) ctx.lookup(topicName);
            System.out.println("Starting listening on topic: " + topic);

            if(isDurable) {
                topicSubscriber = topicSession.createDurableSubscriber(topic,subscriptionId);
            } else {
                topicSubscriber = topicSession.createSubscriber(topic);
            }

        } catch (NamingException e) {
            System.out.println("Error while looking up for topic" + e);
        } catch (JMSException ex) {
            System.out.println("Error while initializing topic connection" + ex);
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


    public void unsubscribe() {
        try {

            System.out.println("unSubscribing Subscriber");
            this.topicSession.unsubscribe(subscriptionId);
            topicSubscriber.close();
            topicSession.close();
            topicConnection.close();
            System.out.println("done unSubscribing Subscriber");

        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void stopListening(){
        try {

            System.out.println("closing Subscriber");
            topicSubscriber.close();
            topicSession.close();
            topicConnection.close();
            System.out.println("done closing Subscriber");

        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public TopicSubscriber getTopicSubscriber() {
        return this.topicSubscriber;
    }

    public void run() {
        try {
            if(useMessageListener) {
                TopicMessageListener messageListener = new TopicMessageListener(topicConnection,topicSession,topicSubscriber,topicName,
                        subscriptionId, messageCounter,delayBetweenMessages,printNumberOfMessagesPer, isToPrintEachMessage, fileToWriteReceivedMessages, stopAfter,unSubscribeAfter, ackAfterEach, rollbackAfterEach, commitAfterEach);
                topicSubscriber.setMessageListener(messageListener);
            }
            else {

                int localMessageCount = 0;
                while(true) {
                    Message message = topicSubscriber.receive();
                    if (message!=null && message instanceof TextMessage) {
                        messageCounter.incrementAndGet();
                        localMessageCount++;
                        String redelivery;
                        TextMessage textMessage = (TextMessage) message;
                        if(message.getJMSRedelivered()) {
                            redelivery = "REDELIVERED";
                        }  else {
                            redelivery = "ORIGINAL";
                        }
                        if(messageCounter.get() % printNumberOfMessagesPer == 0) {
                            System.out.println("[TOPIC RECEIVE] ThreadID:"+Thread.currentThread().getId()+" topic:"+topicName+" localMessageCount:"+localMessageCount+" totalMessageCount:" + messageCounter.get() + " max count:" + stopAfter );
                        }
                        if(isToPrintEachMessage) {
                            System.out.println("(count:"+messageCounter.get()+"/threadID:"+Thread.currentThread().getId()+"/topic:"+ topicName+") "+ redelivery + " >> " + textMessage.getText());
                            AndesClientUtils.writeToFile(textMessage.getText(), fileToWriteReceivedMessages);
                        }
                    }

                    if(messageCounter.get() % ackAfterEach == 0) {
                        if(topicSession.getAcknowledgeMode() == QueueSession.CLIENT_ACKNOWLEDGE) {
                            if(message != null) {
                                message.acknowledge();
                                System.out.println("****Acked message***");
                            }
                        }
                    }

                    //commit get priority
                    if(messageCounter.get() % commitAfterEach == 0) {
                        topicSession.commit();
                        System.out.println("Committed session");
                    }else if(messageCounter.get() % rollbackAfterEach == 0) {
                        topicSession.rollback();
                        System.out.println("Rollbacked session");
                    }

                    if(messageCounter.get() >= unSubscribeAfter) {
                        unsubscribe();
                        break;
                    } else if(messageCounter.get() >= stopAfter) {
                        stopListening();
                        break;
                    }

                    if(delayBetweenMessages !=0)    {
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
