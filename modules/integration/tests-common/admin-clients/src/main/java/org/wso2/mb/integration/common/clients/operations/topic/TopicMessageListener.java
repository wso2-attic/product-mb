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
import java.util.concurrent.atomic.AtomicInteger;

public class TopicMessageListener implements MessageListener {
    private TopicConnection topicConnection;
    private TopicSession topicSession;
    private MessageConsumer topicReceiver;
    private String subscriptionId;
    private int delayBetweenMessages;
    private AtomicInteger messageCount;
    private int localMessageCount;
    private int printNumberOfMessagesPer = 1;
    private boolean isToPrintEachMessage = false;
    private String fileToWriteReceivedMessages = "";
    private int stopMessageCount;
    private int ackAfterEach;
    private int unsubscribeMessageCount;
    private int rollbackPerMessagecount;
    private int commitPerMessageCount;
    private final String topicName;

    //private static final Logger log = Logger.getLogger(topic.TopicMessageListener.class);

    public TopicMessageListener(TopicConnection topicConnection, TopicSession topicSession,
                                MessageConsumer topicReceiver, String topicName, String subscriptionId, AtomicInteger messageCounter, int delayBetweenMessages, int printNumberOfMessagesPer, boolean isToPrintEachMessage, String fileToWriteReceivedMessages, int stopAfter, int unsubscribeAfter, int ackAfterEach, int rollbackPerMessageCount, int commitPerMessageCount) {
        this.topicConnection = topicConnection;
        this.topicSession = topicSession;
        this.topicReceiver = topicReceiver;
        this.subscriptionId = subscriptionId;
        this.delayBetweenMessages = delayBetweenMessages;
        this.printNumberOfMessagesPer = printNumberOfMessagesPer;
        this.isToPrintEachMessage = isToPrintEachMessage;
        this.fileToWriteReceivedMessages = fileToWriteReceivedMessages;
        this.messageCount = messageCounter;
        this.topicName = topicName;
        this.localMessageCount =0;
        this.stopMessageCount = stopAfter;
        this.unsubscribeMessageCount = unsubscribeAfter;
        this.ackAfterEach = ackAfterEach;
        this.rollbackPerMessagecount = rollbackPerMessageCount;
        this.commitPerMessageCount = commitPerMessageCount;

    }

    public void onMessage(Message message) {
        messageCount.incrementAndGet();
        localMessageCount++;
        TextMessage receivedMessage = (TextMessage) message;
        try {
            String redelivery = "";
            if(message.getJMSRedelivered()) {
                redelivery = "REDELIVERED";
            }  else {
                redelivery = "ORIGINAL";
            }

            if(messageCount.get() % printNumberOfMessagesPer == 0) {
                System.out.println("[TOPIC RECEIVE] ThreadID:"+Thread.currentThread().getId()+" topic:"+topicName+" localMessageCount:"+localMessageCount+" totalMessageCount:" + messageCount.get() + " max count:" + stopMessageCount );
            }
            if(isToPrintEachMessage) {
                System.out.println("(count:"+messageCount.get()+"/threadID:"+Thread.currentThread().getId()+"/topic:"+topicName+") " + redelivery + " >> " + receivedMessage.getText());
                AndesClientUtils.writeToFile(receivedMessage.getText(), fileToWriteReceivedMessages);
            }

            if(messageCount.get() % ackAfterEach == 0) {
                if(topicSession.getAcknowledgeMode() == QueueSession.CLIENT_ACKNOWLEDGE) {
                    receivedMessage.acknowledge();
                    System.out.println("****Acked message***");
                }
            }

            //commit get priority
            if(messageCount.get() % commitPerMessageCount == 0) {
                topicSession.commit();
                System.out.println("Committed Topic Session");
            } else if(messageCount.get() % rollbackPerMessagecount == 0) {
                topicSession.rollback();
                System.out.println("Rollbacked Topic Session");
            }

            if(messageCount.get() >= unsubscribeMessageCount) {
                unsubscribeConsumer();
                AndesClientUtils.sleepForInterval(200);
            } else if(messageCount.get() >= stopMessageCount) {
                stopMessageListener();
                AndesClientUtils.sleepForInterval(200);
            }

            if(delayBetweenMessages != 0) {
                try {
                    Thread.sleep(delayBetweenMessages);
                } catch (InterruptedException e) {
                    //silently ignore
                }
            }
        } catch (NumberFormatException e) {
            System.out.println("Wrong inputs." + e);
        } catch (JMSException e) {
            System.out.println("JMS Exception" + e);
        }
    }

    public AtomicInteger getMessageCount() {
        return messageCount;
    }

    public void setToRollbackSessionAtMessageCount(int messageCount) {
        rollbackPerMessagecount = messageCount;
    }

    public void setToCommitAtMessageCount(int messageCount) {
        commitPerMessageCount = messageCount;
    }

    public void setToStopAtMessageCount(int stopMessageCount) {
        this.stopMessageCount = stopMessageCount;
    }

    public void setToUnsubscribeAtMessageCount(int messageCount) {
        this.unsubscribeMessageCount = messageCount;
    }

    public void resetMessageCount() {
        this.messageCount.set(0);
    }

    public void stopMessageListener() {

        new Thread(new Runnable() {
            public void run() {
                try {
                    System.out.println("Closing subscriber");
                    topicReceiver.close();
                    topicSession.close();
                    topicConnection.stop();
                    topicConnection.close();
                } catch (JMSException e) {
                    System.out.println("Error in closing the queue subscriber" + e);
                }
            }
        }).start();

    }

    public void unsubscribeConsumer() {


        new Thread(new Runnable() {
            public void run() {
                try {
                    System.out.println("unSubscribing Subscriber");
                    topicSession.unsubscribe(subscriptionId);
                    topicReceiver.close();
                    topicSession.close();
                    topicConnection.close();

                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
