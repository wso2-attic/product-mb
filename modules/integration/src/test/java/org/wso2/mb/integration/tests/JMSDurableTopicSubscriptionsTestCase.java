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
package org.wso2.mb.integration.tests;

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.Test;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.QueueSession;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

public class JMSDurableTopicSubscriptionsTestCase {

    private static final Log log = LogFactory.getLog(JMSDurableTopicSubscriptionsTestCase.class);

    public static final String ANDES_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    private static final String CF_NAME = "andesConnectionfactory";
    String userName = "admin";
    String password = "admin";
    private String topicName = "newTopic";

    @Test(groups = {"wso2.mb"})
    public void subscribeDurably() {

        try {
            InitialContext ctx = init();
            String subscriptionId = "mySub0";

            /*Start the durable subscriber*/
            TopicConnectionFactory connFactory = (TopicConnectionFactory) ctx.lookup(CF_NAME);
            TopicConnection topicConnection = connFactory.createTopicConnection();
            topicConnection.start();
            TopicSession topicSession = topicConnection.createTopicSession(false, QueueSession.AUTO_ACKNOWLEDGE);

            // Create durable subscriber with subscription ID
            Topic topic = (Topic) ctx.lookup(topicName);
            TopicSubscriber topicSubscriber = topicSession.createDurableSubscriber(topic, subscriptionId);
            log.info("Created the topic subscriber_1 for the topic : " + topicName);
            TestMessageListener messageListener = new TestMessageListener();
            topicSubscriber.setMessageListener(messageListener);
            log.info("Listener registered for the topic : " + topicName);
            publishToTopic(5);
            Thread.sleep(5000);

            Assert.assertEquals(5,((TestMessageListener)topicSubscriber.getMessageListener()).getMsgCount());

            /*Stop the subscriber*/
            topicConnection.close();
            topicSession.close();
            topicSubscriber.close();
            log.info("Topic subscriber_1 closed successfully");
            publishToTopic(5);
            Thread.sleep(5000);

            /* Now restart the subscriber */
            TopicConnectionFactory connFactory2 = (TopicConnectionFactory) ctx.lookup(CF_NAME);
            TopicConnection topicConnection2 = connFactory2.createTopicConnection();
            topicConnection2.start();
            TopicSession topicSession2 = topicConnection2.createTopicSession(false, QueueSession.AUTO_ACKNOWLEDGE);

            Topic topic2 = (Topic) ctx.lookup(topicName);
            TopicSubscriber topicSubscriber2 = topicSession2.createDurableSubscriber(topic2, subscriptionId);
            log.info("Created the topic subscriber_2 for the topic : " + topicName);
            TestMessageListener messageListener2 = new TestMessageListener();
            topicSubscriber2.setMessageListener(messageListener2);
            log.info("Listener registered for the topic : " + topicName);

            /*Publish 5 more messages to the topic after subscriber came alive*/
            publishToTopic(5);

            /*Assert whether the subscriber has received all 10 messages when in online and offline*/
            Assert.assertEquals(10,((TestMessageListener)topicSubscriber2.getMessageListener()).getMsgCount());
            topicConnection2.close();
            topicSession2.close();
            topicSubscriber2.close();
            log.info("Topic subscriber_2 closed successfully");

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void publishToTopic(int numOfMsgs) throws NamingException, JMSException, InterruptedException {
        InitialContext initialContext = init();
        TopicConnectionFactory connFac = (TopicConnectionFactory) initialContext.lookup(CF_NAME);
        TopicConnection topicCon = connFac.createTopicConnection();
        topicCon.start();
        TopicSession topicSes =
                topicCon.createTopicSession(false, QueueSession.AUTO_ACKNOWLEDGE);

        Topic topic = (Topic)initialContext.lookup(topicName);
        // Create the messages to send
        TextMessage textMessage = topicSes.createTextMessage("Test Message");
        TopicPublisher topicPublisher = topicSes.createPublisher(topic);
        log.info("Sending " + numOfMsgs + " messages to Topic: " + topicName);
        for (int i = 0; i < numOfMsgs; i++)
        {
            topicPublisher.publish(textMessage);
            Thread.sleep(1000);
        }
        topicCon.close();
        topicSes.close();
        topicPublisher.close();
    }

    private InitialContext init() throws NamingException {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, ANDES_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        properties.put("topic." + topicName, topicName);
        return new InitialContext(properties);
    }

    private String getTCPConnectionURL(String username, String password) {
        String CARBON_CLIENT_ID = "carbon";
        String CARBON_DEFAULT_PORT = "5672";
        String CARBON_VIRTUAL_HOST_NAME = "carbon";
        String CARBON_DEFAULT_HOSTNAME = "localhost";
        return new StringBuffer()
                .append("amqp://").append(username).append(":").append(password)
                .append("@").append(CARBON_CLIENT_ID)
                .append("/").append(CARBON_VIRTUAL_HOST_NAME)
                .append("?brokerlist='tcp://").append(CARBON_DEFAULT_HOSTNAME).append(":").append(CARBON_DEFAULT_PORT).append("'")
                .toString();
    }

    private static class TestMessageListener implements MessageListener {

        int count = 0;
        public void onMessage(Message message) {
            count++;
            try {
                log.info("Received Message = " + count + " " + ((TextMessage) message).getText());
            } catch (JMSException e) {
                throw new RuntimeException("Error in receiving JMS messages", e);
            }
        }

        public int getMsgCount(){
            return count;
        }
    }
}
