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
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

public class JMSHierarchicalTopicsTestCase {
    private static final Log log = LogFactory.getLog(JMSHierarchicalTopicsTestCase.class);

    public static final String QPID_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    private static final String CF_NAME = "qpidConnectionfactory";
    String userName = "admin";
    String password = "admin";
    String topicName_1 = "WSO2";
    String topicName_2 = "WSO2.MB";
    String topicName_3 = "WSO2.*";
    String topicName_4 = "WSO2.#";

    @Test(groups = {"wso2.mb"})
    public void receiveMessages() throws NamingException, JMSException, InterruptedException {
        InitialContext ctx = init();
        // Lookup connection factory
        TopicConnectionFactory connFactory = (TopicConnectionFactory) ctx.lookup(CF_NAME);
        TopicConnection topicConnection = connFactory.createTopicConnection();
        topicConnection.start();
        TopicSession topicSession =
                topicConnection.createTopicSession(false, QueueSession.AUTO_ACKNOWLEDGE);

        Topic topic1 = topicSession.createTopic(topicName_1);
        Topic topic2 = topicSession.createTopic(topicName_2);
        Topic topic3 = (Topic) ctx.lookup(topicName_3);
        Topic topic4 = (Topic) ctx.lookup(topicName_4);
        TopicSubscriber topicSubscriber1 = topicSession.createSubscriber(topic3);
        TopicSubscriber topicSubscriber2 = topicSession.createSubscriber(topic4);
        topicSubscriber1.setMessageListener(new TestMessageListener("Subscriber_1"));
        topicSubscriber2.setMessageListener(new TestMessageListener("Subscriber_2"));

        publishMessages();
        Thread.sleep(2000);

        Assert.assertEquals(1, ((TestMessageListener)topicSubscriber1.getMessageListener()).getMsgCount());
        Assert.assertEquals(2, ((TestMessageListener)topicSubscriber2.getMessageListener()).getMsgCount());

        topicSubscriber1.close();
        topicSubscriber2.close();
        topicSession.close();
        topicConnection.stop();
        topicConnection.close();
        }


    public void publishMessages() throws JMSException, NamingException {
        InitialContext ctx = init();
        // Lookup connection factory
        TopicConnectionFactory connFactory = (TopicConnectionFactory) ctx.lookup(CF_NAME);
        TopicConnection topicConnection = connFactory.createTopicConnection();
        topicConnection.start();
        TopicSession topicSession =
                topicConnection.createTopicSession(false, QueueSession.AUTO_ACKNOWLEDGE);
        Topic topic1 = (Topic) ctx.lookup(topicName_1);
        Topic topic2 = (Topic) ctx.lookup(topicName_2);
        javax.jms.TopicPublisher topicPublisher1 = topicSession.createPublisher(topic1);
        javax.jms.TopicPublisher topicPublisher2 = topicSession.createPublisher(topic2);

        // Create the messages to send
        TextMessage textMessage1 = topicSession.createTextMessage("Message for WSO2");
        TextMessage textMessage2 = topicSession.createTextMessage("Message for WSO2.MB");
        topicPublisher1.publish(textMessage1);
        topicPublisher2.publish(textMessage2);
        topicSession.close();
        topicConnection.close();
    }

    private InitialContext init() throws NamingException {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        properties.put("topic."+topicName_1,topicName_1);
        properties.put("topic."+topicName_2,topicName_2);
        properties.put("topic."+topicName_3,topicName_3);
        properties.put("topic."+topicName_4, topicName_4);
        return new InitialContext(properties);
    }

    private String getTCPConnectionURL(String username, String password) {
        // amqp://{username}:{password}@carbon/carbon?brokerlist='tcp://{hostname}:{port}'

        String CARBON_DEFAULT_PORT = "5672";
        String CARBON_CLIENT_ID = "carbon";
        String CARBON_VIRTUAL_HOST_NAME = "carbon";
        String CARBON_DEFAULT_HOSTNAME = "localhost";
        return new StringBuffer()
                .append("amqp://").append(username).append(":").append(password)
                .append("@").append(CARBON_CLIENT_ID)
                .append("/").append(CARBON_VIRTUAL_HOST_NAME)
                .append("?brokerlist='tcp://").append(CARBON_DEFAULT_HOSTNAME).append(":").append(CARBON_DEFAULT_PORT).append("'")
                .toString();
    }

    public class TestMessageListener implements MessageListener {

        String subscriber;
        int msgCount = 0;

        public TestMessageListener(String subscriber) {
            this.subscriber = subscriber;
        }

        public void onMessage(Message message) {
            msgCount++;
            TextMessage textMessage = (TextMessage) message;
            try {
                log.info("Got Message from " + subscriber +" => " + textMessage.getText());
            } catch (JMSException e) {
                e.printStackTrace();
            }

        }

        public int getMsgCount(){
            return msgCount;
        }

    }

}
