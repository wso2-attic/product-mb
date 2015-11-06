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

package org.sample.jms;

import javax.jms.JMSException;
import javax.jms.Message;
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

public class SampleHierarchicalTopicsClient extends Thread{
    public static final String QPID_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private static final String CF_NAME_PREFIX = "connectionfactory.";
    private static final String CF_NAME = "qpidConnectionfactory";
    String userName = "admin";
    String password = "admin";
    private static String CARBON_CLIENT_ID = "carbon";
    private static String CARBON_VIRTUAL_HOST_NAME = "carbon";
    private static String CARBON_DEFAULT_HOSTNAME = "localhost";
    private static String CARBON_DEFAULT_PORT = "5672";
    String topicName_1 = "Games";
    String topicName_2 = "Games.Cricket";
    String topicName_3 = "Games.Cricket.SL";
    String topicName_4 = "Games.Cricket.India";
    String topicName_5 = "Games.Cricket.India.Delhi";
    String topicName_6 = "Games.Cricket.*";
    String topicName_7 = "Games.Cricket.#";
    private boolean isSubscriptionComplete = false;

    @Override
    public void run() {
        try {
            subscribe();
        } catch (NamingException e) {
            e.printStackTrace();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void subscribe() throws NamingException, JMSException {
        InitialContext ctx = init();
        // Lookup connection factory
        TopicConnectionFactory connFactory = (TopicConnectionFactory) ctx.lookup(CF_NAME);
        TopicConnection topicConnection = connFactory.createTopicConnection();
        topicConnection.start();

        //Create two topic sessions since a number of clients cannot be connected from the same session
        TopicSession topicSession1 =
                topicConnection.createTopicSession(false, QueueSession.AUTO_ACKNOWLEDGE);
        TopicSession topicSession2 =
                topicConnection.createTopicSession(false, QueueSession.AUTO_ACKNOWLEDGE);

        Topic topic1 = topicSession1.createTopic(topicName_1);
        Topic topic2 = topicSession1.createTopic(topicName_2);
        Topic topic3 = topicSession1.createTopic(topicName_3);
        Topic topic4 = topicSession1.createTopic(topicName_4);
        Topic topic5 = topicSession1.createTopic(topicName_5);
        Topic topic6 = (Topic) ctx.lookup(topicName_6);
        Topic topic7 = (Topic) ctx.lookup(topicName_7);
        TopicSubscriber topicSubscriber1 = topicSession1.createSubscriber(topic6);
        TopicSubscriber topicSubscriber2 = topicSession2.createSubscriber(topic7);

        isSubscriptionComplete = true;
        // Receive messages
        Message message1;
	System.out.println(" Receiving messages for " + topicName_6 + " :");
        while ((message1 = topicSubscriber1.receive(5000)) != null){
            if (message1 instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message1;
                System.out.println("Got Message from subscriber1 => " + textMessage.getText());
            }
        }
        Message message2;
	System.out.println(" Receiving messages for " + topicName_7 + " :");
        while ((message2 = topicSubscriber2.receive(5000)) != null){
            if (message2 instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message2;
                System.out.println("Got Message from subscriber2 => " + textMessage.getText());
            }
        }
        topicSubscriber1.close();
        topicSubscriber2.close();
        topicSession1.close();
        topicSession2.close();
        topicConnection.stop();
        topicConnection.close();
    }

    private InitialContext init() throws NamingException {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, QPID_ICF);
        properties.put(CF_NAME_PREFIX + CF_NAME, getTCPConnectionURL(userName, password));
        properties.put("topic."+topicName_6,topicName_6);
        properties.put("topic."+topicName_7,topicName_7);
        return new InitialContext(properties);
    }

    private String getTCPConnectionURL(String username, String password) {
        // amqp://{username}:{password}@carbon/carbon?brokerlist='tcp://{hostname}:{port}'
        return new StringBuffer()
                .append("amqp://").append(username).append(":").append(password)
                .append("@").append(CARBON_CLIENT_ID)
                .append("/").append(CARBON_VIRTUAL_HOST_NAME)
                .append("?brokerlist='tcp://").append(CARBON_DEFAULT_HOSTNAME).append(":").append(CARBON_DEFAULT_PORT).append("'")
                .toString();
    }

    public boolean isSubscriptionComplete(){
        return this.isSubscriptionComplete;
    }
}

