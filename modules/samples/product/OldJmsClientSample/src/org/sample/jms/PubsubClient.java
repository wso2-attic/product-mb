/*
 * Copyright 2004,2005 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sample.jms;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.jms.*;
import java.util.Properties;

public class PubsubClient {

    public void subscribe(String topicName) {

        Properties initialContextProperties = new Properties();
        initialContextProperties.put("java.naming.factory.initial",
                "org.wso2.andes.jndi.PropertiesFileInitialContextFactory");
        String connectionString = "amqp://admin:admin@clientID/carbon?brokerlist='tcp://localhost:5672'";
        initialContextProperties.put("connectionfactory.qpidConnectionfactory", connectionString);

        try {
            InitialContext initialContext = new InitialContext(initialContextProperties);
            TopicConnectionFactory topicConnectionFactory =
                    (TopicConnectionFactory) initialContext.lookup("qpidConnectionfactory");
            TopicConnection topicConnection = topicConnectionFactory.createTopicConnection();
            topicConnection.start();
            TopicSession topicSession =
                    topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

            Topic topic = topicSession.createTopic(topicName);
            TopicSubscriber topicSubscriber =
                    topicSession.createSubscriber(topic);

            topicSubscriber.setMessageListener(new SampleMessageListener(
                    topicConnection, topicSession, topicSubscriber));

        } catch (NamingException e) {
            e.printStackTrace();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void publish(String topicName, String message) {
        Properties initialContextProperties = new Properties();
        initialContextProperties.put("java.naming.factory.initial",
                "org.wso2.andes.jndi.PropertiesFileInitialContextFactory");
        String connectionString = "amqp://admin:admin@clientID/carbon?brokerlist='tcp://localhost:5672'";
        initialContextProperties.put("connectionfactory.qpidConnectionfactory", connectionString);

        try {
            InitialContext initialContext = new InitialContext(initialContextProperties);
            TopicConnectionFactory topicConnectionFactory =
                    (TopicConnectionFactory) initialContext.lookup("qpidConnectionfactory");
            TopicConnection topicConnection = topicConnectionFactory.createTopicConnection();
            topicConnection.start();
            TopicSession topicSession =
                    topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

            Topic topic = topicSession.createTopic(topicName);
            TopicPublisher topicPublisher = topicSession.createPublisher(topic);

            TextMessage textMessage =
                    topicSession.createTextMessage(message);

            topicPublisher.publish(textMessage);
            topicPublisher.close();


            topicSession.close();
            topicConnection.stop();
            topicConnection.close();

        } catch (NamingException e) {
            e.printStackTrace();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        PubsubClient pubsubClient = new PubsubClient();
        pubsubClient.subscribe("foo.bar");
        pubsubClient.publish("foo.bar", "My test message");

        //wait  some time until message is received by subscriber
        //and exit
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {}

        System.exit(0);
    }
}
