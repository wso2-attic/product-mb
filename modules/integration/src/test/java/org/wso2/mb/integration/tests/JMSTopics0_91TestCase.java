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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.Test;
import org.wso2.andes.configuration.ClientProperties;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

public class JMSTopics0_91TestCase {

    private static final Log log = LogFactory.getLog(JMSTopics0_91TestCase.class);
    private String topicName = "SimpleStockQuoteService";
    private String initialContextFactory = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    private String connectionString = "amqp://admin:admin@clientID/carbon?brokerlist='tcp://localhost:5672'";

    @Test(groups = {"wso2.mb"})
    public void subscribeWithTopicLookup() {

        Properties properties = new Properties();
        TopicConnection topicConnection = null;
        properties.put("java.naming.factory.initial", initialContextFactory);
        properties.put("connectionfactory.QueueConnectionFactory", connectionString);
        properties.put("topic.SimpleStockQuoteService", topicName);

        try {
            InitialContext ctx = new InitialContext(properties);
            TopicConnectionFactory topicConnectionFactory =
                    (TopicConnectionFactory) ctx.lookup("QueueConnectionFactory");
            topicConnection = topicConnectionFactory.createTopicConnection();
            TopicSession topicSession =
                    topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);


            Topic topic = (Topic) ctx.lookup(topicName);

            // create a topic subscriber
            TopicSubscriber topicSubscriber = topicSession.createSubscriber(topic);

            // start the connection
            topicConnection.start();

            log.info("Created the topic subscriber for the topic : " + topicName);

            TestMessageListener testMessageListener = new TestMessageListener();
            topicSubscriber.setMessageListener(testMessageListener);

            log.info("Listener registered for the topic : " + topicName);
            publishWithTopicLookup();
            try {
                //thread to sleep for the specified number of milliseconds
                Thread.sleep(10 * 1000);
            } catch (Exception e) {
                throw new RuntimeException("Error in subscriber thread sleep", e);
            }

            topicSubscriber.close();
            topicSession.close();
            topicConnection.close();

            log.info("Topic connection closed successfully");

        } catch (NamingException e) {
            throw new RuntimeException("Error in initial context lookup", e);
        } catch (JMSException e) {
            throw new RuntimeException("Error in JMS operations", e);
        } finally {
            if (topicConnection != null) {
                try {
                    topicConnection.close();
                } catch (JMSException e) {
                    throw new RuntimeException("Error in closing topic connection", e);
                }
            }
        }
    }

    private void publishWithTopicLookup() {
        Properties properties = new Properties();
        TopicConnection topicConnection = null;
        properties.put("java.naming.factory.initial", initialContextFactory);
        properties.put("connectionfactory.QueueConnectionFactory", connectionString);
        properties.put("topic.SimpleStockQuoteService", topicName);

        try {
            // initialize
            // the required connection factories
            InitialContext ctx = new InitialContext(properties);
            TopicConnectionFactory topicConnectionFactory =
                    (TopicConnectionFactory) ctx.lookup("QueueConnectionFactory");
            topicConnection = topicConnectionFactory.createTopicConnection();
            TopicSession topicSession =
                    topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            System.setProperty(ClientProperties.AMQP_VERSION, "0-91");
            String msg = "<ser:placeOrder xmlns:ser=\"http://services.samples\" xmlns:xsd=\"http://services.samples/xsd\">\n" +
                    "         <ser:order>\n" +
                    "            <xsd:price>120</xsd:price>\n" +
                    "            <xsd:quantity>100</xsd:quantity>\n" +
                    "            <xsd:symbol>WSO2</xsd:symbol>\n" +
                    "         </ser:order>\n" +
                    "      </ser:placeOrder>";
            // create or use the topic
            Topic topic = (Topic) ctx.lookup(topicName);
            TopicPublisher topicPublisher = topicSession.createPublisher(topic);

            log.info("Created a topic publisher for the topic : " + topicName);
            TextMessage textMessage = topicSession.createTextMessage(msg);

            // publish the message

            topicPublisher.publish(textMessage);
            log.info("Published the message for the topic successfully");
            topicPublisher.close();
            topicSession.close();
            topicConnection.close();


        } catch (JMSException e) {
            throw new RuntimeException("Error in JMS operations", e);
        } catch (NamingException e) {
            throw new RuntimeException("Error in initial context lookup", e);
        }
    }

    private static class TestMessageListener implements MessageListener {
        public void onMessage(Message message) {
            try {
                System.out.println("Received Message = " + ((TextMessage) message).getText());
            } catch (JMSException e) {
                throw new RuntimeException("Error in receiving JMS messages", e);
            }
        }
    }
}
