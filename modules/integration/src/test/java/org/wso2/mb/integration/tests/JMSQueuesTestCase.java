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

public class JMSQueuesTestCase {
    private static final Log log = LogFactory.getLog(JMSQueuesTestCase.class);


    @Test(groups = {"wso2.mb"})
    public void consumeMessage()  {

        System.setProperty(ClientProperties.AMQP_VERSION, "0-91");
        Properties initialContextProperties = new Properties();
        initialContextProperties.put("java.naming.factory.initial",
                "org.wso2.andes.jndi.PropertiesFileInitialContextFactory");
        String connectionString = "amqp://admin:admin@clientID/carbon?brokerlist='tcp://localhost:5672'";
        initialContextProperties.put("connectionfactory.qpidConnectionfactory", connectionString);
        initialContextProperties.put("queue.JMSProxyQueue2", "JMSProxyQueue2");


        try {
            InitialContext initialContext = new InitialContext(initialContextProperties);
            ConnectionFactory queueConnectionFactory
                    = (ConnectionFactory) initialContext.lookup("qpidConnectionfactory");
            Connection queueConnection = queueConnectionFactory.createConnection();
            queueConnection.start();

            Session queueSession = queueConnection.createSession(false, QueueSession.AUTO_ACKNOWLEDGE);
            Destination destination = (Destination) initialContext.lookup("JMSProxyQueue2");

            MessageConsumer messageConsumer = queueSession.createConsumer(destination);


            TestMessageListener testMessageListener = new TestMessageListener();
            messageConsumer.setMessageListener(testMessageListener);

            sendMessages();
            //thread to sleep for the specified number of milliseconds
            Thread.sleep(20 * 1000);

            messageConsumer.close();

            queueSession.close();
            queueConnection.stop();
            queueConnection.close();
        } catch (NamingException e) {
             throw new RuntimeException("Error in Naming", e);
        } catch (JMSException e) {
             throw new RuntimeException("Unable to start the consumer", e);
        } catch (InterruptedException e) {
             throw new RuntimeException("Unable to make the server thread sleep", e);
        }

    }

    public class TestMessageListener implements MessageListener {
        public void onMessage(Message message) {
            try {
                System.out.println("Received Message = " + ((TextMessage) message).getText());
            } catch (JMSException e) {
                throw new RuntimeException("Unable receive messages", e);
            }
        }
    }

    public void sendMessages()  {

        ConnectionFactory connectionFactory;
        Connection con = null;
        Session session = null;
        MessageProducer producer = null;

        System.setProperty(ClientProperties.AMQP_VERSION, "0-91");

        Properties initialContextProperties = new Properties();
        initialContextProperties.put("java.naming.factory.initial",
                "org.wso2.andes.jndi.PropertiesFileInitialContextFactory");
        String connectionString = "amqp://admin:admin@clientID/carbon?brokerlist='tcp://localhost:5672'";
        initialContextProperties.put("connectionfactory.qpidConnectionfactory", connectionString);
        initialContextProperties.put("queue.JMSProxyQueue2", "JMSProxyQueue2");


        try {
            InitialContext initialContext = new InitialContext(initialContextProperties);
            connectionFactory = (ConnectionFactory) initialContext.lookup("qpidConnectionfactory");
            con = connectionFactory.createConnection();
            con.start();
            session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = (Destination) initialContext.lookup("JMSProxyQueue2");
            if (destination == null) {
                System.out.println("null");
                destination = session.createQueue("JMSProxyQueue2");
            }

            producer = session.createProducer(destination);

            for (int i = 0; i < 10; i++) {

                String msg = "<ser:placeOrder xmlns:ser=\"http://services.samples\" xmlns:xsd=\"http://services.samples/xsd\">\n" +
                        "         <ser:order>\n" +
                        "            <xsd:price>120</xsd:price>\n" +
                        "            <xsd:quantity>100</xsd:quantity>\n" +
                        "            <xsd:symbol>WSO2</xsd:symbol>\n" +
                        "         </ser:order>\n" +
                        "      </ser:placeOrder> *******message # : " + i;


                TextMessage textMessage = session.createTextMessage(msg);
                producer.send(textMessage);
            }
            con.close();
            session.close();
            producer.close();
        } catch (NamingException e) {
             throw new RuntimeException("Unable to send jms messages", e);
        } catch (JMSException e) {
             throw new RuntimeException("Unable to send jms messages", e);
        }
    }

}
