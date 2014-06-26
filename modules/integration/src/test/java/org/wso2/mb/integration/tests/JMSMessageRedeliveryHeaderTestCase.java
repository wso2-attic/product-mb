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
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.andes.configuration.ClientProperties;
import org.wso2.carbon.automation.core.ProductConstant;
import org.wso2.carbon.automation.core.utils.environmentutils.EnvironmentBuilder;
import org.wso2.carbon.automation.core.utils.environmentutils.EnvironmentVariables;
import org.wso2.carbon.automation.core.utils.serverutils.ServerConfigurationManager;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

// TODO : This is not the correct
// TODO  : way to do this, since we have to wait a long time to this test, we need to inject the configuration on the fly.
public class JMSMessageRedeliveryHeaderTestCase {
    private static final Log log = LogFactory.getLog(JMSMessageRedeliveryHeaderTestCase.class);

    @BeforeClass
    public void oneTimeSetUp() throws Exception {
        // one-time initialization code
        log.info("JMSMessageRedelivery testcase started to execute ....");
    }

    @AfterClass
    public void oneTimeTearDown() {
        // one-time cleanup code
        log.info("JMSMessageRedelivery testcase finished executing ....");
    }

    @Test(groups = {"wso2.mb"})
    public void consumeMessage() {
        System.setProperty(ClientProperties.AMQP_VERSION, "0-91");
        Properties initialContextProperties = new Properties();
        initialContextProperties.put("java.naming.factory.initial",
                "org.wso2.andes.jndi.PropertiesFileInitialContextFactory");
        String connectionString = "amqp://admin:admin@clientID/carbon?brokerlist='tcp://localhost:5672'";
        initialContextProperties.put("connectionfactory.qpidConnectionfactory", connectionString);
        initialContextProperties.put("queue.test-q1", "test-q1");

        try {
            InitialContext initialContext = new InitialContext(initialContextProperties);
            ConnectionFactory queueConnectionFactory
                    = (ConnectionFactory) initialContext.lookup("qpidConnectionfactory");
            Connection queueConnection = queueConnectionFactory.createConnection();
            queueConnection.start();

            Session queueSession = queueConnection.createSession(false, QueueSession.CLIENT_ACKNOWLEDGE);
            Destination destination = (Destination) initialContext.lookup("test-q1");

            MessageConsumer messageConsumer = queueSession.createConsumer(destination);

            sendMessages();
            int count = 0;
            while (count <= 20) {
                Message m = messageConsumer.receive(1);
                if (m != null) {
                    if (m instanceof TextMessage) {
                        TextMessage message = (TextMessage) m;
                        ++count;
                        if(count > 10){
                            Assert.assertEquals(message.getJMSRedelivered(), true);
                        }
                    }
                }
            }

            messageConsumer.close();
            queueSession.close();
            queueConnection.stop();
            queueConnection.close();
        } catch (NamingException e) {
            throw new RuntimeException("Error in Naming", e);
        } catch (JMSException e) {
            throw new RuntimeException("Unable to start the consumer", e);
        }
    }

    public void sendMessages() {
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
        initialContextProperties.put("queue.test-q1", "test-q1");

        try {
            InitialContext initialContext = new InitialContext(initialContextProperties);
            connectionFactory = (ConnectionFactory) initialContext.lookup("qpidConnectionfactory");
            con = connectionFactory.createConnection();
            con.start();
            session = con.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Destination destination = (Destination) initialContext.lookup("test-q1");
            if (destination == null) {
                System.out.println("null");
                destination = session.createQueue("test-q1");
            }

            producer = session.createProducer(destination);

            log.info("Sending the messages");
            for (int i = 1; i < 11; i++) {
                String msg = "test";
                TextMessage textMessage = session.createTextMessage(msg);
                textMessage.setIntProperty("count", i);
                producer.send(textMessage);
            }
            con.close();
            session.close();
            producer.close();
        } catch (NamingException e) {
            Assert.fail(e.getMessage());
            throw new RuntimeException("Unable to send jms messages", e);
        } catch (JMSException e) {
            Assert.fail(e.getMessage());
            throw new RuntimeException("Unable to send jms messages", e);
        }
    }
}
