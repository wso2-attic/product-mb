/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.mb.platform.tests.clustering;

import com.google.common.net.HostAndPort;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.andes.stub.AndesAdminServiceBrokerManagerAdminException;
import org.wso2.carbon.authenticator.stub.LoginAuthenticationExceptionException;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.integration.common.utils.exceptions.AutomationUtilException;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.clients.AndesAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientConfigurationException;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.clients.operations.utils.JMSMessageType;
import org.wso2.mb.platform.common.utils.MBPlatformBaseTest;
import org.xml.sax.SAXException;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.xml.stream.XMLStreamException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.rmi.RemoteException;


/**
 * This class includes test cases to test different types of messages (e.g. byte, map, object,
 * stream) which can be sent to a topic.
 */
public class DifferentMessageTypesQueueTestCase extends MBPlatformBaseTest {

    /**
     * Prepare environment for tests.
     *
     * @throws LoginAuthenticationExceptionException
     * @throws IOException
     * @throws XPathExpressionException
     * @throws URISyntaxException
     * @throws SAXException
     * @throws XMLStreamException
     */
    @BeforeClass(alwaysRun = true)
    public void init()
            throws LoginAuthenticationExceptionException, IOException, XPathExpressionException,
            URISyntaxException, SAXException, XMLStreamException, AutomationUtilException {
        super.initCluster(TestUserMode.SUPER_TENANT_ADMIN);
        super.initAndesAdminClients();
    }

    /**
     * Publish byte messages to a queue in a single node and receive from the same node with one
     * subscriber
     *
     * @throws XPathExpressionException
     * @throws AndesClientConfigurationException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     * @throws AndesClientException
     */
    @Test(groups = "wso2.mb", description = "single publisher single subscriber byte messages",
            enabled = true)
    public void testByteMessageSingleSubSinglePub()
            throws XPathExpressionException, AndesClientConfigurationException, NamingException,
                   JMSException,
                   IOException, AndesClientException {

        this.runMessageTypeTestCase(JMSMessageType.BYTE, 1, "byteMessageQueue1");
    }

    /**
     * Publish byte messages to a queue in a single node and receive from the same node with
     * multiple publishers and subscribe to that queue using multiple subscribers
     *
     * @throws IOException
     * @throws JMSException
     * @throws AndesClientConfigurationException
     * @throws XPathExpressionException
     * @throws NamingException
     * @throws AndesClientException
     */
    @Test(groups = "wso2.mb", description = "multiple publisher multiple subscriber byte " +
                                            "messages", enabled = true)
    public void testByteMessageMultipleSubMultiplePub()
            throws IOException, JMSException, AndesClientConfigurationException,
                   XPathExpressionException,
                   NamingException, AndesClientException {

        this.runMessageTypeTestCase(JMSMessageType.BYTE, 10, "byteMessageQueue2");
    }

    /**
     * Publish map messages to a queue in a single node and receive from the same node with one
     * subscriber
     *
     * @throws IOException
     * @throws JMSException
     * @throws AndesClientConfigurationException
     * @throws XPathExpressionException
     * @throws NamingException
     * @throws AndesClientException
     */
    @Test(groups = "wso2.mb", description = "single publisher single subscriber map messages",
            enabled = true)
    public void testMapMessageSingleSubSinglePub()
            throws IOException, JMSException, AndesClientConfigurationException,
                   XPathExpressionException,
                   NamingException, AndesClientException {
        this.runMessageTypeTestCase(JMSMessageType.MAP, 1, "mapMessageQueue1");
    }

    /**
     * Publish map messages to a queue in a single node and receive from the same node with
     * multiple publishers and subscribe to that queue using multiple subscribers
     *
     * @throws IOException
     * @throws JMSException
     * @throws AndesClientConfigurationException
     * @throws XPathExpressionException
     * @throws NamingException
     * @throws AndesClientException
     */
    @Test(groups = "wso2.mb", description = "multiple publisher multiple subscriber map " +
                                            "messages", enabled = true)
    public void testMapMessageMultiplePubMultipleSub()
            throws IOException, JMSException, AndesClientConfigurationException,
                   XPathExpressionException,
                   NamingException, AndesClientException {
        this.runMessageTypeTestCase(JMSMessageType.MAP, 10, "mapMessageQueue2");
    }

    /**
     * Publish Object messages to a queue in a single node and receive from the same node with one
     * subscriber
     *
     * @throws IOException
     * @throws JMSException
     * @throws AndesClientConfigurationException
     * @throws XPathExpressionException
     * @throws NamingException
     * @throws AndesClientException
     */
    @Test(groups = "wso2.mb", description = "single publisher single subscriber object messages",
            enabled = true)
    public void testObjectMessageSingleSubSinglePub()
            throws IOException, JMSException, AndesClientConfigurationException,
                   XPathExpressionException,
                   NamingException, AndesClientException {
        this.runMessageTypeTestCase(JMSMessageType.OBJECT, 1, "objectMessageQueue1");
    }

    /**
     * Publish object messages to a queue in a single node and receive from the same node with
     * multiple publishers and subscribe to that queue using multiple subscribers
     * @throws IOException
     * @throws JMSException
     * @throws AndesClientConfigurationException
     * @throws XPathExpressionException
     * @throws NamingException
     * @throws AndesClientException
     */
    @Test(groups = "wso2.mb", description = "multiple publisher multiple subscriber object " +
                                            "messages", enabled = true)
    public void testObjectMessageMultiplePubMultipleSub()
            throws IOException, JMSException, AndesClientConfigurationException,
                   XPathExpressionException,
                   NamingException, AndesClientException {
        this.runMessageTypeTestCase(JMSMessageType.OBJECT, 10, "objectMessageQueue2");
    }

    /**
     * Publish stream messages to a queue in a single node and receive from the same node with one
     * subscriber
     *
     * @throws IOException
     * @throws JMSException
     * @throws AndesClientConfigurationException
     * @throws XPathExpressionException
     * @throws NamingException
     * @throws AndesClientException
     */
    @Test(groups = "wso2.mb", description = "single publisher single subscriber stream messages",
            enabled = true)
    public void testStreamMessageSingleSubSinglePub()
            throws IOException, JMSException, AndesClientConfigurationException,
                   XPathExpressionException,
                   NamingException, AndesClientException {
        this.runMessageTypeTestCase(JMSMessageType.STREAM, 1, "streamMessageQueue1");
    }

    /**
     * Publish stream messages to a queue in a single node and receive from the same node with
     * multiple publishers and subscribe to that queue using multiple subscribers
     *
     * @throws IOException
     * @throws JMSException
     * @throws AndesClientConfigurationException
     * @throws XPathExpressionException
     * @throws NamingException
     * @throws AndesClientException
     */
    @Test(groups = "wso2.mb", description = "multiple publisher multiple subscriber stream " +
                                            "messages", enabled = true)
    public void testStreamMessageMultiplePubMultipleSub()
            throws IOException, JMSException, AndesClientConfigurationException,
                   XPathExpressionException,
                   NamingException, AndesClientException {
        this.runMessageTypeTestCase(JMSMessageType.STREAM, 10, "streamMessageQueue2");
    }

    /**
     * Cleanup after running tests.
     *
     * @throws AndesAdminServiceBrokerManagerAdminException
     * @throws RemoteException
     */
    @AfterClass(alwaysRun = true)
    public void destroy() throws AndesAdminServiceBrokerManagerAdminException, RemoteException {

        String randomInstanceKey = getRandomMBInstance();

        AndesAdminClient tempAndesAdminClient = getAndesAdminClientWithKey(randomInstanceKey);

        if (tempAndesAdminClient.getQueueByName("byteMessageQueue1") != null) {
            tempAndesAdminClient.deleteQueue("byteMessageQueue1");
        }
        if (tempAndesAdminClient.getQueueByName("byteMessageQueue2") != null) {
            tempAndesAdminClient.deleteQueue("byteMessageQueue2");
        }
        if (tempAndesAdminClient.getQueueByName("mapMessageQueue1") != null) {
            tempAndesAdminClient.deleteQueue("mapMessageQueue1");
        }
        if (tempAndesAdminClient.getQueueByName("mapMessageQueue2") != null) {
            tempAndesAdminClient.deleteQueue("mapMessageQueue2");
        }
        if (tempAndesAdminClient.getQueueByName("objectMessageQueue1") != null) {
            tempAndesAdminClient.deleteQueue("objectMessageQueue1");
        }
        if (tempAndesAdminClient.getQueueByName("objectMessageQueue2") != null) {
            tempAndesAdminClient.deleteQueue("objectMessageQueue2");
        }
        if (tempAndesAdminClient.getQueueByName("streamMessageQueue1") != null) {
            tempAndesAdminClient.deleteQueue("streamMessageQueue1");
        }
        if (tempAndesAdminClient.getQueueByName("streamMessageQueue2") != null) {
            tempAndesAdminClient.deleteQueue("streamMessageQueue2");
        }
    }

    /**
     * Runs a topic send and receive test case
     *
     * @param messageType        The message type to be used when publishing
     * @param numberOfPublishers The number of publishers
     * @param destinationName    The destination name for sender and receiver
     * @throws XPathExpressionException
     * @throws AndesClientConfigurationException
     * @throws NamingException
     * @throws JMSException
     * @throws IOException
     * @throws AndesClientException
     */
    private void runMessageTypeTestCase(JMSMessageType messageType, int numberOfPublishers,
                                        String destinationName)
            throws XPathExpressionException, AndesClientConfigurationException, NamingException,
                   JMSException,
                   IOException, AndesClientException {

        // Number of messages send
        long sendCount = 2000L;

        HostAndPort brokerAddress = getRandomAMQPBrokerAddress();

        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig =
                new AndesJMSConsumerClientConfiguration(brokerAddress.getHostText(),
                                    brokerAddress.getPort(), ExchangeType.QUEUE, destinationName);
        consumerConfig.setPrintsPerMessageCount(sendCount / 10L);

        // Creating publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig =
                new AndesJMSPublisherClientConfiguration(brokerAddress.getHostText(),
                                     brokerAddress.getPort(), ExchangeType.QUEUE, destinationName);
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);
        publisherConfig.setJMSMessageType(messageType);

        // Creating clients
        AndesClient consumerClient = new AndesClient(consumerConfig, true);
        consumerClient.startClient();

        AndesClient publisherClient = new AndesClient(publisherConfig, numberOfPublishers, true);
        publisherClient.startClient();

        AndesClientUtils
                .waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

        // Evaluating
        Assert.assertEquals(publisherClient
                            .getSentMessageCount(), sendCount * numberOfPublishers, "Message sending failed.");
        Assert.assertEquals(consumerClient
                            .getReceivedMessageCount(), sendCount * numberOfPublishers, "Message receiving failed.");
    }
}
