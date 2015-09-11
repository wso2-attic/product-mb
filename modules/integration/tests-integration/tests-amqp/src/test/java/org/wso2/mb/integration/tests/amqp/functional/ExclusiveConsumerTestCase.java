package org.wso2.mb.integration.tests.amqp.functional;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.andes.stub.AndesAdminServiceBrokerManagerAdminException;
import org.wso2.carbon.authenticator.stub.LoginAuthenticationExceptionException;
import org.wso2.carbon.authenticator.stub.LogoutAuthenticationExceptionException;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.test.utils.axis2client.ConfigurationContextProvider;
import org.wso2.carbon.event.stub.internal.TopicManagerAdminServiceEventAdminExceptionException;
import org.wso2.carbon.integration.common.utils.LoginLogoutClient;
import org.wso2.carbon.user.mgt.stub.UserAdminUserAdminException;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientConfigurationException;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientException;
import org.wso2.mb.integration.common.clients.operations.clients.AndesAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationBaseTest;
import org.xml.sax.SAXException;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.xml.stream.XMLStreamException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.net.URISyntaxException;

public class ExclusiveConsumerTestCase extends MBIntegrationBaseTest {
    /**
     * Initializing test case
     *
     * @throws javax.xml.xpath.XPathExpressionException
     */
    @BeforeClass(alwaysRun = true)
    public void init() throws XPathExpressionException {
        super.init(TestUserMode.SUPER_TENANT_USER);
        AndesClientUtils.sleepForInterval(15000);
    }

    /**
     * 1. Create queue names "exlusiveQueue" with exclusive consumer feature enabled
     * 2. Create two subscribers for the "exclusiveQueue"
     * 3. Publish 1000 messages to queue.
     * 4. First consumer should receive all the 1000 messages.
     * 5. Second consumer should not receive any messages
     *
     * @throws org.wso2.mb.integration.common.clients.exceptions.AndesClientConfigurationException
     * @throws javax.jms.JMSException
     * @throws javax.naming.NamingException
     * @throws java.io.IOException
     * @throws org.wso2.mb.integration.common.clients.exceptions.AndesClientException
     */
    @Test(groups = "wso2.mb", description = "Exclusive Consumer test case")
    public void performSingleQueueSendReceiveTestCase()
            throws AndesClientConfigurationException, JMSException, NamingException, IOException,
                   AndesClientException, LoginAuthenticationExceptionException, URISyntaxException,
                   LogoutAuthenticationExceptionException, XMLStreamException,
                   AndesAdminServiceBrokerManagerAdminException, SAXException,
                   TopicManagerAdminServiceEventAdminExceptionException, XPathExpressionException,
                   UserAdminUserAdminException {
        long sendCount = 1000L;
        long expectedCount = 1000L;
        createQueueWithExclusiveConsumer("exlusiveQueue");
        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "exlusiveQueue");
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);
        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "exlusiveQueue");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);
        // Creating first client
        AndesClient consumerClient1 = new AndesClient(consumerConfig, true);
        consumerClient1.startClient();
        // Creating second client
        AndesClient consumerClient2 = new AndesClient(consumerConfig, true);
        consumerClient2.startClient();
        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();
        AndesClientUtils.waitForMessagesAndShutdown(consumerClient1, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitForMessagesAndShutdown(consumerClient2, AndesClientConstants.DEFAULT_RUN_TIME);
        // Evaluating
        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Error in sending all the messages.");
        Assert.assertEquals(consumerClient1.getReceivedMessageCount(), expectedCount, "Error in sending all the messages to the exclusive consumer.");
        Assert.assertEquals(consumerClient2.getReceivedMessageCount(), 0, "Error in sending messages to non-exclusive consumer.");
    }

    /**
     * 1. Create queue names "exlusiveQueue2"
     * 2. Create two subscribers for the "exlusiveQueue2"
     * 3. Publish 1000 messages to queue.
     * 4. First subscriber dies after receiving 400 messages
     * 5. Rest of the 600 messages, has to be sent to second subscriber
     *
     * @throws org.wso2.mb.integration.common.clients.exceptions.AndesClientConfigurationException
     * @throws javax.jms.JMSException
     * @throws javax.naming.NamingException
     * @throws java.io.IOException
     * @throws org.wso2.mb.integration.common.clients.exceptions.AndesClientException
     */
    @Test(groups = "wso2.mb", description = "Exclusive Consumer with Fail-over test case")
    public void performSingleQueueSendReceiveTestCase1()
            throws AndesClientConfigurationException, JMSException, NamingException, IOException,
                   AndesClientException, LoginAuthenticationExceptionException, URISyntaxException,
                   LogoutAuthenticationExceptionException, XMLStreamException,
                   AndesAdminServiceBrokerManagerAdminException, SAXException,
                   TopicManagerAdminServiceEventAdminExceptionException, XPathExpressionException,
                   UserAdminUserAdminException {
        long sendCount = 1000L;
        long expectedCount = 1000L;
        createQueueWithExclusiveConsumer("exlusiveQueue2");
        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "exlusiveQueue2");
        consumerConfig.setMaximumMessagesToReceived(400);
        consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);
        // Creating a consumer client configuration
        AndesJMSConsumerClientConfiguration consumerConfig2 = new AndesJMSConsumerClientConfiguration(ExchangeType.QUEUE, "exlusiveQueue2");
        //consumerConfig2.setMaximumMessagesToReceived(600);
        consumerConfig2.setPrintsPerMessageCount(expectedCount / 10L);
        // Creating a publisher client configuration
        AndesJMSPublisherClientConfiguration publisherConfig = new AndesJMSPublisherClientConfiguration(ExchangeType.QUEUE, "exlusiveQueue2");
        publisherConfig.setNumberOfMessagesToSend(sendCount);
        publisherConfig.setPrintsPerMessageCount(sendCount / 10L);
        // Creating clients
        AndesClient consumerClient1 = new AndesClient(consumerConfig, true);
        consumerClient1.startClient();
        // Creating clients
        AndesClient consumerClient2 = new AndesClient(consumerConfig2, true);
        consumerClient2.startClient();
        AndesClient publisherClient = new AndesClient(publisherConfig, true);
        publisherClient.startClient();
        AndesClientUtils.waitForMessagesAndShutdown(consumerClient1, AndesClientConstants.DEFAULT_RUN_TIME);
        AndesClientUtils.waitForMessagesAndShutdown(consumerClient2, AndesClientConstants.DEFAULT_RUN_TIME);
        // Evaluating
        Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount, "Error in sending all the messages.");
        Assert.assertEquals(consumerClient1.getReceivedMessageCount(), 400, "Error in sending messages to exclusive consumer until it goes down.");
        Assert.assertEquals(consumerClient2.getReceivedMessageCount(), 600, "Error in connection fail-over and sending all the rest of the messages to the next subscriber.");
    }

    /**
     * @param queueName name of the destination
     * @throws javax.xml.xpath.XPathExpressionException
     * @throws java.io.IOException
     * @throws java.net.URISyntaxException
     * @throws org.xml.sax.SAXException
     * @throws javax.xml.stream.XMLStreamException
     * @throws org.wso2.carbon.authenticator.stub.LoginAuthenticationExceptionException
     * @throws org.wso2.carbon.andes.stub.AndesAdminServiceBrokerManagerAdminException
     * @throws org.wso2.carbon.authenticator.stub.LogoutAuthenticationExceptionException
     * @throws org.wso2.carbon.user.mgt.stub.UserAdminUserAdminException
     * @throws org.wso2.carbon.event.stub.internal.TopicManagerAdminServiceEventAdminExceptionException
     */
    public void createQueueWithExclusiveConsumer(String queueName)
            throws XPathExpressionException, IOException, URISyntaxException, SAXException,
                   XMLStreamException, LoginAuthenticationExceptionException,
                   AndesAdminServiceBrokerManagerAdminException,
                   LogoutAuthenticationExceptionException,
                   UserAdminUserAdminException,
                   TopicManagerAdminServiceEventAdminExceptionException {
        LoginLogoutClient loginLogoutClientForUser = new LoginLogoutClient(super.automationContext);
        String sessionCookie = loginLogoutClientForUser.login();
        AndesAdminClient andesAdminClient = new AndesAdminClient(super.backendURL, sessionCookie, ConfigurationContextProvider
                .getInstance().getConfigurationContext());
        andesAdminClient.createQueue(queueName, true);
        loginLogoutClientForUser.logout();
    }
}