/*
*  Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.mb.platform.tests.clustering.slot;

import org.apache.commons.configuration.ConfigurationException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.carbon.andes.stub.AndesAdminServiceBrokerManagerAdminException;
import org.wso2.carbon.andes.stub.admin.types.Queue;
import org.wso2.carbon.authenticator.stub.LoginAuthenticationExceptionException;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.integration.common.utils.exceptions.AutomationUtilException;
import org.wso2.carbon.integration.common.utils.mgt.ServerConfigurationManager;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSConsumerClientConfiguration;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.clients.AndesAdminClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.ExchangeType;
import org.wso2.mb.integration.common.utils.backend.ConfigurationEditor;
import org.wso2.mb.platform.common.utils.MBPlatformBaseTest;
import org.xml.sax.SAXException;

import javax.xml.stream.XMLStreamException;
import javax.xml.xpath.XPathExpressionException;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.rmi.RemoteException;
import java.util.Collection;

import static org.testng.Assert.assertTrue;

/**
 * Test class to test message delivery using RDBMS slot implementation
 */

public class QueueClusterRdbmsSlotTestCase extends MBPlatformBaseTest {

	/**
	 * Set slot storage to RDBMS in all automation context instances
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
			       URISyntaxException, SAXException, XMLStreamException,
			       AndesAdminServiceBrokerManagerAdminException, AutomationUtilException,
			       ConfigurationException {

		super.initCluster(TestUserMode.SUPER_TENANT_ADMIN);

		// Changing slot storage to RDBMS in all instances
		Collection<AutomationContext> instances = getAllInstances();
		for (AutomationContext instance : instances) {
			ServerConfigurationManager publisherConfigurationManager =
					new ServerConfigurationManager(instance);
			String configurationPath =
					instance.getInstance().getProperty("carbon-home") + File.separator +
					"repository" + File.separator + "conf" + File.separator + "broker.xml";
			ConfigurationEditor configurationEditor = new ConfigurationEditor(configurationPath);
			configurationEditor.updateProperty(AndesConfiguration.SLOT_MANAGEMENT_STORAGE, "RDBMS");
			configurationEditor
					.applyUpdatedConfigurationAndRestartServer(publisherConfigurationManager);
		}

		super.initAndesAdminClients();
	}

	/**
	 * Test Case 01
	 * 1. Start 01 publisher and 01 consumer for queue slotTestQueue1 in single node
	 * 2. Publish 10000 messages to queue
	 * 3. Consumer should receive 10000 messages
	 * Test sending and receiving messages without failing in a single queue and single node using
	 * RDBMS slot information implementation
	 *
	 * @throws Exception
	 */
	@Test(groups = "wso2.mb", description = "Single queue Single node send-receive test case")
	public void testSingleQueueSingleNodeSendReceive()
			throws Exception {

		long sendCount = 10000L;
		long expectedCount = 10000L;

		String randomInstanceKey = getRandomMBInstance();

		AutomationContext tempContext = getAutomationContextWithKey(randomInstanceKey);

		AndesJMSConsumerClientConfiguration consumerConfig =
				new AndesJMSConsumerClientConfiguration(
						tempContext.getInstance().getHosts().get("default"),
						Integer.parseInt(tempContext.getInstance().getPorts().get("amqp")),
						ExchangeType.QUEUE, "slotTestQueue1");
		consumerConfig.setMaximumMessagesToReceived(expectedCount);
		consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);

		AndesJMSPublisherClientConfiguration publisherConfig =
				new AndesJMSPublisherClientConfiguration(
						tempContext.getInstance().getHosts().get("default"),
						Integer.parseInt(tempContext.getInstance().getPorts().get("amqp")),
						ExchangeType.QUEUE, "slotTestQueue1");

		publisherConfig.setNumberOfMessagesToSend(sendCount);
		publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

		AndesClient consumerClient = new AndesClient(consumerConfig, true);
		consumerClient.startClient();

		AndesClient publisherClient = new AndesClient(publisherConfig, true);
		publisherClient.startClient();

		Queue queue =
				getAndesAdminClientWithKey(randomInstanceKey).getQueueByName("slotTestQueue1");

		assertTrue(queue.getQueueName().equalsIgnoreCase("slotTestQueue1"),
		           "Queue created in MB node 1 not exist");

		AndesClientUtils
				.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

		Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount,
		                    "Message sending failed.");
		Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedCount,
		                    "Message receiving failed.");
	}

	/**
	 * Test Case 02
	 * 1. Start 01 publisher and 01 consumer for queue slotTestQueue2 in multi nodes
	 * 2. Publish 10000 messages to queue
	 * 3. Consumer should receive 10000 messages
	 * Test sending and receiving messages without failing in a single queue and multiple nodes
	 *  using RDBMS slot information implementation
	 *
	 * @throws Exception
	 */
	@Test(groups = "wso2.mb", description = "Single queue Single node send-receive test case")
	public void testSingleQueueMultiNodeSendReceive()
			throws Exception {
		long sendCount = 10000L;
		long expectedCount = 10000L;

		String randomInstanceKey = getRandomMBInstance();

		AutomationContext tempContext = getAutomationContextWithKey(randomInstanceKey);

		AndesJMSConsumerClientConfiguration consumerConfig =
				new AndesJMSConsumerClientConfiguration(
						tempContext.getInstance().getHosts().get("default"),
						Integer.parseInt(tempContext.getInstance().getPorts().get("amqp")),
						ExchangeType.QUEUE, "slotTestQueue2");
		consumerConfig.setMaximumMessagesToReceived(expectedCount);
		consumerConfig.setPrintsPerMessageCount(expectedCount / 10L);

		randomInstanceKey = getRandomMBInstance();
		tempContext = getAutomationContextWithKey(randomInstanceKey);

		AndesJMSPublisherClientConfiguration publisherConfig =
				new AndesJMSPublisherClientConfiguration(
						tempContext.getInstance().getHosts().get("default"),
						Integer.parseInt(tempContext.getInstance().getPorts().get("amqp")),
						ExchangeType.QUEUE, "slotTestQueue2");
		publisherConfig.setNumberOfMessagesToSend(sendCount);
		publisherConfig.setPrintsPerMessageCount(sendCount / 10L);

		AndesClient consumerClient = new AndesClient(consumerConfig, true);
		consumerClient.startClient();

		AndesClient publisherClient = new AndesClient(publisherConfig, true);
		publisherClient.startClient();

		AndesClientUtils
				.waitForMessagesAndShutdown(consumerClient, AndesClientConstants.DEFAULT_RUN_TIME);

		Assert.assertEquals(publisherClient.getSentMessageCount(), sendCount,
		                    "Message sending failed.");
		Assert.assertEquals(consumerClient.getReceivedMessageCount(), expectedCount,
		                    "Message receiving failed.");
	}

	/**
	 * Cleanup after running tests
	 *
	 * @throws AndesAdminServiceBrokerManagerAdminException
	 * @throws RemoteException
	 */
	@AfterClass(alwaysRun = true)
	public void destroy()
			throws AndesAdminServiceBrokerManagerAdminException, RemoteException {

		String randomInstanceKey = getRandomMBInstance();
		AndesAdminClient tempAndesAdminClient = getAndesAdminClientWithKey(randomInstanceKey);
		if (null != tempAndesAdminClient.getQueueByName("slotTestQueue1")) {
			tempAndesAdminClient.deleteQueue("slotTestQueue1");
		}
	}

}
