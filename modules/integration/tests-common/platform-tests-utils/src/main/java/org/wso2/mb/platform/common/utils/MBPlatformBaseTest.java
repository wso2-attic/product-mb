/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
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

package org.wso2.mb.platform.common.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.authenticator.stub.LoginAuthenticationExceptionException;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.engine.context.beans.Instance;
import org.wso2.carbon.automation.test.utils.axis2client.ConfigurationContextProvider;
import org.wso2.carbon.integration.common.utils.LoginLogoutClient;
import org.wso2.mb.integration.common.clients.operations.clients.AndesAdminClient;
import org.wso2.mb.integration.common.clients.operations.clients.TopicAdminClient;
import org.xml.sax.SAXException;

import javax.xml.stream.XMLStreamException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Base class of all MB integration tests
 */
public class MBPlatformBaseTest {

    protected Log log = LogFactory.getLog(MBPlatformBaseTest.class);
    protected Map<String, AutomationContext> contextMap;
    protected Map<String, AndesAdminClient> andesAdminClients;
    protected Map<String, TopicAdminClient> topicAdminClients;
    Stack stack = null;

    /**
     * create automationcontext objects for every node in config
     * @param userMode
     * @throws XPathExpressionException
     */

    protected void initCluster(TestUserMode userMode) throws XPathExpressionException {

        contextMap = new HashMap<String, AutomationContext>();

        AutomationContext automationContext1 = new AutomationContext("MB_Cluster", userMode);
        log.info("Cluster instance loading");
        Map<String, Instance> instanceMap = automationContext1.getProductGroup().getInstanceMap();

        if (instanceMap != null && instanceMap.size() > 0) {
            for (Map.Entry<String, Instance> entry : instanceMap.entrySet()) {
                String instanceKey = entry.getKey();
                contextMap.put(instanceKey, new AutomationContext("MB_Cluster", instanceKey, userMode));
                log.info(instanceKey);
            }
        }

        stack = new Stack();

    }

    /**
     * get automation context object with given node key
     * @param key
     * @return
     */

    protected AutomationContext getAutomationContextWithKey(String key) {

        if (contextMap != null && contextMap.size() > 0) {
            for (Map.Entry<String, AutomationContext> entry : contextMap.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(key)) {
                    return entry.getValue();
                }
            }
        }

        return null;
    }

    /**
     * get andes admin client for given node
     * @param key
     * @return
     */

    protected AndesAdminClient getAndesAdminClientWithKey(String key) {

        if (andesAdminClients != null && andesAdminClients.size() > 0) {
            for (Map.Entry<String, AndesAdminClient> entry : andesAdminClients.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(key)) {
                    return entry.getValue();
                }
            }
        }

        return null;
    }

    /**
     * get topic admin client for given node
     * @param key
     * @return
     */

    protected TopicAdminClient getTopicAdminClientWithKey(String key) {

        if (topicAdminClients != null && topicAdminClients.size() > 0) {
            for (Map.Entry<String, TopicAdminClient> entry : topicAdminClients.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(key)) {
                    return entry.getValue();
                }
            }
        }

        return null;
    }

    /**
     * login and provide session cookie for node
     * @param context
     * @return
     * @throws IOException
     * @throws XPathExpressionException
     * @throws URISyntaxException
     * @throws SAXException
     * @throws XMLStreamException
     * @throws LoginAuthenticationExceptionException
     */

    protected String login(AutomationContext context)
            throws IOException, XPathExpressionException, URISyntaxException, SAXException,
            XMLStreamException, LoginAuthenticationExceptionException {
        LoginLogoutClient loginLogoutClient = new LoginLogoutClient(context);
        return loginLogoutClient.login();
    }

    /**
     * make MB instances in random mode to support pick a random instance for test cases
     */

    protected void makeMBInstancesRandom() {

        Object[] keys = contextMap.keySet().toArray();

        List<Object> list = new ArrayList<Object>();

        for (Object object : keys) {
            list.add(object);
        }

        Collections.shuffle(list);

        for (int i = 0; i < list.size(); i++) {
            keys[i] = list.get(i);
            stack.push(list.get(i).toString());
        }

    }

    /**
     *
     * @return random instance key of the MB node
     */
    protected String getRandomMBInstance() {

        if (stack.empty()) {
            makeMBInstancesRandom();
        }

        return stack.pop().toString();
    }

    /**
     * create and login andes admin client to nodes in cluster
     * @throws Exception
     */

    protected void initAndesAdminClients() throws Exception {

        andesAdminClients = new HashMap<String, AndesAdminClient>();

        if (contextMap != null && contextMap.size() > 0) {
            for (Map.Entry<String, AutomationContext> entry : contextMap.entrySet()) {
                AutomationContext tempContext = entry.getValue();
                andesAdminClients.put(entry.getKey(),
                        new AndesAdminClient(tempContext.getContextUrls().getBackEndUrl(),
                                login(tempContext),
                                ConfigurationContextProvider.getInstance().getConfigurationContext()));
            }
        }
    }

    /**
     * create and login topic admin client to nodes in cluster
     * @throws Exception
     */

    protected void initTopicAdminClients() throws Exception {

        topicAdminClients = new HashMap<String, TopicAdminClient>();

        if (contextMap != null && contextMap.size() > 0) {
            for (Map.Entry<String, AutomationContext> entry : contextMap.entrySet()) {
                AutomationContext tempContext = entry.getValue();
                topicAdminClients.put(entry.getKey(),
                        new TopicAdminClient(tempContext.getContextUrls().getBackEndUrl(),
                                login(tempContext),
                                ConfigurationContextProvider.getInstance().getConfigurationContext()));
            }
        }
    }

    /**
     * check whether given queue is deleted from the cluster nodes
     * @param queue
     * @return success status
     * @throws Exception
     */

    protected boolean isQueueDeletedFromCluster(String queue) throws Exception {
        AndesAdminClient andesAdminClient;
        boolean bDeleted = true;
        if (andesAdminClients != null && andesAdminClients.size() > 0) {
            for (Map.Entry<String, AndesAdminClient> entry : andesAdminClients.entrySet()) {
                andesAdminClient = entry.getValue();

                if (andesAdminClient.getQueueByName(queue) != null) {
                    bDeleted = false;
                }

            }
        }

        return bDeleted;
    }

    /**
     * check whether given queue is created in the cluster nodes
     * @param queue
     * @return success status
     * @throws Exception
     */

    protected boolean isQueueCreatedInCluster(String queue) throws Exception{
        AndesAdminClient andesAdminClient;
        boolean bExist = true;
        if (andesAdminClients != null && andesAdminClients.size() > 0) {
            for (Map.Entry<String, AndesAdminClient> entry : andesAdminClients.entrySet()) {
                andesAdminClient = entry.getValue();

                if (andesAdminClient.getQueueByName(queue) == null) {
                    bExist = false;
                }

            }
        }

        return bExist;
    }

    /**
     * Give a random AMQP broker URL.
     *
     * @return Broker URL in host:port format (E.g "127.0.0.1:5672")
     * @throws XPathExpressionException
     */
    protected String getRandomAMQPBrokerUrl() throws XPathExpressionException {
        String randomInstanceKey = getRandomMBInstance();
        AutomationContext tempContext = getAutomationContextWithKey(randomInstanceKey);

        return tempContext.getInstance().getHosts().get("default") + ":" +
               tempContext.getInstance().getPorts().get("amqp");
    }

}
