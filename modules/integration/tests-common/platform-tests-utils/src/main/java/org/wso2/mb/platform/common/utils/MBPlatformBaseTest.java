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

package org.wso2.mb.platform.common.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.authenticator.stub.LoginAuthenticationExceptionException;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;
import org.wso2.carbon.automation.engine.context.beans.Instance;
import org.wso2.carbon.automation.test.utils.axis2client.ConfigurationContextProvider;
import org.wso2.carbon.integration.common.utils.LoginLogoutClient;
import org.wso2.mb.integration.common.clients.operations.queue.AndesAdminClient;
import org.wso2.mb.integration.common.clients.operations.topic.TopicAdminClient;
import org.xml.sax.SAXException;

import javax.xml.stream.XMLStreamException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Base class of all MB integration tests
 */
public class MBPlatformBaseTest {

    protected Log log = LogFactory.getLog(MBPlatformBaseTest.class);
    protected AutomationContext automationContext;
    protected String backendURL;
    protected Map<String, AutomationContext> contextMap;
    protected Map<String, AndesAdminClient> andesAdminClients;
    protected Map<String, TopicAdminClient> topicAdminClients;

    protected void initCluster(TestUserMode userMode) throws XPathExpressionException {
        contextMap = new HashMap<String, AutomationContext>();

        AutomationContext automationContext1 = new AutomationContext("MB_Cluster", userMode);
        log.debug("Cluster instance loading");
        Map<String, Instance> instanceMap = automationContext1.getProductGroup().getInstanceMap();

        if (instanceMap != null && instanceMap.size() > 0) {
            for (Map.Entry<String, Instance> entry : instanceMap.entrySet()) {
                String instanceKey = entry.getKey();
                contextMap.put(instanceKey, new AutomationContext("MB_Cluster", instanceKey, userMode));
                log.debug(instanceKey);
            }
        }

    }

    protected AutomationContext getAutomationContextWithKey(String key) {

        if (contextMap != null && contextMap.size() > 0) {
            for (Map.Entry<String, AutomationContext> entry : contextMap.entrySet()) {
                if (entry.getKey().toString().equalsIgnoreCase(key)) {
                    return entry.getValue();
                }
            }
        }

        return null;
    }

    protected AndesAdminClient getAndesAdminClientWithKey(String key) {

        if (andesAdminClients != null && andesAdminClients.size() > 0) {
            for (Map.Entry<String, AndesAdminClient> entry : andesAdminClients.entrySet()) {
                if (entry.getKey().toString().equalsIgnoreCase(key)) {
                    return entry.getValue();
                }
            }
        }

        return null;
    }

    protected TopicAdminClient getTopicAdminClientWithKey(String key) {

        if (topicAdminClients != null && topicAdminClients.size() > 0) {
            for (Map.Entry<String, TopicAdminClient> entry : topicAdminClients.entrySet()) {
                if (entry.getKey().toString().equalsIgnoreCase(key)) {
                    return entry.getValue();
                }
            }
        }

        return null;
    }

    protected String login(AutomationContext context)
            throws IOException, XPathExpressionException, URISyntaxException, SAXException,
            XMLStreamException, LoginAuthenticationExceptionException {
        LoginLogoutClient loginLogoutClient = new LoginLogoutClient(context);
        return loginLogoutClient.login();
    }

    protected String getRandomInstance(String old) {
        Object[] keys = contextMap.keySet().toArray();
        Object key = keys[new Random().nextInt(keys.length)];
        int instanceLength = keys.length;
        int count = 0;
        String instance = key.toString();

        if (old != null && keys.length > 1) {
            while (true) {

                if (!old.equalsIgnoreCase(instance) || (count > (instanceLength*5))) {
                    break;
                }

                key = keys[new Random().nextInt(keys.length)];
                instance = contextMap.get(key).toString();
                count++;
            }
        }

        return instance;
    }

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

        return true;
    }

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

        return true;
    }

}