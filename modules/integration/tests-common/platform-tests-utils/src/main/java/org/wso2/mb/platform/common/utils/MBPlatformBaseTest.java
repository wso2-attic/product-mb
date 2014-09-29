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
import org.wso2.carbon.integration.common.utils.LoginLogoutClient;
import org.xml.sax.SAXException;

import javax.xml.stream.XMLStreamException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Base class of all MB integration tests
 */
public class MBPlatformBaseTest {

    protected Log log = LogFactory.getLog(MBPlatformBaseTest.class);
    protected AutomationContext automationContext;
    protected String backendURL;
    protected Map<String, AutomationContext> contextMap;

    protected void initCluster(TestUserMode userMode) throws XPathExpressionException {
        contextMap = new HashMap<String, AutomationContext>();

        AutomationContext automationContext1 = new AutomationContext("MB_Cluster", userMode);

        Map<String, Instance> instanceMap = automationContext1.getProductGroup().getInstanceMap();

        if (instanceMap != null && instanceMap.size() > 0) {
            for (Map.Entry<String, Instance> entry : instanceMap.entrySet()) {
                System.out.println(entry.getKey() + " : " + entry.getValue());
                String instanceKey = entry.getKey();
                contextMap.put(instanceKey, new AutomationContext("MB_Cluster", instanceKey, userMode));
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

    protected String login(AutomationContext context)
            throws IOException, XPathExpressionException, URISyntaxException, SAXException,
            XMLStreamException, LoginAuthenticationExceptionException {
        LoginLogoutClient loginLogoutClient = new LoginLogoutClient(context);
        return loginLogoutClient.login();
    }

}