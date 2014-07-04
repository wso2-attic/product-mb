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

package org.wso2.mb.integration.common.utils.backend;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.context.TestUserMode;

/**
 * Base class of all MB integration tests
 */
public class MBIntegrationBaseTest {

    protected Log log = LogFactory.getLog(MBIntegrationBaseTest.class);
    protected AutomationContext automationContext;
    protected String backendURL;

    protected void init(TestUserMode userMode) throws Exception {
        automationContext = new AutomationContext("MB", userMode);
        backendURL = automationContext.getContextUrls().getBackEndUrl();
    }

}
