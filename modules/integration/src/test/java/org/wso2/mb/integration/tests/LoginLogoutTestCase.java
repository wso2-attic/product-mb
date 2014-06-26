/*
*Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*WSO2 Inc. licenses this file to you under the Apache License,
*Version 2.0 (the "License"); you may not use this file except
*in compliance with the License.
*You may obtain a copy of the License at
*
*http://www.apache.org/licenses/LICENSE-2.0
*
*Unless required by applicable law or agreed to in writing,
*software distributed under the License is distributed on an
*"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
*KIND, either express or implied.  See the License for the
*specific language governing permissions and limitations
*under the License.
*/

package org.wso2.mb.integration.tests;

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.wso2.carbon.integration.framework.ClientConnectionUtil;
import org.wso2.carbon.integration.framework.LoginLogoutUtil;

/**
 * A test case which tests logging in & logging out of a Carbon core server
 */
public class LoginLogoutTestCase {

    private LoginLogoutUtil util = new LoginLogoutUtil();
    private static final Log log = LogFactory.getLog(LoginLogoutTestCase.class);

    @BeforeClass(groups = {"wso2.mb"})
    public void login() throws Exception {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            log.error("Error in thread sleep in LoginLogoutTestCase ", e);
            Assert.fail("Error in thread sleep in LoginLogoutTestCase");
        }
        ClientConnectionUtil.waitForPort(9763);
        util.login();
    }

    @AfterClass(groups = {"wso2.mb"})
    public void logout() throws Exception {
        ClientConnectionUtil.waitForPort(9763);
        util.logout();
    }
}

