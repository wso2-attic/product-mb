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
 * under the License. and limitations under the License.
 */

package org.wso2.carbon.mb.ui.test.queues;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.utils.backend.MBIntegrationUiBaseTest;
import org.wso2.mb.integration.common.utils.ui.pages.login.LoginPage;
import org.wso2.mb.integration.common.utils.ui.pages.main.HomePage;
import org.wso2.mb.integration.common.utils.ui.pages.main.QueueAddPage;
import org.wso2.mb.integration.common.utils.ui.pages.main.QueuesBrowsePage;

/**
 * This class contains test cases to verify functionality related to 'Queues ->
 * Browse -> Queue Content' page.
 */
public class BrowseQueueContentTestCase extends MBIntegrationUiBaseTest {

	@BeforeClass()
	public void init() throws Exception {
		super.init();
	}

	/***
	 * This test case will add a queue to MB and navigate to browse the queue
	 * content.
	 *
	 * @throws Exception
	 */
	@Test()
	public void navigateQueueContentPage() throws Exception {

		String qName = "testQcontent";
		driver.get(getLoginURL());
		LoginPage loginPage = new LoginPage(driver);
		HomePage homePage = loginPage.loginAs(mbServer.getContextTenant()
				.getContextUser().getUserName(), mbServer.getContextTenant()
				.getContextUser().getPassword());

		QueueAddPage queueAddPage = homePage.getQueueAddPage();
		Assert.assertEquals(queueAddPage.addQueue(qName), true);
		QueuesBrowsePage queuesBrowsePage = homePage.getQueuesBrowsePage();
		Assert.assertNotNull(queuesBrowsePage.browseQueue(qName),
				"Unable to browse Queue " + qName);
		Assert.assertEquals(homePage.getQueuesBrowsePage().deleteQueue(qName), true, "Unable to delete the queue " + qName + " after browsing");

	}

	@AfterClass()
	public void tearDown() {
		driver.quit();
	}

}
