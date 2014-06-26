/*
*  Copyright (c) 2005-2011, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.wso2.carbon.integration.framework.ClientConnectionUtil;
import org.wso2.carbon.integration.framework.TestServerManager;
import org.wso2.carbon.utils.FileManipulator;

import java.io.File;
import java.io.IOException;

/**
 * Prepares the WSO2 MB for test runs, starts the server, and stops the server after
 * test runs
 */
public class MBTestServerManager extends TestServerManager {
    private static final Log log = LogFactory.getLog(MBTestServerManager.class);

    @Override
    @BeforeSuite(timeOut = 600000)
    public String startServer() throws IOException {
        String carbonHome = super.startServer();
        System.setProperty("carbon.home", carbonHome);
        ClientConnectionUtil.waitForPort(5672);
        return carbonHome;
    }

    @Override
    @AfterSuite(timeOut = 600000)
    public void stopServer() throws Exception {
        super.stopServer();
    }

    protected void copyArtifacts(String carbonHome) throws IOException {
        File[] samples = FileManipulator.getMatchingFiles(getMessageBrokerSampleLocation(carbonHome), null, "aar");
        File axis2servicesRepo = new File(carbonHome + File.separator + "repository" + File.separator +
                                 "deployment" + File.separator +
                                 "server" + File.separator + "axis2services" + File.separator);
        for (File sample : samples) {

            FileManipulator.copyFileToDir(sample, axis2servicesRepo);
            log.info("Copying: " + sample.getAbsolutePath() + " to " + axis2servicesRepo.getAbsolutePath());

        }
    }
    private String getMessageBrokerSampleLocation(String carbonHome){
            String mbSampleLoc = carbonHome + File.separator + "samples" + File.separator + "services" +
                    File.separator  + "EventSinkService" + File.separator;
            return mbSampleLoc;
        }

}
