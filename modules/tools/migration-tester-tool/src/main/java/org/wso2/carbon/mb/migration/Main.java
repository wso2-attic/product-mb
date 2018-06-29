/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.wso2.carbon.mb.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.andes.event.stub.service.AndesEventAdminServiceEventAdminException;
import org.wso2.carbon.andes.stub.AndesAdminServiceBrokerManagerAdminException;
import org.wso2.carbon.authenticator.stub.LoginAuthenticationExceptionException;
import org.wso2.carbon.authenticator.stub.LogoutAuthenticationExceptionException;
import org.wso2.carbon.mb.migration.admin.services.AdminClientManager;
import org.wso2.carbon.mb.migration.config.ConfigReader;
import org.wso2.carbon.mb.migration.config.TestPlan;
import org.wso2.carbon.mb.migration.jms.DataPopulator;
import org.wso2.carbon.um.ws.api.stub.RemoteUserStoreManagerServiceUserStoreExceptionException;

import java.io.IOException;
import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.xml.bind.JAXBException;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static final String RESOURCE_DIR = "conf/";

    private static final String CONFIG_FILE_NAME = "test-plan.xml";

    private static final String BEFORE_MIGRATION_SETUP = "before-migration-setup";
    private static final String AFTER_MIGRATION_SETUP = "after-migration-setup";
    private static final String AFTER_MIGRATION_VERIFY = "verify";
    private static final String CLEAN = "cleanup";

    public static void main(String[] args) throws NamingException,
                                                  IOException,
                                                  JMSException,
                                                  JAXBException,
                                                  LoginAuthenticationExceptionException,
                                                  AndesEventAdminServiceEventAdminException,
                                                  AndesAdminServiceBrokerManagerAdminException,
                                                  LogoutAuthenticationExceptionException,
                                                  RemoteUserStoreManagerServiceUserStoreExceptionException,
                                                  InterruptedException {
        LOGGER.info("Starting migration tool");

        if (args.length == 0) {
            throw new RuntimeException("Execution mode not specified.");
        }

        String currentMode = args[0];
        ConfigReader configReader = new ConfigReader();
        TestPlan testPlan = configReader.read(CONFIG_FILE_NAME);

        AdminClientManager adminClientManager = new AdminClientManager(testPlan);
        DataPopulator dataPopulator = new DataPopulator(testPlan, adminClientManager.getUserMap());

        switch (currentMode) {
            case Main.BEFORE_MIGRATION_SETUP:
                adminClientManager.setupBeforeMigration();
                dataPopulator.populateBeforeMigrationData();
                break;
            case Main.AFTER_MIGRATION_SETUP:
                adminClientManager.setupAfterMigration();
                dataPopulator.populateAfterMigrationData();
                break;
            case AFTER_MIGRATION_VERIFY:
                adminClientManager.verifyAfterMigration();
                break;
            case CLEAN:
                adminClientManager.cleanUp();
                break;
            default:
                LOGGER.info("Unknown operation {}", currentMode);
        }

        dataPopulator.close();
        LOGGER.info("Migration tester step \"{}\" completed", currentMode);
    }
}
