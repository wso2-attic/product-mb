/*
 * Copyright (c)2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.mb.migration;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Processor is responsible for reading all the data, modifying them as necessary and storing them back into the
 * database.
 */
public class Processor {

    private static final Logger logger = Logger.getLogger(DBConnector.class);

    /**
     * The instance of the DBConnector which reads and writes queues, bindings and subscriptions
     */
    private DBConnector connector;

    /**
     * The variable which modifies all existing queues, bindings and subscriptions
     */
    private Modifier modifier;

    /**
     * String constant representing the name of the DLC message router.
     */
    private final String DLC_MESSAGE_ROUTER = "amq.dlc";

    /**
     * Initializes the processor with properties given in config.properties file.
     *
     * @throws IOException in case of error whilw loading file reader
     * @throws ClassNotFoundException in case of error while reading file
     */
    public Processor() throws IOException, ClassNotFoundException {

        Properties prop = new Properties();
        // load the properties file
        File configFile = new File("conf/config.properties");
        FileReader reader = new FileReader(configFile);
        prop.load(reader);
        connector = new DBConnector(prop);
        modifier = new Modifier();

    }

    /**
     * Adds DLC bindings to the database. WSO2MB 3.1.0 only contains queues for the DLC where as WSO2MB 3.2.0
     * contains binding for the DLC queue in addition. This method, reads all DLC queues (there can be multiple DLC
     * queues when tenants are present) and binds them to amq.dlc message router.
     */
    private void addDlcBindings() {
        try {
            List<String> queueNames = connector.readDlcQueues();
            List<Binding> bindings = new ArrayList<>(queueNames.size());
            for (String queueName : queueNames) {
                String bindingDetails = modifier.createBindingDetails(queueName, DLC_MESSAGE_ROUTER);
                bindings.add(new Binding(DLC_MESSAGE_ROUTER, queueName, bindingDetails));
            }
            connector.insertBindings(bindings);
        } catch (SQLException e) {
            logger.warn("Error while inserting bindings for DLC queues", e);
        }
    }

    /**
     * Adds amq.dlc message router to the database. WSO2MB 3.1.0 does not contain an exchange(message router) named
     * 'amq .dlc' hence it should be added.
     *
     * @throws MigrationException in case of error while write message router
     */
    void creteDlcMessageRouter() throws MigrationException {
        try {
            connector.writeMessageRouter(DLC_MESSAGE_ROUTER, modifier.createExchangeDetails(DLC_MESSAGE_ROUTER,
                    "DLC", "false"));
            logger.info("DLC message router created");
        } catch (SQLException e) {
            throw new MigrationException("Error while creating message router", e);
        }
    }

    /**
     * Reads all bindings from the database, modifies them from the format in WSO2MB 3.1.0 to WSO2MB 3.2.0 and
     * updates them. Also adds DLC bindings that were not present in WSO2MB 3.1.0.
     *
     * @throws MigrationException in case of error while read bindings
     */
    void modifyBindings() throws MigrationException {

        try {
            List<Binding> bindings = connector.readBindings();
            for (Binding binding : bindings) {
                try {
                    binding.setBindingDetails(modifier.modifyBinding(binding.getBindingDetails()));
                    logger.info("Modified binding information in database");
                } catch (MigrationException e) {
                    logger.error("Error modifying binding for queue: " + binding.getQueueName() + ". Incorrect "
                            + "binding info format: " + binding.getBindingDetails(), e);
                }
            }
            connector.updateBindings(bindings);
            addDlcBindings();

        } catch (SQLException e) {
            throw new MigrationException("Error while modifying bindings in the database", e);
        }
    }

    /**
     * Read data of tables and modify data making all queue name references all simple.
     *
     * @throws MigrationException in case of error while converting queue names to lowercase
     */
    void makeQueueNamesAllSimple() throws MigrationException {
        try {
            connector.updateQueueNamesInQueuesAndBindings();
            connector.updateQueueNamesInSlots();
            connector.updateQueueNamesInQueueMappings();
            connector.updateQueueNamesInSlotMessageIds();
            connector.updateQueueNamesInQueueToLastAssignedIds();
            logger.info("Converted queue names into lowercase");
        } catch (SQLException e) {
            throw new MigrationException("Error while converting queue names to lowercase", e);
        }
    }
}
