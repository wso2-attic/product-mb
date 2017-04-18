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
     */
    public Processor() {

        Properties prop = new Properties();
        try {

            // load the properties file
            File configFile = new File("config.properties");
            FileReader reader = new FileReader(configFile);
            prop.load(reader);
            connector = new DBConnector(prop);
            modifier = new Modifier();

        } catch (IOException io) {
            io.printStackTrace();
        }
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
            e.printStackTrace();
        }
    }

    /**
     * Adds amq.dlc message router to the database. WSO2MB 3.1.0 does not contain an exchange(message router) named
     * 'amq .dlc' hence it should be added.
     */
    void creteDlcMessageRouter() {
        try {
            connector.writeMessageRouter(DLC_MESSAGE_ROUTER, modifier.createExchangeDetails(DLC_MESSAGE_ROUTER,
                                                                                      "DLC", "false"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Reads all bindings from the database, modifies them from the format in WSO2MB 3.1.0 to WSO2MB 3.2.0 and
     * updates them. Also adds DLC bindings that were not present in WSO2MB 3.1.0
     */
    void modifyBindings() {
        List<Binding> bindings;
        try {
            bindings = connector.readBindings();
            for (Binding binding : bindings) {
                try {
                    binding.setBindingDetails(modifier.modifyBinding(binding.getBindingDetails()));
                } catch (Exception e) {
                    System.out.println("Error modifying binding for queue: " + binding.getQueueName()
                                       + ". Incorrect binding info format: " + binding.getBindingDetails());
                    e.printStackTrace();
                }
            }
            connector.updateBindings(bindings);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        addDlcBindings();
    }

    /**
     * Read data of tables and modify data making all queue name references all simple.
     */
    void makeQueueNamesAllSimple() {
        try {
            connector.updateQueueNamesInQueuesAndBindings();
            connector.updateQueueNamesInSlots();
            connector.updateQueueNamesInQueueMappings();
            connector.updateQueueNamesInSlotMessageIds();
        } catch (Exception e) {
            System.out.println("Error while making queue names simple in all places");
            e.printStackTrace();
        }
    }
}
