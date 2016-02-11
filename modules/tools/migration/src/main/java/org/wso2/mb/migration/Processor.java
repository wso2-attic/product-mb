/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public class Processor {

    /**
     * The instance of the DBConnector which reads and writes queues, bindings and subscriptions
     */
    DBConnector connector;

    /**
     * The variable which modifies all existing queues, bindings and subscriptions
     */
    Modifier modifier;

    /**
     * List of all the storage queue names for which subscriptions are bound
     */
    List<String> storageQueues;

    public Processor() {

        Properties prop = new Properties();
        OutputStream output = null;
        try {

            // load the properties file
            File configFile = new File("config.properties");
            FileReader reader = new FileReader(configFile);
            prop.load(reader);

        } catch (IOException io) {
            io.printStackTrace();
        } finally {
            if (output != null) {
                try {
                    output.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

        connector = new DBConnector(prop);
        modifier = new Modifier();

        storageQueues = new ArrayList<>();
    }

    /**
     * Method to modify all queues, bindings and subscriptions
     */
    public void modifyTables() {
        Logger log = Logger.getLogger(Processor.class.getName());
        try {

            List<Subscription> subscriptions = connector.readSubscriptions();

            // For each subscriber that is read from the database, modify the queue and the binding it is associated
            // with
            for (Subscription subscription : subscriptions) {

                subscription.setDestinationType(modifier.modifyDestinationType(subscription.getDestinationType()));
                subscription.setSubscriptionData(modifier.modifySubscription(subscription.getSubscriptionData()));
                connector.writeSubscription(subscription);

                String storageQueueName = modifier.getStorageQueueName();
                storageQueues.add(storageQueueName);

                String queue = connector.readQueue(storageQueueName);
                if (null != queue) {
                    queue = modifier.modifyQueue(queue);
                    connector.writeQueue(storageQueueName, queue);
                }

                String binding = connector.readBinding(storageQueueName);
                if (null != binding) {
                    binding = modifier.modifyBinding(binding);
                    connector.writeBinding(storageQueueName, binding);
                }
            }
            log.info("Modified " + subscriptions.size() + " subscriptions with associated queues and bindings.");
            int idleQueues = modifyIdleQueues();
            log.info("Modified " + idleQueues + " queues without subscriptions.");
            int idleBindings = modifyIdleBindings();
            log.info("Modified " + idleBindings + " bindings without subscriptions.");
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    /**
     * Method to modify queues that have no subscriptions.
     *
     * @return the number of modified queues
     * @throws SQLException
     */
    public int modifyIdleQueues() throws SQLException {
        List<Queue> queues = connector.readQueues();
        int idleQueues = 0;
        for (Queue queue : queues) {
            if (!(storageQueues.contains(queue.getQueueName()))) {
                idleQueues = idleQueues + 1;
                queue.setQueueData(modifier.modifyDefaultQueue(queue.getQueueData()));
                connector.writeQueue(queue.getQueueName(), queue.getQueueData());
            }
        }
        return idleQueues;
    }

    /**
     * Method to modify bindings that have no subscriptions.
     *
     * @return the number of modified bindings
     * @throws SQLException
     */
    public int modifyIdleBindings() throws SQLException {
        List<Binding> bindings = connector.readBindings();
        int idleBindings = 0;
        for (Binding binding : bindings) {
            if (!(storageQueues.contains(binding.getQueueName()))) {
                idleBindings = idleBindings + 1;
                binding.setBindingDetails(modifier.modifyDefaultBinding(binding.getBindingDetails()));
                connector.writeBinding(binding.getQueueName(), binding.getBindingDetails());
            }
        }
        return idleBindings;
    }

}
