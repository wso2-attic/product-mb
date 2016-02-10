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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * The class which communicate with the database. Performs the operation of reading and writing queues, bindings and
 * subscriptions
 */
public class DBConnector {

    // variables which hold the database connection parameters
    String DB_URL;
    String USER;
    String PASS;

    Connection conn;

    // String constants to store table names and column names
    static final String DURABLE_SUB_ID = "SUBSCRIPTION_ID";
    static final String DURABLE_SUB_DATA = "SUBSCRIPTION_DATA";
    static final String DESTINATINATION_TYPE = "DESTINATION_IDENTIFIER";
    static final String DURABLE_SUB_TABLE = "MB_DURABLE_SUBSCRIPTION";
    static final String BINDING_DETAILS = "BINDING_DETAILS";
    static final String MB_BINDING = "MB_BINDING";
    static final String MB_QUEUE = "MB_QUEUE";
    static final String QUEUE_DATA = "QUEUE_DATA";
    static final String QUEUE_NAME = "QUEUE_NAME";

    // Prepared statements to read and write queues, bindings and subscriptions
    static final String GET_SUBSCRIPTION = "SELECT * FROM MB_DURABLE_SUBSCRIPTION";
    static final String GET_QUEUES = "SELECT * FROM " + MB_QUEUE;
    static final String GET_BINDINGS = "SELECT * FROM " + MB_BINDING;
    static final String UPDATE_SUBSCRIPTION = "UPDATE " + DURABLE_SUB_TABLE + " SET " + DURABLE_SUB_DATA
                                              + " =?," + DESTINATINATION_TYPE + " =? WHERE " + DURABLE_SUB_ID + "=?";

    static final String UPDATE_QUEUE = "UPDATE " + MB_QUEUE + " SET " + QUEUE_DATA
                                       + " =? WHERE " + QUEUE_NAME + "=?";

    static final String UPDATE_BINDING = "UPDATE " + MB_BINDING + " SET " + BINDING_DETAILS
                                         + " =? WHERE " + QUEUE_NAME + "=?";
    static final String GET_QUEUE = "SELECT " + QUEUE_DATA + " FROM " + MB_QUEUE + " WHERE " + QUEUE_NAME + " =?";
    static final String GET_BINDING =
            "SELECT " + BINDING_DETAILS + " FROM " + MB_BINDING + " WHERE " + QUEUE_NAME + " =?";

    public DBConnector(Properties properties){
        try {

            // Initialize the Driver and the connection parameters
            Class.forName(properties.getProperty("driverclassname"));
            DB_URL=properties.getProperty("dburl");
            USER = properties.getProperty("dbuser");
            PASS = properties.getProperty("dbpassword");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Connection getConnection(){
        try {
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * Receives all the subscriptions stored in the database.
     *
     * @return the list of Subscriptions
     * @throws SQLException
     */
    public List<Subscription> readSubscriptions() throws SQLException {

        getConnection();
        List<Subscription> subscribers = new ArrayList<>();

        try {
            PreparedStatement preparedStatement = conn.prepareStatement(GET_SUBSCRIPTION);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String subId = resultSet.getString(DURABLE_SUB_ID);
                String destType = resultSet.getString(DESTINATINATION_TYPE);
                String subData = resultSet.getString(DURABLE_SUB_DATA);
                subscribers.add(new Subscription(subId, destType, subData));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            conn.close();
        }
        return subscribers;
    }

    /**
     * Write a subscription to the database.
     *
     * @param subscription new subscription details
     * @throws SQLException
     */
    public void writeSubscription(Subscription subscription) throws SQLException {

        getConnection();
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(UPDATE_SUBSCRIPTION);
            preparedStatement.setString(1, subscription.getSubscriptionData());
            preparedStatement.setString(2, subscription.getDestinationType());
            preparedStatement.setString(3, subscription.getIdentifier());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            conn.close();
        }

    }

    /**
     * Retrieve a queue by its storage queue name.
     *
     * @param storageQueueName the storage queue name of the queue
     * @return the queue details
     * @throws SQLException
     */
    public String readQueue(String storageQueueName) throws SQLException {
        getConnection();
        String queue = null;

        try {
            PreparedStatement preparedStatement = conn.prepareStatement(GET_QUEUE);
            preparedStatement.setString(1, storageQueueName);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                queue = resultSet.getString(QUEUE_DATA);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            conn.close();
        }
        return queue;
    }

    /**
     * Update queue details of a storage queue.
     *
     * @param storageQueue the storage queue name
     * @param queueData    new queue details
     * @throws SQLException
     */
    public void writeQueue(String storageQueue, String queueData) throws SQLException {

        getConnection();
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(UPDATE_QUEUE);
            preparedStatement.setString(1, queueData);
            preparedStatement.setString(2, storageQueue);
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            conn.close();
        }

    }

    /**
     * Retrieve a binding by its storage queue name.
     *
     * @param storageQueueName the queue for which the binding is bound
     * @return binding details
     * @throws SQLException
     */
    public String readBinding(String storageQueueName) throws SQLException {
        getConnection();
        String binding = null;

        try {
            PreparedStatement preparedStatement = conn.prepareStatement(GET_BINDING);
            preparedStatement.setString(1, storageQueueName);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                binding = resultSet.getString(BINDING_DETAILS);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            conn.close();
        }
        return binding;
    }

    /**
     * Update Binding details of a binding to a particular storage queue
     *
     * @param queueName      storage queue name
     * @param bindingDetails new binding details
     * @throws SQLException
     */
    public void writeBinding(String queueName, String bindingDetails) throws SQLException {

        getConnection();
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(UPDATE_BINDING);
            preparedStatement.setString(1, bindingDetails);
            preparedStatement.setString(2, queueName);
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            conn.close();
        }

    }

    /**
     * Retrieve all queues from the databases
     *
     * @return a list of queues
     * @throws SQLException
     */
    public List<Queue> readQueues() throws SQLException {
        getConnection();
        List<Queue> queues = new ArrayList<>();

        try {
            PreparedStatement preparedStatement = conn.prepareStatement(GET_QUEUES);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String name = resultSet.getString(QUEUE_NAME);
                String data = resultSet.getString(QUEUE_DATA);
                queues.add(new Queue(name, data));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            conn.close();
        }
        return queues;
    }

    /**
     * Retrieve all the bindings from the database
     *
     * @return a list of bindings
     * @throws SQLException
     */
    public List<Binding> readBindings() throws SQLException {
        getConnection();
        List<Binding> bindings = new ArrayList<>();
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(GET_BINDINGS);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String queueName = resultSet.getString(QUEUE_NAME);
                String bindingDetails = resultSet.getString(BINDING_DETAILS);
                String exchange = resultSet.getString("EXCHANGE_NAME");
                bindings.add(new Binding(queueName, exchange, bindingDetails));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            conn.close();
        }
        return bindings;
    }
}


