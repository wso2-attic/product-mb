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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;

/**
 * The class which communicates with the database. Performs the operation of reading, inserting and updating bindings
 * and message routers.
 */
class DBConnector {

    /**
     * variables which hold the database connection parameters
     */
    private String DB_URL;
    private String USER;
    private String PASSWORD;

    /**
     * String constants representing tables to be modified.
     */
    private static final String MB_BINDING = "MB_BINDING";
    private static final String MB_QUEUE = "MB_QUEUE";
    private static final String MB_EXCHANGE = "MB_EXCHANGE";
    private static final String MB_QUEUE_MAPPING = "MB_QUEUE_MAPPING";
    private static final String MB_SLOT_MESSAGE_ID = "MB_SLOT_MESSAGE_ID";
    private static final String MB_SLOT = "MB_SLOT";
    private static final String MB_QUEUE_TO_LAST_ASSIGNED_ID = "MB_QUEUE_TO_LAST_ASSIGNED_ID";

    /**
     * String constants representing patterns used in MB_BINDING table for queue name.
     */
    private static final String QUEUE_NAME_PATTERN = "queueName=%s";

    /**
    *  String constants representing patterns used in MB_BINDING table for bound queue name.
    */
    private static final String BOUND_QUEUE_NAME_PATTERN = "boundQueueName=%s";

    /**
    *  String constants representing patterns used in MB_BINDING table for binding key.
    */
    private static final String BINDING_KEY_PATTERN = "bindingKey=%s";


    /**
     * String constants representing table columns.
     */
    private static final String BINDING_DETAILS = "BINDING_DETAILS";
    private static final String QUEUE_NAME = "QUEUE_NAME";
    private static final String QUEUE_DATA = "QUEUE_DATA";
    private static final String EXCHANGE_NAME = "EXCHANGE_NAME";
    private static final String EXCHANGE_DATA = "EXCHANGE_DATA";
    private static final String SLOT_ID = "SLOT_ID";
    private static final String STORAGE_QUEUE_NAME = "STORAGE_QUEUE_NAME";
    private static final String ASSIGNED_QUEUE_NAME = "ASSIGNED_QUEUE_NAME";

    /**
     * Prepared statements to read, insert and update bindings and message routers.
     */
    private static final String GET_BINDINGS = "SELECT * FROM " + MB_BINDING;
    private static final String GET_QUEUES = "SELECT * FROM " + MB_QUEUE;
    private static final String GET_QUEUE_MAPPINGS = "SELECT * FROM " + MB_QUEUE_MAPPING;
    private static final String GET_MB_SLOT_MESSAGE_IDS = "SELECT * FROM " + MB_SLOT_MESSAGE_ID;
    private static final String GET_SLOTS = "SELECT * FROM " + MB_SLOT;
    private static final String GET_MB_QUEUE_TO_LAST_ASSIGNED_IDS = "SELECT * FROM " + MB_QUEUE_TO_LAST_ASSIGNED_ID;



    /**
     * Queries related to updating queues.
     */
    private static final String UPDATE_QUEUE = "UPDATE " + MB_QUEUE
                                                + " SET " + QUEUE_NAME + " =?"
                                                + " , " + QUEUE_DATA + " =?"
                                                + " WHERE " + QUEUE_NAME + " =?";

    private static final String DELETE_ALL_BINDINGS = "DELETE FROM " + MB_BINDING;

    private static final String UPDATE_MB_QUEUE_MAPPING = "UPDATE " + MB_QUEUE_MAPPING
                                                + " SET " + QUEUE_NAME + " =?"
                                                + " WHERE " + QUEUE_NAME + " =?";

    private static final String UPDATE_MB_SLOT_MESSAGE_ID = "UPDATE " + MB_SLOT_MESSAGE_ID
                                                + " SET " + QUEUE_NAME + " =?"
                                                + " WHERE " + QUEUE_NAME + " =?";

    private static final String UPDATE_MB_SLOT = "UPDATE " + MB_SLOT
                                                + " SET " + STORAGE_QUEUE_NAME + " =?"
                                                + " , " + ASSIGNED_QUEUE_NAME + " =?"
                                                + " WHERE " + SLOT_ID + " =?";

    private static final String UPDATE_MB_QUEUE_TO_LAST_ASSIGNED_ID = "UPDATE " + MB_QUEUE_TO_LAST_ASSIGNED_ID
            + " SET " + QUEUE_NAME + " =?"
            + " WHERE " + QUEUE_NAME + " =?";

    private static final String UPDATE_BINDING = "UPDATE " + MB_BINDING
                                                 + " SET " + BINDING_DETAILS + " =?"
                                                 + " WHERE " + QUEUE_NAME + "=?";

    private static final String INSERT_BINDING = "INSERT INTO " + MB_BINDING + " ("
                                                 + EXCHANGE_NAME + ", "
                                                 + QUEUE_NAME + ", "
                                                 + BINDING_DETAILS + ") "
                                                 + " VALUES (?,?,?)";

    private static final String INSERT_EXCHANGE = "INSERT INTO " + MB_EXCHANGE + " ("
                                                  + EXCHANGE_NAME + ", "
                                                  + EXCHANGE_DATA + ") "
                                                  + " VALUES (?,?)";

    private static final Logger logger = Logger.getLogger(DBConnector.class);




    DBConnector(Properties properties) throws ClassNotFoundException {
        // Initialize the Driver and the connection parameters
        Class.forName(properties.getProperty("driverclassname"));
        DB_URL = properties.getProperty("dburl");
        USER = properties.getProperty("dbuser");
        PASSWORD = properties.getProperty("dbpassword");
    }

    /**
     * Retrieves the connection object initialized by the provided properties.
     *
     * @return the connection object
     * @throws SQLException when a database error occurs
     */
    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL, USER, PASSWORD);
    }

    /**
     * Update Binding details of a list of existing bindings.
     *
     * @param bindings the list of modified bindings to be stored.
     * @throws SQLException when a database error occurs
     */
    void updateBindings(List<Binding> bindings) throws SQLException {

        try (Connection conn = getConnection();
             PreparedStatement preparedStatement = conn.prepareStatement(UPDATE_BINDING)) {
            for (Binding binding : bindings) {
                preparedStatement.setString(1, binding.getBindingDetails());
                preparedStatement.setString(2, binding.getQueueName());
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        }
    }

    /**
     * Insert bindings to the MB_BINDING table.
     *
     * @param bindings list of bindings to be inserted
     */
    void insertBindings(List<Binding> bindings) throws SQLException {

        try (Connection conn = getConnection();
             PreparedStatement preparedStatement = conn.prepareStatement(INSERT_BINDING)) {

            for (Binding binding : bindings) {
                preparedStatement.setString(1, binding.getMessageRouter());
                preparedStatement.setString(2, binding.getQueueName());
                preparedStatement.setString(3, binding.getBindingDetails());
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        }
    }

    /**
     * Retrieve all the bindings from the database.
     *
     * @return a list of bindings
     * @throws SQLException if a database error occurs when closing the connection
     */
    List<Binding> readBindings() throws SQLException {
        List<Binding> bindings = new ArrayList<>();

        try (Connection conn = getConnection();
             PreparedStatement preparedStatement = conn.prepareStatement(GET_BINDINGS);
             ResultSet resultSet = preparedStatement.executeQuery()) {

            while (resultSet.next()) {
                String queueName = resultSet.getString(QUEUE_NAME);
                String bindingDetails = resultSet.getString(BINDING_DETAILS);
                bindings.add(new Binding(queueName, bindingDetails));
            }
        }
        return bindings;
    }

    /**
     * Retrieves all the DLC queues from the database. There could be multiple DLC queues if tenants were present.
     *
     * @return a list of DLC queues
     * @throws SQLException if a database error occurs when closing the connection
     */
    List<String> readDlcQueues() throws SQLException {

        List<String> queueNames = new ArrayList<>();
        try (Connection conn = getConnection();
             PreparedStatement preparedStatement = conn.prepareStatement(GET_QUEUES);
             ResultSet resultSet = preparedStatement.executeQuery()) {

            while (resultSet.next()) {
                String queueName = resultSet.getString(QUEUE_NAME);
                if (queueName.contains("DeadLetterChannel")) {
                    queueNames.add(queueName);
                }
            }
        }
        return queueNames;
    }

    /**
     * Insert a row to MB_EXCHANGE using the given router parameters given.
     *
     * @param routerName the name of the message router
     * @param routerData string representing details of the message router
     * @throws SQLException if a database error occurs when closing the connection
     */
    void writeMessageRouter(String routerName, String routerData) throws SQLException {

        try (Connection conn = getConnection();
             PreparedStatement preparedStatement = conn.prepareStatement(INSERT_EXCHANGE)) {
            preparedStatement.setString(1, routerName);
            preparedStatement.setString(2, routerData);
            preparedStatement.executeUpdate();
        }
    }

    /**
     * Make queue names of MB_QUEUE table all simple letters. Before that read and delete
     * all entries in MB_BINDING to get rid of foreign key constraints. Then update
     * MB_BINDING table as well with modified queue names.
     *
     * @throws SQLException in case of executing updates
     */
    void updateQueueNamesInQueuesAndBindings() throws SQLException {

        try (Connection conn = getConnection();
             PreparedStatement getBindingsStatement = conn.prepareStatement(GET_BINDINGS);
             ResultSet bindingsResultSet = getBindingsStatement.executeQuery();
             PreparedStatement removeBindingsStatement = conn.prepareStatement(DELETE_ALL_BINDINGS);
             PreparedStatement getQueuesPreparedStatement = conn.prepareStatement(GET_QUEUES);
             PreparedStatement updatePreparedStatement =  conn.prepareStatement(UPDATE_QUEUE);
             PreparedStatement addBindingsStatement = conn.prepareStatement(INSERT_BINDING);
             ResultSet getQueuesResultSet = getQueuesPreparedStatement.executeQuery()) {


            //delete all bindings to get rid of constraints
            removeBindingsStatement.executeUpdate();
            updateQueueNamesInQueues(updatePreparedStatement, getQueuesResultSet);

            while (bindingsResultSet.next()) {
                String queueName = bindingsResultSet.getString(QUEUE_NAME);
                String bindingData = bindingsResultSet.getString(BINDING_DETAILS);
                if (queueNameHasCapitals(queueName)) {
                    String newQueueName = queueName.toLowerCase();
                    bindingData = bindingData.replaceAll(generateQueueNameString(queueName),
                            generateQueueNameString(newQueueName));
                    bindingData = bindingData.replaceAll(generateBoundQueueNameDataString(queueName),
                            generateBoundQueueNameDataString(newQueueName));
                    queueName = newQueueName;
                }

                Pattern pattern = Pattern.compile("bindingKey=.*");
                Matcher matcher = pattern.matcher(bindingData);

                if (matcher.find()) {
                    String bindingKey = matcher.group(0).split("=")[1];

                    if(bindingKeyHasCapitals(bindingKey)) {
                        String newBindingKey = bindingKey.toLowerCase();
                        bindingData = bindingData.replaceAll(generateBindingKeyDataString(bindingKey),
                                generateBindingKeyDataString(newBindingKey));
                    }
                }
                addBindingsStatement.setString(1, bindingsResultSet.getString(EXCHANGE_NAME));
                addBindingsStatement.setString(2, queueName);
                addBindingsStatement.setString(3, bindingData);
                addBindingsStatement.addBatch();
            }
            addBindingsStatement.executeBatch();

        }
    }

    /**
     * Make queue names of MB_QUEUE table all simple letters.
     *
     * @throws SQLException in case of executing update
     */
    private void updateQueueNamesInQueues(PreparedStatement updateStatement, ResultSet resultSet) throws SQLException {

        while (resultSet.next()) {
            String queueName = resultSet.getString(QUEUE_NAME);
            if (queueNameHasCapitals(queueName)) {
                String newQueueName = queueName.toLowerCase();
                String queueData = resultSet.getString(QUEUE_DATA);
                String newQueueData = queueData.replaceAll(generateQueueNameString(queueName),
                        generateQueueNameString(newQueueName));

                updateStatement.setString(1, newQueueName);
                updateStatement.setString(2, newQueueData);
                updateStatement.setString(3, queueName);
                updateStatement.executeUpdate();
            }
        }
    }

    /**
     * Generate new queue name to put into database.
     * @param queueName Name of queue
     * @return generated data string
     */
    private String generateQueueNameString(String queueName) {
        return String.format(QUEUE_NAME_PATTERN, queueName);
    }

    /**
     * Generate new queue name to put into database.
     * @param bindingKey Name of binding key
     * @return generated data string
     */
    private String generateBindingKeyDataString(String bindingKey) {
        return String.format(BINDING_KEY_PATTERN, bindingKey);
    }

    /**
     * Generate new queue name to put into database.
     * @param queueName Name of bound queue
     * @return generated data string
     */
    private String generateBoundQueueNameDataString(String queueName) {
        return String.format(BOUND_QUEUE_NAME_PATTERN, queueName);
    }

    /**
     * Make all queue names in MB_SLOT table all simple
     *
     * @throws SQLException in case of executing update
     */
    void updateQueueNamesInSlots() throws SQLException {

        try(Connection conn = getConnection();
            PreparedStatement updateStatement = conn.prepareStatement(UPDATE_MB_SLOT);
            PreparedStatement preparedStatement = conn.prepareStatement(GET_SLOTS);
            ResultSet resultSet = preparedStatement.executeQuery()) {

            while (resultSet.next()) {
                String queueName = resultSet.getString(STORAGE_QUEUE_NAME);
                if(queueNameHasCapitals(queueName)) {
                    String newQueueName = queueName.toLowerCase();
                    updateStatement.setString(1, newQueueName);
                    updateStatement.setString(2, newQueueName);
                    updateStatement.setString(3, resultSet.getString(SLOT_ID));
                    updateStatement.addBatch();
                }
            }
            updateStatement.executeBatch();

        }
    }

    /**
     * Make queue names in MB_QUEUE_MAPPING table all simple.
     *
     * @throws SQLException in case of executing update
     */
    void updateQueueNamesInQueueMappings() throws SQLException {

        try(Connection conn = getConnection();
            PreparedStatement updateStatement = conn.prepareStatement(UPDATE_MB_QUEUE_MAPPING);
            PreparedStatement preparedStatement = conn.prepareStatement(GET_QUEUE_MAPPINGS);
            ResultSet resultSet = preparedStatement.executeQuery()) {

            while (resultSet.next()) {
                String queueName = resultSet.getString(QUEUE_NAME);
                if(queueNameHasCapitals(queueName)) {
                    String newQueueName = queueName.toLowerCase();
                    updateStatement.setString(1, newQueueName);
                    updateStatement.setString(2, queueName);
                    updateStatement.addBatch();
                }
            }
            updateStatement.executeBatch();

        }
    }

    /**
     * Make queue names in  MB_SLOT_MESSAGE_ID table all simple.
     *
     * @throws SQLException in case of executing update
     */
    void updateQueueNamesInSlotMessageIds() throws SQLException {

        try(Connection conn = getConnection();
            PreparedStatement updateStatement = conn.prepareStatement(UPDATE_MB_SLOT_MESSAGE_ID);
            PreparedStatement preparedStatement = conn.prepareStatement(GET_MB_SLOT_MESSAGE_IDS);
            ResultSet resultSet = preparedStatement.executeQuery()) {

            while (resultSet.next()) {
                String queueName = resultSet.getString(QUEUE_NAME);
                if(queueNameHasCapitals(queueName)) {
                    String newQueueName = queueName.toLowerCase();
                    updateStatement.setString(1, newQueueName);
                    updateStatement.setString(2, queueName);
                    updateStatement.addBatch();
                }
            }
            updateStatement.executeBatch();
        }
    }

    /**
     * Make all queue names in MB_QUEUE_TO_LAST_ASSIGNED_ID table all simple.
     *
     * @throws SQLException in case of executing update
     */
    void updateQueueNamesInQueueToLastAssignedIds() throws SQLException {

        try(Connection conn = getConnection();
            PreparedStatement updateStatement = conn.prepareStatement(UPDATE_MB_QUEUE_TO_LAST_ASSIGNED_ID);
            PreparedStatement preparedStatement = conn.prepareStatement(GET_MB_QUEUE_TO_LAST_ASSIGNED_IDS);
            ResultSet resultSet = preparedStatement.executeQuery()) {

            while (resultSet.next()) {
                String queueName = resultSet.getString(QUEUE_NAME);
                if(queueNameHasCapitals(queueName)) {
                    String newQueueName = queueName.toLowerCase();
                    updateStatement.setString(1, newQueueName);
                    updateStatement.setString(2, queueName);
                    updateStatement.addBatch();
                }
            }
            updateStatement.executeBatch();
        }
    }


    /**
     * Check if string has any uppercase letter
     *
     * @param queueName Name of queue
     * @return true if has any upper case letter
     */
    private boolean queueNameHasCapitals(String queueName) {
        return !queueName.equals(queueName.toLowerCase());
    }

    /**
     * Check if string has any uppercase letter
     *
     * @param bindingKey Binding key of queue
     * @return true if has any upper case letter
     */
    private boolean bindingKeyHasCapitals(String bindingKey) {
        return !bindingKey.equals(bindingKey.toLowerCase());
    }

}


