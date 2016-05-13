/*
*  Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.mb.integration.common.clients.operations.utils;

import java.io.File;

/**
 * This class contains the constants that are being used by the andes client.
 */
public class AndesClientConstants {
    /**
     * Andes initial context factory.
     */
    public static final String ANDES_ICF =
            "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";

    /**
     * Connection factory name prefix.
     */
    public static final String CF_NAME_PREFIX = "connectionfactory.";

    /**
     * Andes connection factory name
     */
    public static final String CF_NAME = "andesConnectionfactory";

    /**
     * WSO2 carbon factory name.
     */
    public static final String CARBON_VIRTUAL_HOST_NAME = "carbon";

    /**
     * Carbon client ID.
     */
    public static final String CARBON_CLIENT_ID = "carbon";

    /**
     * Default file path to write received messages by subscriber/consumer
     */
    public static final String FILE_PATH_TO_WRITE_RECEIVED_MESSAGES =
            System.getProperty("project.build.directory") + File.separator + "receivedMessages.txt";

    /**
     * Default file path to write statistics by subscriber/consumer and publisher Suppressing
     * "UnusedDeclaration" as this may be used in client configuration
     */
    @SuppressWarnings("UnusedDeclaration")
    public static final String FILE_PATH_TO_WRITE_STATISTICS =
            System.getProperty("framework.resource.location") + File.separator + "stats.csv";

    /**
     * Message publishing format
     */
    public static final String PUBLISH_MESSAGE_FORMAT = "Sending Message:{0} ThreadID:{1}";
    // please see usages prior editing

    /**
     * Default waiting time that is used to check whether the consumer has received messages.
     */
    public static final long DEFAULT_RUN_TIME = 10000L;

    /**
     * Admin user name for AMQP connection string
     */
    public static final String DEFAULT_USERNAME = "admin";

    /**
     * Admin password for AMQP connection string
     */
    public static final String DEFAULT_PASSWORD = "admin";

    /**
     * Default host name for AMQP connection string
     */
    public static final String DEFAULT_HOST_NAME = "127.0.0.1";

    /**
     * Default port for AMQP connections string
     */
    public static final int DEFAULT_PORT = 5672;

    /**
     * File path to read message content for publishing
     */
    public static final String MESSAGE_CONTENT_INPUT_FILE_PATH_1MB =
            System.getProperty("framework.resource.location") + File.separator +
            "MessageContentInput.txt";

    /**
     * File path creating a file.
     */
    public static final String FILE_PATH_FOR_CREATING_A_NEW_FILE =
            System.getProperty("project.build.directory") + File.separator + "newFile.txt";

    /**
     * File path for a file of size 1 kb.
     */
    public static final String FILE_PATH_FOR_ONE_KB_SAMPLE_FILE =
            System.getProperty("framework.resource.location") + "sample" + File.separator +
            "sample_1KB_msg.xml";

    /**
     * System property name of andes acknowledgement wait timeout
     */
    public static final String ANDES_ACK_WAIT_TIMEOUT_PROPERTY = "AndesAckWaitTimeOut";

}
