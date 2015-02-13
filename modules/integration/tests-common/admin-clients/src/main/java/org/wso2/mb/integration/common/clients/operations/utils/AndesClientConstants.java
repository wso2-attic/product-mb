package org.wso2.mb.integration.common.clients.operations.utils;

import java.io.File;

/**
 * Created by hemikakodikara on 1/30/15.
 */
public class AndesClientConstants {
    public static final String ANDES_ICF = "org.wso2.andes.jndi.PropertiesFileInitialContextFactory";
    public static final String CF_NAME_PREFIX = "connectionfactory.";
    public static final String CF_NAME = "andesConnectionfactory";
    public static final String CARBON_VIRTUAL_HOST_NAME = "carbon";
    public static final String CARBON_CLIENT_ID = "carbon";
    public static final String FILE_PATH_TO_WRITE_RECEIVED_MESSAGES = System.getProperty("framework.resource.location") + File.separator + "receivedMessages.txt";
    public static final String FILE_PATH_TO_WRITE_STATISTICS = System.getProperty("framework.resource.location") + File.separator + "stats.csv";


    // please see usages prior editing
    public static final String PUBLISH_MESSAGE_FORMAT = "Sending Message:{0} ThreadID:{1}";
    public static final long DEFAULT_RUN_TIME = 10000L;

}
