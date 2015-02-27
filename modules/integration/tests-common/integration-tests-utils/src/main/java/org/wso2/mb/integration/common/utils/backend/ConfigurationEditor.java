/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.mb.integration.common.utils.backend;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.carbon.integration.common.utils.mgt.ServerConfigurationManager;

import java.io.File;
import java.nio.file.Files;

/**
 * This class allows a test case to edit the main server configuration (currently broker.xml) and apply it to the
 * server before execution.
 */
public class ConfigurationEditor {

    public static final String ORIGINAL_CONFIG_BACKUP_PREFIX = "original_";

    public static final String UPDATED_CONFIG_FILE_PREFIX = "updated_";

    public XMLConfiguration configuration;

    public String originalConfigFilePath;

    public String updatedConfigFilePath;

    public ConfigurationEditor(String originalConfigFilePath) throws ConfigurationException {
        this.originalConfigFilePath = originalConfigFilePath;

        configuration = new XMLConfiguration(this.originalConfigFilePath);

        // Support XPath queries.
        configuration.setExpressionEngine(new XPathExpressionEngine());

        configuration.setDelimiterParsingDisabled(true); // If we don't do this,
        // we can't add a new configuration to the compositeConfiguration by code.
    }

    /**
     * Update a property in loaded original configuration
     * @param property
     * @param value
     * @return
     */
    public String updateProperty(AndesConfiguration property, String value) {
        configuration.setProperty(property.get().getKeyInFile(),value);
        return value;
    }

    /**
     * Apply modified configuration and restart server
     * @param serverConfigurationManager
     * @return
     * @throws Exception
     */
    public boolean applyUpdatedConfigurationAndRestartServer(ServerConfigurationManager serverConfigurationManager) throws Exception {

        //Rename original configuration file to original_broker.xml
        String originalConfigFileDirectory = originalConfigFilePath.substring(0,originalConfigFilePath.lastIndexOf(File.separator));
        String originalConfigFileName = originalConfigFilePath.substring(originalConfigFilePath.lastIndexOf(File.separator));

        /*File originalConfigFile = new File(originalConfigFilePath);
        File renamedOriginalConfigFile = new File(originalConfigFileDirectory + ORIGINAL_CONFIG_BACKUP_PREFIX + originalConfigFile.getName());
        originalConfigFile.renameTo(renamedOriginalConfigFile);*/

        //Save updated Configuration as updated_broker.xml in same path

        updatedConfigFilePath = originalConfigFileDirectory + UPDATED_CONFIG_FILE_PREFIX + originalConfigFileName;
        configuration.save(updatedConfigFilePath);

        serverConfigurationManager.applyConfiguration(new File(updatedConfigFilePath), new File(originalConfigFilePath), true, true);

        return true;
    }

    /**
     * Remove temporarily generated config file after running the test case. (recommended for the @cleanup method.)
     * @return true if the delete was successful.
     */
    public boolean removeUpdatedConfigurationFile() {

        File updatedFile = new File(updatedConfigFilePath);
        return updatedFile.delete();
    }


}
