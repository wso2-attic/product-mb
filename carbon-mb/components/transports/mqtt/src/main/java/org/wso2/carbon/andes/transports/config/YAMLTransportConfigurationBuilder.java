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

package org.wso2.carbon.andes.transports.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.introspector.BeanAccess;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Reads the transport configuration from the yaml file
 */
public class YAMLTransportConfigurationBuilder {

    public static final String MQTT_TRANSPORT_CONF = "transport.mqtt.conf";

    private static final Log log = LogFactory.getLog(YAMLTransportConfigurationBuilder.class);

    public static MqttTransportConfiguration readConfiguration() throws FileNotFoundException {

        MqttTransportConfiguration mqttConfiguration;
        InputStream input = null;
        try {
            String mqttTransportConfigPath = System.getProperty(MQTT_TRANSPORT_CONF, "conf" + File
                    .separator + "transports" + File.separator + "mqtt-transports.yaml");
            input = new FileInputStream(new File(mqttTransportConfigPath));
            Yaml yaml = new Yaml();
            yaml.setBeanAccess(BeanAccess.FIELD);
            mqttConfiguration = yaml.loadAs(input, MqttTransportConfiguration.class);
        } finally {
            if (null != input) {
                try {
                    input.close();
                } catch (IOException e) {
                    log.error("Error occurred while closing the file reading stream ", e);
                }
            }
        }

        return mqttConfiguration;
    }
}
