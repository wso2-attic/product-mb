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

package org.wso2.carbon.andes.transports.mqtt.internal;


import org.osgi.framework.BundleContext;
import org.wso2.andes.kernel.Andes;
import org.wso2.carbon.kernel.CarbonRuntime;


/**
 * MqttTransportDataHolder to holds instances referenced through org.wso2.carbon.helloworld.internal
 * .MqttTransportServiceComponent.
 *
 * @since 3.5.0-SNAPSHOT
 */
public class MqttTransportDataHolder {

    private static MqttTransportDataHolder instance = new MqttTransportDataHolder();
    private CarbonRuntime carbonRuntime;
    private Andes andesInstance;
    private BundleContext context;

    private MqttTransportDataHolder() {

    }

    /**
     * This returns the MqttTransportDataHolder instance.
     *
     * @return The MqttTransportDataHolder instance of this singleton class
     */
    public static MqttTransportDataHolder getInstance() {
        return instance;
    }

    /**
     * Returns the CarbonRuntime service which gets set through a service component.
     *
     * @return CarbonRuntime Service
     */
    public CarbonRuntime getCarbonRuntime() {
        return carbonRuntime;
    }

    /**
     * This method is for setting the CarbonRuntime service. This method is used by
     * MqttTransportServiceComponent.
     *
     * @param carbonRuntime The reference being passed through MqttTransportServiceComponent
     */
    public void setCarbonRuntime(CarbonRuntime carbonRuntime) {
        this.carbonRuntime = carbonRuntime;
    }

    public void setAndesInstance(Andes andesInstance) {
        this.andesInstance = andesInstance;
    }

    public BundleContext getContext() {
        return context;
    }

    public void setContext(BundleContext context) {
        this.context = context;
    }
}
