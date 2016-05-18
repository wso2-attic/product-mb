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

import org.osgi.service.component.annotations.Component;
import org.wso2.carbon.kernel.startupresolver.CapabilityProvider;

/**
 * This class signals Startup Order Resolver module in kernel that this bundle provides
 * two services of type {@link org.wso2.carbon.andes.transports.mqtt.internal.MqttTransport}
 */
@Component(
        name = "org.wso2.carbon.andes.transports.mqtt.internal.MqttTransportCapabilityProvider",
        immediate = true,
        property = {
                "capabilityName=mqtt-transport-capability-provider"
        }
)
public class MqttTransportCapabilityProvider implements CapabilityProvider {

    /**
     * Holds the number of transports which will be initialized through the service component
     */
    private static final int TRANSPORT_COUNT = 2;

    /**
     * {@inheritDoc}
     */
    @Override
    public int getCount() {
        return TRANSPORT_COUNT;
    }
}
