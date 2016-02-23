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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.wso2.andes.kernel.Andes;
import org.wso2.carbon.andes.transports.config.NettyServerContext;
import org.wso2.carbon.andes.transports.config.YAMLTransportConfigurationBuilder;
import org.wso2.carbon.kernel.CarbonRuntime;
import org.wso2.carbon.kernel.transports.CarbonTransport;

/**
 * <p>
 * Service component to consume CarbonRuntime instance which has been registered as an OSGi service
 * by Carbon Kernel.
 * </p>
 * <p>
 * This service will be registered to enable MQTT transport
 * </p>
 */
@Component(
        name = "org.wso2.carbon.andes.mqtt.internal.NettyTransportServiceComponent",
        immediate = true,
        property = {"component-key=mqtt-transport"}
)
@SuppressWarnings("unused")
public class NettyTransportServiceComponent {

    private static final Log log = LogFactory.getLog(NettyTransportServiceComponent.class);

    /**
     * Provides the name of the protocol addressed by the transport
     */
    private static final String PROTOCOL = "MQTT";


    /**
     * Processors configuration to adhere to config changes provided by carbon
     *
     * @param ctx server context which holds server initialization information
     */
    private void processConfiguration(NettyServerContext ctx) {
        //If an offset has being defined at carbon level, we need to add that here
        int offset = MqttTransportDataHolder.getInstance().getCarbonRuntime().getConfiguration().getPortsConfig()
                .getOffset();
        ctx.setPort(ctx.getPort() + offset);
    }

    /**
     * This is the activation method of NettyTransportServiceComponent. This will be called when its references are
     * satisfied.
     *
     * @param bundleContext the bundle context instance of this bundle.
     * @throws Exception this will be thrown if an issue occurs while executing the activate method
     */
    @Activate
    protected void start(BundleContext bundleContext) throws Exception {

        NettyServerContext nettyServerContext = YAMLTransportConfigurationBuilder.readConfiguration();
        processConfiguration(nettyServerContext);
        nettyServerContext.setProtocol(PROTOCOL);

        //Creates a transport from the given configuration
        MqttTransport transport = new MqttTransport(nettyServerContext);

        bundleContext.registerService(CarbonTransport.class, transport, null);

        log.info("MQTT Server Component Activate");
    }

    /**
     * This is the deactivation method of NettyTransportServiceComponent. This will be called when this component
     * is being stopped or references are satisfied during runtime.
     *
     * @throws Exception this will be thrown if an issue occurs while executing the de-activate method
     */
    @Deactivate
    protected void stop() throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("Stopping NettyTransportServiceComponent");
        }
    }

    /**
     * This bind method will be called when CarbonRuntime OSGi service is registered.
     *
     * @param carbonRuntime The CarbonRuntime instance registered by Carbon Kernel as an OSGi service
     */
    @Reference(
            name = "carbon.runtime.service",
            service = CarbonRuntime.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetCarbonRuntime"
    )
    protected void setCarbonRuntime(CarbonRuntime carbonRuntime) {
        MqttTransportDataHolder.getInstance().setCarbonRuntime(carbonRuntime);
    }

    /**
     * This is the unbind method which gets called at the un-registration of CarbonRuntime OSGi service.
     *
     * @param carbonRuntime The CarbonRuntime instance registered by Carbon Kernel as an OSGi service
     */
    protected void unsetCarbonRuntime(CarbonRuntime carbonRuntime) {
        MqttTransportDataHolder.getInstance().setCarbonRuntime(null);
    }

    /**
     * This bind method will be called when Andes OSGI service is registered.
     *
     * @param andesInstance The Andes instance registered
     */
    @Reference(
            name = "andes.instance",
            service = Andes.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetAndesInstance"
    )
    protected void setAndesInstance(Andes andesInstance) {
        MqttTransportDataHolder.getInstance().setAndesInstance(andesInstance);
    }

    /**
     * The unbind which gets called at the un-registration of Andes component.
     *
     * @param andesInstance provides the service component the component should interact with
     */
    protected void unsetAndesInstance(Andes andesInstance) {
        MqttTransportDataHolder.getInstance().setAndesInstance(null);
    }

}
