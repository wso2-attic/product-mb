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
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolInfo;
import org.wso2.andes.subscription.LocalDurableTopicSubscriptionStore;
import org.wso2.andes.subscription.QueueSubscriptionStore;
import org.wso2.carbon.andes.transports.config.MqttSecuredTransportProperties;
import org.wso2.carbon.andes.transports.config.MqttTransportConfiguration;
import org.wso2.carbon.andes.transports.config.MqttTransportProperties;
import org.wso2.carbon.andes.transports.config.YAMLTransportConfigurationBuilder;
import org.wso2.carbon.andes.transports.mqtt.MqttSSLServer;
import org.wso2.carbon.andes.transports.mqtt.MqttServer;
import org.wso2.carbon.andes.transports.mqtt.Util;
import org.wso2.carbon.andes.transports.mqtt.adaptors.andes.subscriptions.MQTTopicSubscriptionBitMapStore;
import org.wso2.carbon.andes.transports.server.Server;
import org.wso2.carbon.kernel.CarbonRuntime;
import org.wso2.carbon.kernel.startupresolver.CapabilityProvider;
import org.wso2.carbon.kernel.startupresolver.RequiredCapabilityListener;
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
        name = "org.wso2.carbon.andes.mqtt.internal.MqttTransportServiceComponent",
        immediate = true,
        property = {"componentName=mqtt-transport-component"}
)
@SuppressWarnings("unused")
public class MqttTransportServiceComponent implements RequiredCapabilityListener, CapabilityProvider {

    private static final Log log = LogFactory.getLog(MqttTransportServiceComponent.class);

    /**
     * Provides the name of the protocol addressed by the transport
     */
    private static final String PROTOCOL = "MQTT";

    /**
     * Holds the number of transports which will be initialized through the service component
     */
    private static final int TRANSPORT_COUNT = 2;


    /**
     * Processors configuration to adhere to config changes provided by carbon
     *
     * @param ctx server context which holds server initialization information
     */
    private void processConfiguration(MqttTransportProperties ctx) {
        //If an offset has being defined at carbon level, we need to add that here
        int offset = MqttTransportDataHolder.getInstance().getCarbonRuntime().getConfiguration().getPortsConfig()
                .getOffset();
        ctx.setPort(ctx.getPort() + offset);
    }

    /**
     * This is the activation method of MqttTransportServiceComponent. This will be called when its references are
     * satisfied.
     *
     * @param bundleContext the bundle context instance of this bundle.
     * @throws Exception this will be thrown if an issue occurs while executing the activate method
     */
    @Activate
    protected void start(BundleContext bundleContext) throws Exception {

        MqttTransportDataHolder.getInstance().getAndesInstance().registerProtocolType(createProtocolInformation());

        MqttTransportConfiguration mqttTransportConfiguration = YAMLTransportConfigurationBuilder.readConfiguration();

        MqttTransportDataHolder.getInstance().setContext(bundleContext);
        //Default MQTT transport
        MqttTransportProperties mqttTransportProperties = mqttTransportConfiguration.getMqttTransportProperties();
        processConfiguration(mqttTransportProperties);
        mqttTransportProperties.setProtocol(PROTOCOL);
        Server mqttServer = new MqttServer();

        //Secured transport properties
        MqttSecuredTransportProperties mqttSecuredTransportProperties = mqttTransportConfiguration
                .getMqttSecuredTransportProperties();
        processConfiguration(mqttSecuredTransportProperties);
        mqttSecuredTransportProperties.setProtocol(PROTOCOL);

        MqttSSLServer securedMqttServer = new MqttSSLServer(Util.getSSLConfig(mqttSecuredTransportProperties));

        //Creates a transport from the given configuration
        MqttTransport transport = new MqttTransport(mqttTransportProperties, mqttServer);
        MqttTransport securedTransport = new MqttTransport(mqttSecuredTransportProperties, securedMqttServer);


        bundleContext.registerService(CarbonTransport.class, transport, null);
        bundleContext.registerService(CarbonTransport.class, securedTransport, null);

        log.info("MQTT Server Component Activated");
    }


    /**
     * Create protocol information for MQTT.
     *
     * @return The protocol information object.
     * @throws AndesException
     */
    private ProtocolInfo createProtocolInformation() throws AndesException {
        ProtocolInfo protocolInfo = new ProtocolInfo("MQTT", "default");

        protocolInfo.addClusterSubscriptionStore(DestinationType.TOPIC, new MQTTopicSubscriptionBitMapStore());
        protocolInfo.addClusterSubscriptionStore(DestinationType.DURABLE_TOPIC, new MQTTopicSubscriptionBitMapStore());

        protocolInfo.addLocalSubscriptionStore(DestinationType.TOPIC, new QueueSubscriptionStore());
        protocolInfo.addLocalSubscriptionStore(DestinationType.DURABLE_TOPIC, new LocalDurableTopicSubscriptionStore());

        return protocolInfo;
    }

    /**
     * This is the deactivation method of MqttTransportServiceComponent. This will be called when this component
     * is being stopped or references are satisfied during runtime.
     *
     * @throws Exception this will be thrown if an issue occurs while executing the de-activate method
     */
    @Deactivate
    protected void stop() throws Exception {

        MqttTransportDataHolder.getInstance().getAndesInstance().unregisterProtocolType(createProtocolInformation());

        if (log.isDebugEnabled()) {
            log.debug("Stopping MqttTransportServiceComponent");
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

    @Override
    public void onAllRequiredCapabilitiesAvailable() {
        MqttTransportDataHolder.getInstance().getContext().registerService(MqttTransportServiceComponent.class, this,
                null);
    }

    @Override
    public int getCount() {
        return TRANSPORT_COUNT;
    }
}
