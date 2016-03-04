package org.wso2.carbon.andes.amqp.internal;

import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.wso2.andes.framing.ProtocolVersion;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolInfo;
import org.wso2.andes.subscription.LocalDurableTopicSubscriptionStore;
import org.wso2.andes.subscription.QueueSubscriptionStore;
import org.wso2.carbon.andes.amqp.subscription.AMQPTopicSubscriptionBitMapStore;
import org.wso2.carbon.kernel.CarbonRuntime;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Service component to consume CarbonRuntime instance which has been registered as an OSGi service
 * by Carbon Kernel.
 *
 * @since 3.5.0-SNAPSHOT
 */
@Component(
        name = "org.wso2.carbon.andes.amqp.internal.AMQPTransportServiceComponent",
        immediate = true
)
public class AMQPTransportServiceComponent {

    Logger logger = Logger.getLogger(AMQPTransportServiceComponent.class.getName());
    private ServiceRegistration serviceRegistration;

    /**
     * This is the activation method of ServiceComponent. This will be called when its references are
     * satisfied.
     *
     * @param bundleContext the bundle context instance of this bundle.
     * @throws Exception this will be thrown if an issue occurs while executing the activate method
     */
    @Activate
    protected void start(BundleContext bundleContext) throws Exception {

        // Register AMQP protocol on andes
        for (ProtocolInfo protocolInfo : getAMQPProtocolInformationList()) {
            DataHolder.getInstance().getAndesInstance().registerProtocolType(protocolInfo);
        }

        logger.info("AMQP Service Component is activated");
    }

    private ProtocolInfo createProtocolInfo(ProtocolVersion protocolVersion) throws AndesException {
        ProtocolInfo protocolInfo = new ProtocolInfo("AMQP", protocolVersion.toString());

        protocolInfo.addClusterSubscriptionStore(DestinationType.QUEUE, new QueueSubscriptionStore());
        protocolInfo.addClusterSubscriptionStore(DestinationType.TOPIC, new AMQPTopicSubscriptionBitMapStore());
        protocolInfo.addClusterSubscriptionStore(DestinationType.DURABLE_TOPIC, new AMQPTopicSubscriptionBitMapStore());

        protocolInfo.addLocalSubscriptionStore(DestinationType.QUEUE, new QueueSubscriptionStore());

        // Using queue subscription store for topics since wildcard matching is not required for local subscriptions
        protocolInfo.addLocalSubscriptionStore(DestinationType.TOPIC, new QueueSubscriptionStore());

        // Local durable topic subscription store is specific to local mode since subscriptions are stored
        // against their targetQueue in this store
        protocolInfo.addLocalSubscriptionStore(DestinationType.DURABLE_TOPIC, new LocalDurableTopicSubscriptionStore());

        return protocolInfo;
    }

    /**
     * Get list of protocol information for each version of AMQP protocol.
     *
     * @return List of protocol information objects
     * @throws AndesException
     */
    private List<ProtocolInfo> getAMQPProtocolInformationList() throws AndesException {
        List<ProtocolInfo> protocolInfos = new ArrayList<>();

        protocolInfos.add(createProtocolInfo(ProtocolVersion.v8_0));
        protocolInfos.add(createProtocolInfo(ProtocolVersion.v0_9));
        protocolInfos.add(createProtocolInfo(ProtocolVersion.v0_91));
        protocolInfos.add(createProtocolInfo(ProtocolVersion.v0_10));

        return protocolInfos;
    }

    /**
     * This is the deactivation method of ServiceComponent. This will be called when this component
     * is being stopped or references are satisfied during runtime.
     *
     * @throws Exception this will be thrown if an issue occurs while executing the de-activate method
     */
    @Deactivate
    protected void stop() throws Exception {

        for (ProtocolInfo protocolInfo : getAMQPProtocolInformationList()) {
            Andes.getInstance().unregisterProtocolType(protocolInfo);
        }

        // Unregister Greeter OSGi service
        serviceRegistration.unregister();

        logger.info("AMQP Service Component is deactivated");
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
        DataHolder.getInstance().setCarbonRuntime(carbonRuntime);
    }

    /**
     * This is the unbind method which gets called at the un-registration of CarbonRuntime OSGi service.
     *
     * @param carbonRuntime The CarbonRuntime instance registered by Carbon Kernel as an OSGi service
     */
    protected void unsetCarbonRuntime(CarbonRuntime carbonRuntime) {
        DataHolder.getInstance().setCarbonRuntime(null);
    }

    /**
     * This bind method will be called when Andes OSGI service is registered.
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
        DataHolder.getInstance().setAndesInstance(andesInstance);
    }

    /**
     * The unbind which gets called at the un-registration of Andes component.
     * @param andesInstance
     */
    protected void unsetAndesInstance(Andes andesInstance) {
        DataHolder.getInstance().setAndesInstance(null);
    }
}
