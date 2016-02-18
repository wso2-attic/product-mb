package org.wso2.carbon.andes.mqtt.internal;

import org.dna.mqtt.moquette.server.Server;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.wso2.andes.kernel.Andes;

import java.util.logging.Logger;

/**
 * Declarative service component for MQTT.
 * This handles initialization of the transport
 */
@Component(
        name = "org.wso2.carbon.andes.mqtt.internal.MqttTransportServiceComponent",
        immediate = true
)
public class MqttTransportServiceComponent {

    Logger logger = Logger.getLogger(MqttTransportServiceComponent.class.getName());
    private ServiceRegistration mqttTransportService;
    //TODO we need to change this to get from the configuration
    //This will be a temporary measure
    private static final int MQTT_PORT = 1883;
    //The running MQTT server instance
    private Server mqttServer = null;

    /**
     * This is the activation method of MqttTransportServiceComponent. This will be called when its references are
     * satisfied.
     *
     * @param bundleContext the bundle context instance of this bundle.
     * @throws Exception this will be thrown if an issue occurs while executing the activate method
     */
    @Activate
    protected void start(BundleContext bundleContext) throws Exception {
        //TODO this is a bad way of starting the service, without registering a service
        //This is temporary
        mqttServer = new Server();
        mqttServer.startServer(MQTT_PORT);
    }

    /**
     * This is the deactivation method of MqttTransportServiceComponent. This will be called when this component
     * is being stopped or references are satisfied during runtime.
     *
     * @throws Exception this will be thrown if an issue occurs while executing the de-activate method
     */
    @Deactivate
    protected void stop() throws Exception {
        logger.info("MqttTransportServiceComponent deactivated");

        //We stop the server when the bundle is deactivated
        if (null != mqttServer) {
            mqttServer.stopServer();
        }

        // Unregister Greeter OSGi service
        mqttTransportService.unregister();
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
