package org.wso2.carbon.andes.internal;

import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.server.BrokerOptions;
import org.wso2.andes.server.Main;
import org.wso2.andes.server.registry.ApplicationRegistry;
import org.wso2.carbon.andes.internal.config.QpidServiceImpl;
import org.wso2.carbon.hazelcast.CarbonHazelcastAgent;
import org.wso2.carbon.kernel.utils.Utils;

import java.util.logging.Logger;

/**
 * Service component to consume CarbonRuntime instance which has been registered as an OSGi service
 * by Carbon Kernel.
 *
 * @since 3.5.0-SNAPSHOT
 */
@Component(
        name = "org.wso2.carbon.andes.internal.AndesServiceComponent",
        immediate = true
)
public class AndesServiceComponent {

    Logger logger = Logger.getLogger(AndesServiceComponent.class.getName());
    private ServiceRegistration serviceRegistration;
    /**
     * This holds the configuration values
     */
    private QpidServiceImpl qpidServiceImpl;

    /**
     * This is the activation method of ServiceComponent. This will be called when its references are
     * satisfied.
     *
     * @param bundleContext the bundle context instance of this bundle.
     * @throws Exception this will be thrown if an issue occurs while executing the activate method
     */
    @Activate
    protected void start(BundleContext bundleContext) throws Exception {

        //Initialize AndesConfigurationManager
        AndesConfigurationManager.initialize(0);

        //Load qpid specific configurations
        qpidServiceImpl = new QpidServiceImpl("carbon");
        qpidServiceImpl.loadConfigurations();

        // set message store and andes context store related configurations
        AndesContext.getInstance().constructStoreConfiguration();

        System.setProperty(BrokerOptions.ANDES_HOME, Utils.getCarbonConfigHome() + "/qpid/");
        String[] args = {"-p" + qpidServiceImpl.getAMQPPort(), "-s" + qpidServiceImpl.getAMQPSSLPort(),
                "-q" + qpidServiceImpl.getMqttPort()};
        Main.main(args);
        Runtime.getRuntime().removeShutdownHook(ApplicationRegistry.getShutdownHook());
        logger.info("Andes service component activated");
    }

    /**
     * This is the deactivation method of ServiceComponent. This will be called when this component
     * is being stopped or references are satisfied during runtime.
     *
     * @throws Exception this will be thrown if an issue occurs while executing the de-activate method
     */
    @Deactivate
    protected void stop() throws Exception {
        logger.info("Andes Service Component is deactivated");

        // Unregister Greeter OSGi service
        serviceRegistration.unregister();
    }

    /**
     * This bind method will be called when CarbonRuntime OSGi service is registered.
     *
     * @param carbonHazelcastAgent The CarbonRuntime instance registered by Carbon Kernel as an OSGi service
     */
    @Reference(
            name = "carbon.hazelcast",
            service = CarbonHazelcastAgent.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetCarbonHazelcastAgent"
    )
    protected void setCarbonHazelcastAgent(CarbonHazelcastAgent carbonHazelcastAgent) {
        AndesDataHolder.getInstance().setHazelcastAgent(carbonHazelcastAgent);
    }

    /**
     * This is the unbind method which gets called at the un-registration of CarbonRuntime OSGi service.
     *
     * @param carbonHazelcastAgent The CarbonRuntime instance registered by Carbon Kernel as an OSGi service
     */
    protected void unsetCarbonHazelcastAgent(CarbonHazelcastAgent carbonHazelcastAgent) {
        AndesDataHolder.getInstance().setHazelcastAgent(null);
    }
}
