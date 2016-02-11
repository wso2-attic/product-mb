package org.wso2.carbon.andes.internal;

import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.wso2.andes.server.BrokerOptions;
import org.wso2.andes.server.Main;
import org.wso2.carbon.andes.Greeter;
import org.wso2.carbon.andes.GreeterImpl;
import org.wso2.carbon.kernel.CarbonRuntime;
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
     * This is the activation method of AndesServiceComponent. This will be called when its references are
     * satisfied.
     *
     * @param bundleContext the bundle context instance of this bundle.
     * @throws Exception this will be thrown if an issue occurs while executing the activate method
     */
    @Activate
    protected void start(BundleContext bundleContext) throws Exception {
        logger.info("Service Component is activated");
        System.setProperty(BrokerOptions.ANDES_HOME, "advanced");
        String[] args = {"-p" + 5672, "-s" + 8672, "-q" + 1883};
        Main.main(args);
        // Register GreeterImpl instance as an OSGi service.
        serviceRegistration = bundleContext.registerService(Greeter.class.getName(), new GreeterImpl("WSO2"), null);
    }

    /**
     * This is the deactivation method of AndesServiceComponent. This will be called when this component
     * is being stopped or references are satisfied during runtime.
     *
     * @throws Exception this will be thrown if an issue occurs while executing the de-activate method
     */
    @Deactivate
    protected void stop() throws Exception {
        logger.info("Service Component is deactivated");

        // Unregister Greeter OSGi service
        serviceRegistration.unregister();
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
    protected void setHazelcastInstance(CarbonRuntime carbonRuntime) {
    }

    /**
     * This is the unbind method which gets called at the un-registration of CarbonRuntime OSGi service.
     *
     * @param carbonRuntime The CarbonRuntime instance registered by Carbon Kernel as an OSGi service
     */
    protected void unsetCarbonRuntime(CarbonRuntime carbonRuntime) {
    }
}
