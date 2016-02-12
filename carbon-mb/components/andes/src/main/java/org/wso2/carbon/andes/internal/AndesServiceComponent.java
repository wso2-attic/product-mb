/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.wso2.carbon.datasource.core.api.DataSourceService;
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
     * This is the activation method of {@link AndesServiceComponent}. This will be called when its references are
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
        QpidServiceImpl qpidServiceImpl = new QpidServiceImpl("carbon");
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
        // Unregister Greeter OSGi service
        serviceRegistration.unregister();

        logger.info("Andes Service Component is deactivated");
    }

    /**
     * This bind method will be called when {@link CarbonHazelcastAgent} OSGi service is registered.
     *
     * @param carbonHazelcastAgent The {@link CarbonHazelcastAgent} instance registered by Carbon Kernel as an OSGi
     *                             service
     */
    @Reference(
            name = "carbon-hazelcast-agent",
            service = CarbonHazelcastAgent.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetCarbonHazelcastAgent"
    )
    protected void setCarbonHazelcastAgent(CarbonHazelcastAgent carbonHazelcastAgent) {
        AndesDataHolder.getInstance().setHazelcastAgent(carbonHazelcastAgent);
    }

    /**
     * This is the unbind method which gets called at the un-registration of {@link CarbonHazelcastAgent} OSGi service.
     *
     * @param carbonHazelcastAgent The {@link CarbonHazelcastAgent} instance registered by Carbon Kernel as an OSGi
     *                             service
     */
    protected void unsetCarbonHazelcastAgent(CarbonHazelcastAgent carbonHazelcastAgent) {
        AndesDataHolder.getInstance().setHazelcastAgent(null);
    }


    /**
     * This bind method will be called when Carbon Data Source OSGI service is registered.
     *
     * @param dataSourceService The data source service instance created
     */
    @Reference(
            name = "carbon.datasource.service",
            service = DataSourceService.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetDataSourceService"
    )
    protected void setDataSourceService(DataSourceService dataSourceService) {
        AndesDataHolder.getInstance().setDataSourceService(dataSourceService);
    }

    /**
     * This will be at the un-registration of the Carbon Data Source OSGI service.
     *
     * @param dataSourceService The instance to un-register
     */
    protected void unsetDataSourceService(DataSourceService dataSourceService) {
        AndesDataHolder.getInstance().setDataSourceService(null);
    }
}
