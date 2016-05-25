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

import com.hazelcast.core.HazelcastInstance;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.security.AndesAuthenticationManager;
import org.wso2.andes.server.BrokerOptions;
import org.wso2.andes.server.Main;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.server.registry.ApplicationRegistry;
import org.wso2.carbon.andes.internal.config.QpidServiceImpl;
import org.wso2.carbon.datasource.core.api.DataSourceService;
import org.wso2.carbon.kernel.CarbonRuntime;
import org.wso2.carbon.kernel.utils.Utils;
import org.wso2.carbon.security.caas.user.core.service.RealmService;


/**
 * Service component to consume CarbonRuntime instance which has been registered as an OSGi service
 * by Carbon Kernel.
 *
 * @since 3.5.0-SNAPSHOT
 */
@Component(
        name = "org.wso2.carbon.andes.internal.AndesServiceComponent",
        immediate = true,
        property = {
                "componentName=andes-component"
        }
)
public class AndesServiceComponent {

    private static final Logger log = LoggerFactory.getLogger(AndesServiceComponent.class);
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
        int offset = AndesDataHolder.getInstance().getCarbonRuntime().getConfiguration().getPortsConfig().getOffset();
        AndesConfigurationManager.initialize(offset);

        //Load qpid specific configurations
        QpidServiceImpl qpidServiceImpl = new QpidServiceImpl("carbon");
        qpidServiceImpl.loadConfigurations();

        // set message store and andes context store related configurations
        // TODO: C5 migration - Use data holder after moving core to product
        HazelcastAgent.getInstance().init(AndesDataHolder.getInstance().getCarbonHazelcastAgent());

        AndesContext andesContext = AndesContext.getInstance();
        andesContext.setClusteringEnabled(true);
        andesContext.constructStoreConfiguration();
        andesContext.setAndesAuthenticationManager(new AndesAuthenticationManager("CarbonSecurityConfig"));

        System.setProperty(BrokerOptions.ANDES_HOME, Utils.getCarbonConfigHome() + "/qpid/");
        String[] args = {"-p" + qpidServiceImpl.getAMQPPort(), "-s" + qpidServiceImpl.getAMQPSSLPort()};
        Main.main(args);
        Runtime.getRuntime().removeShutdownHook(ApplicationRegistry.getShutdownHook());
        serviceRegistration = bundleContext.registerService(Andes.class.getName(), Andes.getInstance(), null);
        log.info("Andes service component activated");
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

        log.info("Andes Service Component is deactivated");
    }

    /**
     * This bind method will be called when {@link HazelcastInstance} OSGi service is registered.
     *
     * @param hazelcastInstance The {@link HazelcastInstance} instance registered by Carbon Kernel as an OSGi
     *                             service
     */
    @Reference(
            name = "carbon.hazelcast.agent",
            service = HazelcastInstance.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetHazelcastOSGiInstance"
    )
    protected void setHazelcastOSGiInstance(HazelcastInstance hazelcastInstance) {
        AndesDataHolder.getInstance().setHazelcastInstance(hazelcastInstance);
    }

    /**
     * This is the unbind method which gets called at the un-registration of {@link HazelcastInstance} OSGi service.
     *
     * @param hazelcastInstance The {@link HazelcastInstance} instance registered by Carbon Kernel as an OSGi
     *                             service
     */
    protected void unsetHazelcastOSGiInstance(HazelcastInstance hazelcastInstance) {
        AndesDataHolder.getInstance().setHazelcastInstance(null);
    }

    /**
     * This bind method will be called when Carbon Data Source OSGI service is registered.
     *
     * @param dataSourceService The data source service instance created
     */
    @Reference(
            name = "org.wso2.carbon.datasource.DataSourceService",
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
        AndesDataHolder.getInstance().setCarbonRuntime(carbonRuntime);
    }

    /**
     * This is the unbind method which gets called at the un-registration of CarbonRuntime OSGi service.
     *
     * @param carbonRuntime The CarbonRuntime instance registered by Carbon Kernel as an OSGi service
     */
    protected void unsetCarbonRuntime(CarbonRuntime carbonRuntime) {
        AndesDataHolder.getInstance().setCarbonRuntime(null);
    }

    @Reference(
            name = "org.wso2.carbon.security.CarbonRealmServiceImpl",
            service = RealmService.class,
            cardinality = ReferenceCardinality.AT_LEAST_ONE,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unregisterCarbonRealm"
    )
    public void registerCarbonRealm(RealmService realmService) {
        AndesDataHolder.getInstance().setRealmService(realmService);
        AndesContext.getInstance().setRealmService(realmService);
    }

    public void unregisterCarbonRealm(RealmService carbonRealmService) {
        AndesDataHolder.getInstance().setRealmService(null);
        AndesContext.getInstance().setRealmService(null);
    }
}
