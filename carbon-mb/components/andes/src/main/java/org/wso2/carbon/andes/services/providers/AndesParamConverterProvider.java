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

package org.wso2.carbon.andes.services.providers;

import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.carbon.kernel.CarbonRuntime;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.ext.ParamConverter;
import javax.ws.rs.ext.ParamConverterProvider;
import javax.ws.rs.ext.Provider;

/**
 * Andes Param Converter Provider. Currently not being used.
 */
//@Component(
//        name = "org.wso2.carbon.andes.service.AndesParamConverterProvider",
//        service = ParamConverterProvider.class,
//        immediate = true
//)
@Provider
public class AndesParamConverterProvider implements ParamConverterProvider {
    private static final Logger log = LoggerFactory.getLogger(AndesParamConverterProvider.class);
    private ServiceRegistration serviceRegistration;
    private final ProtocolTypeConverter protocolTypeConverter = new ProtocolTypeConverter();
    private final DestinationTypeConverter destinationTypeConverter = new DestinationTypeConverter();

    @Activate
    protected void start(BundleContext bundleContext) {
        log.info("AndesParamConverter is activated");
        serviceRegistration = bundleContext.registerService(AndesParamConverterProvider.class.getName(), this, null);
    }

    @Deactivate
    protected void stop(BundleContext bundleContext) {
        log.info("AndesParamConverter is deactivated");
        serviceRegistration.unregister();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> ParamConverter<T> getConverter(Class<T> rawType, Type genericType, Annotation[] annotations) {
        if (rawType.getTypeName().equals(ProtocolType.class.getName())) {
            return (ParamConverter<T>) protocolTypeConverter;
        } else if (rawType.getName().equals(DestinationType.class.getName())) {
            return (ParamConverter<T>) destinationTypeConverter;
        }
        return null;
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
        //        DataHolder.getInstance().setCarbonRuntime(carbonRuntime);
    }

    /**
     * This is the unbind method which gets called at the un-registration of CarbonRuntime OSGi service.
     *
     * @param carbonRuntime The CarbonRuntime instance registered by Carbon Kernel as an OSGi service
     */
    protected void unsetCarbonRuntime(CarbonRuntime carbonRuntime) {
        //        DataHolder.getInstance().setCarbonRuntime(null);
    }
}
