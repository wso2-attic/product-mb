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

package org.wso2.carbon.andes.services;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.carbon.andes.services.exceptions.QueueManagerException;
import org.wso2.carbon.andes.services.types.Queue;
import org.wso2.msf4j.Microservice;

import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 * Andes microservice.
 */
@Component(
        name = "org.wso2.carbon.andes.AndesService",
        service = Microservice.class,
        immediate = true
)
@Path("/mb")
public class AndesService implements Microservice {

    private static final Logger log = LoggerFactory.getLogger(AndesService.class);
    private static QueueManagerService queueManager;
    private static SubscriptionManagerService subscriptionManagerService;
    public AndesService() {
        queueManager = new QueueManagerServiceImpl();
        subscriptionManagerService = new SubscriptionManagerServiceImpl();
    }

    /**
     * Retrieve a stock for a given symbol.
     * http://localhost:8080/stockquote/IBM
     *
     * @return
     */
    @GET
    @Path("/v350/{protocol}/destination_type/{destination_type}")
    @Produces({"application/json", "text/xml"})
    public Response getAllQueues(@PathParam("protocol") String protocol,
                                 @PathParam("destination_type") String destinationType) {

        if (protocol.equalsIgnoreCase(ProtocolType.AMQP.name()) && destinationType.equals("queue")) {
            List<Queue> queueList;
            try {
                queueList = queueManager.getAllQueues();
                return Response.status(Response.Status.OK).entity(queueList).build();
            } catch (QueueManagerException e) {
                return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
            }
        }
        return Response.status(Response.Status.NOT_FOUND).build();
    }

    @Override
    public String toString() {
        return "AndesService-OSGi-bundle";
    }


    /**
     * This bind method will be called when CarbonRuntime OSGi service is registered.
     *
     * @param andesInstace The CarbonRuntime instance registered by Carbon Kernel as an OSGi service
     */
    @Reference(
            name = "carbon.andes.service",
            service = Andes.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetAndesRuntime"
    )
    protected void setAndesRuntime(Andes andesInstace) {
        //        DataHolder.getInstance().setCarbonRuntime(andesInstace);
    }

    /**
     * This is the unbind method which gets called at the un-registration of CarbonRuntime OSGi service.
     *
     * @param andesInstance The CarbonRuntime instance registered by Carbon Kernel as an OSGi service
     */
    protected void unsetAndesRuntime(Andes andesInstance) {

    }
}
