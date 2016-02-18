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

import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.carbon.andes.services.types.Queue;
import org.wso2.carbon.andes.services.utils.QueueManagementConstants;
import org.wso2.msf4j.Microservice;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 * StockQuote microservice.
 */
@Component(
        name = "org.wso2.msf4j.stockquote.StockQuoteService",
        service = Microservice.class,
        immediate = true
)
@Path("/stockquote")
public class StockQuoteService implements Microservice {

    private static final Logger log = LoggerFactory.getLogger(StockQuoteService.class);
    private Map<String, Stock> stockQuotes = new HashMap<String, Stock>();

    /**
     * Add initial stocks IBM, GOOG, AMZN.
     */
    public StockQuoteService() {
        stockQuotes.put("IBM", new Stock("IBM", "International Business Machines", 149.62, 150.78, 149.18));
        stockQuotes.put("GOOG", new Stock("GOOG", "Alphabet Inc.", 652.30, 657.81, 643.15));
        stockQuotes.put("AMZN", new Stock("AMZN", "Amazon.com", 548.90, 553.20, 543.10));
    }

    @Activate
    protected void activate(BundleContext bundleContext) {
        // Nothing to do
    }

    @Deactivate
    protected void deactivate(BundleContext bundleContext) {
        // Nothing to do
    }


    /**
     * Retrieve a stock for a given symbol.
     * http://localhost:8080/stockquote/IBM
     *
     * @param symbol Stock symbol will be taken from the path parameter.
     * @return
     */
    @GET
    @Path("/{symbol}")
    @Produces({"application/json", "text/xml"})
    public Response getQuote(@PathParam("symbol") String symbol) {
        Stock stock = stockQuotes.get(symbol);
        return (stock == null) ?
               Response.status(Response.Status.NOT_FOUND).build() :
               Response.status(Response.Status.OK).entity(stock).build();
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
            List<Queue> queueList = new ArrayList<>();
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            try {
                ObjectName objectName =
                        new ObjectName("org.wso2.andes:type=QueueManagementInformation," +
                                       "name=QueueManagementInformation");
                Object result = mBeanServer.getAttribute(objectName, QueueManagementConstants
                        .QUEUES_COUNT_MBEAN_ATTRIBUTE);
                if (result != null) {
                    Map<String, Integer> queueCountMap = (Map<String, Integer>) result;
                    for (Map.Entry<String, Integer> entry : queueCountMap.entrySet()) {
                        Queue queue = new Queue();
                        queue.setQueueName(entry.getKey());
                        queue.setMessageCount(entry.getValue());
                        queueList.add(queue);
                    }
                }
            } catch (MalformedObjectNameException | ReflectionException | MBeanException
                                                                                        | InstanceNotFoundException e) {
                return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
            } catch (AttributeNotFoundException e) {
                return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
            }
            return Response.status(Response.Status.OK).entity(queueList).build();
        }
                       return Response.status(Response.Status.NOT_FOUND).build();

        //        Stock stock = stockQuotes.get(symbol);
        //        return (stock == null) ?
        //               Response.status(Response.Status.NOT_FOUND).build() :
        //               Response.status(Response.Status.OK).entity(stock).build();
    }

    /**
     * Add a new stock.
     * curl -v -X POST -H "Content-Type:application/json" \
     * -d '{"symbol":"BAR","name": "Bar Inc.", \
     * "last":149.62,"low":150.78,"high":149.18,
     * "createdByHost":"10.100.1.192"}' \
     * http://localhost:8080/stockquote
     *
     * @param stock Stock object will be created from the request Json body.
     */
    @POST
    @Consumes("application/json")
    public void addStock(Stock stock) {
        stockQuotes.put(stock.getSymbol(), stock);
    }

    /**
     * Retrieve all stocks.
     * http://localhost:8080/stockquote/all
     *
     * @return All stocks will be sent to the client as Json/xml
     * according to the Accept header of the request.
     */
    @GET
    @Path("/all")
    @Produces({"application/json", "text/xml"})
    public Stocks getAllStocks() {
        return new Stocks(stockQuotes.values());
    }

    @Override
    public String toString() {
        return "StockQuoteService-OSGi-bundle";
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
