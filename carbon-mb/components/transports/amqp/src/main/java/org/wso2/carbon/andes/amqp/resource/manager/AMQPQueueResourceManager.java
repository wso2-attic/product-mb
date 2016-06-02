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

package org.wso2.carbon.andes.amqp.resource.manager;

import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesQueue;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.kernel.disruptor.inbound.InboundQueueEvent;
import org.wso2.andes.server.resource.manager.DefaultResourceHandler;
import org.wso2.carbon.andes.amqp.internal.AMQPComponentDataHolder;

import java.lang.management.ManagementFactory;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

/**
 * AMQP resource handler for queues.
 */
public class AMQPQueueResourceManager extends DefaultResourceHandler {
    private final ProtocolType protocolType;
    private final DestinationType destinationType;
    private Andes andesInstance = AMQPComponentDataHolder.getInstance().getAndesInstance();

    public AMQPQueueResourceManager(ProtocolType protocolType, DestinationType destinationType) throws AndesException {
        super(protocolType, destinationType);
        this.protocolType = protocolType;
        this.destinationType = destinationType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteDestinations() throws AndesException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesQueue createDestination(String destinationName, String userName) throws AndesException {
        InboundQueueEvent createQueueInboundEvent = new InboundQueueEvent(
                destinationName, (userName != null) ? userName : "null", false, true, protocolType, destinationType);
        andesInstance.createQueue(createQueueInboundEvent);

        return super.getDestination(destinationName);
//        new InboundBindingEvent("amqp.direct", AMQPUtils.createAndesQueue(createQueueInboundEvent), )
//        andesInstance.addBinding();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteDestination(String destinationName) throws AndesException {
        try {
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

            ObjectName objectName = new ObjectName("org.wso2.andes:type=VirtualHost.VirtualHostManager," +
                                                   "VirtualHost=\"carbon\"");
            String operationName = "deleteQueue";

            Object[] parameters = new Object[]{destinationName};
            String[] signature = new String[]{String.class.getName()};

            mBeanServer.invoke(objectName, operationName, parameters, signature);
        } catch (MalformedObjectNameException | InstanceNotFoundException | ReflectionException | MBeanException e) {
            throw new AndesException("Error occurred while deleting queue : \"" + destinationName + "\".", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessages(String destinationName) throws AndesException {
    }
}
