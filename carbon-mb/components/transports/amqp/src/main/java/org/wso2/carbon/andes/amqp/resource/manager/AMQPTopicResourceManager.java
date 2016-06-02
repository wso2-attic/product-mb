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

import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesQueue;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.andes.server.resource.manager.DefaultResourceHandler;

/**
 * AMQP resource handler for topics.
 */
public class AMQPTopicResourceManager extends DefaultResourceHandler {

    public AMQPTopicResourceManager(ProtocolType protocolType, DestinationType destinationType) {
        super(protocolType, destinationType);
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
    public AndesQueue createDestination(String s, String s1) throws AndesException {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteDestination(String s) throws AndesException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessages(String s) throws AndesException {
    }
}
