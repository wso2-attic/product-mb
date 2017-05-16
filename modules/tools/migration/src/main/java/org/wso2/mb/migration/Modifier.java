/*
 * Copyright (c)2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.mb.migration;

/**
 * Modifier accepts string such as binding details and message router details that were stored in WSO2MB 3.1.0 and
 * modifies them to the format that is accepted by WSO2MB 3.2.0.
 */
class Modifier {

    /**
     * Modify binding info to be compatible with MB 3.2.0. Extracts the queue name, message router name and the
     * routing key and simply adds that as binding details.
     *
     * @param bindingInfo binding information as stored in WSO2MB 3.1.0 to be modified
     * @return modified binding info that is compatible with WSO2MB 3.2.0
     * @throws MigrationException when binding information is null
     */
    String modifyBinding(String bindingInfo) throws MigrationException {

        String boundMessageRouter;
        String boundQueueName;
        String bindingKey;

        if (null != bindingInfo) {
            String[] parts = bindingInfo.split("\\|");
            if (parts.length != 3) {
                throw new MigrationException("Cannot recognize binging info: " + bindingInfo);
            }

            /* Create binding details in the format of
             "boundMessageRouter=amq.direct, boundQueueName=queue_name,bindingKey=routing_key"*/
            boundMessageRouter = parts[0].split("&")[1];
            String[] boundQueueInfo = parts[1].split(",");
            boundQueueName = boundQueueInfo[0].split("=")[1];
            bindingKey = parts[2].split("&")[1];
            return "boundMessageRouter=" + boundMessageRouter + ",boundQueueName=" + boundQueueName
                   + ",bindingKey=" + bindingKey;
        } else {
            throw new MigrationException("Binding cannot be null");
        }
    }

    /**
     * Given the queue name and the message router to be bound, this method creates the binding details to be
     * compatible with WSO2MB 3.2.0.
     *
     * @param queueName  the queue name to be bound to
     * @param routerName the message router to which the queue is bound
     * @return modified queue info
     */
    String createBindingDetails(String queueName, String routerName) {
        return "boundMessageRouter=" + routerName + ",boundQueueName=" + queueName
               + ",bindingKey=" + queueName;
    }

    /**
     * Given the router name, the type of the router and the auto delete mode, this
     * method creates the message router(exchange) details to be compatible with WSO2MB 3.2.0.
     *
     * @param routerName the name of the message router
     * @param type       the type of the message router
     * @param autoDelete defines if auto deletion is enabled or disabled
     * @return modified message router details
     */
    String createExchangeDetails(String routerName, String type, String autoDelete) {
        return "messageRouterName=" + routerName + ",type=" + type
               + ",autoDeletefalse=" + autoDelete;
    }
}
