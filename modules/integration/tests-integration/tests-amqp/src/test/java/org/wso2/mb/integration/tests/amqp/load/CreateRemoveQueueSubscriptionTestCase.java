/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * under the License. and limitations under the License.
 */

package org.wso2.mb.integration.tests.amqp.load;

import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesQueueSubscriber;

public class CreateRemoveQueueSubscriptionTestCase {
    @Test
    public void testCreateRemoveSubscription() throws Exception {
        AndesQueueSubscriber queueClient = new AndesQueueSubscriber.Builder("admin", "admin",
                                                                            "TestQueue").build();
        queueClient.connect();

        // TODO Check the number of subscribers in broker

        queueClient.disconnect();

        // TODO Check the number of subscribers in broker
    }
}
