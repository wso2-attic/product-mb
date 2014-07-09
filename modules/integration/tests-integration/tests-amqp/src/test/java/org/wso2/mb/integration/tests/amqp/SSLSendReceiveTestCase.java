/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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

package org.wso2.mb.integration.tests.amqp;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.mb.integration.common.clients.AndesClient;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;

import java.io.File;


/**
 * send messages using SSL and receive messages using SSL
 */
public class SSLSendReceiveTestCase {

    @BeforeClass
    public void prepare() {
        System.out.println("=========================================================================");
        AndesClientUtils.sleepForInterval(15000);
    }

    @Test(groups = {"wso2.mb", "queue", "security"})
    public void performSingleQueueSendReceiveTestCase() {
        Integer sendCount = 100;
        Integer runTime = 20;
        Integer expectedCount = 100;
        String keyStorePath = System.getProperty("carbon.home") + File.separator + "repository" + File.separator +
                "resources" + File.separator +"security" + File.separator +"wso2carbon.jks";
        String trustStorePath = System.getProperty("carbon.home") + File.separator + "repository" + File.separator +
                "resources" + File.separator +"security" + File.separator +"client-truststore.jks";
        String keyStorePassword = "wso2carbon";
        String trustStorePassword = "wso2carbon";
        String sslConnectionURL = "amqp://admin:admin@carbon/carbon?brokerlist='tcp://localhost:8672?ssl='true'&ssl_cert_alias='RootCA'&trust_store='"+trustStorePath+"'&trust_store_password='"+trustStorePassword+"'&key_store='"+keyStorePath+"'&key_store_password='"+keyStorePassword+"''";

        AndesClient receivingClient = new AndesClient("receive", "127.0.0.1:8672", "queue:SSLSingleQueue",
                "100", "false", runTime.toString(), expectedCount.toString(),
                "1", "listener=true,ackMode=1,delayBetweenMsg=0,stopAfter="+expectedCount, sslConnectionURL);

        receivingClient.startWorking();

        AndesClient sendingClient = new AndesClient("send", "127.0.0.1:5672", "queue:SSLSingleQueue", "100", "false",
                runTime.toString(), sendCount.toString(), "1",
                "ackMode=1,delayBetweenMsg=0,stopAfter="+sendCount, sslConnectionURL);

        sendingClient.startWorking();

        boolean receiveSuccess = AndesClientUtils.waitUntilMessagesAreReceived(receivingClient, expectedCount, runTime);

        boolean sendSuccess = AndesClientUtils.getIfSenderIsSuccess(sendingClient,sendCount);

        if(receiveSuccess && sendSuccess) {
            System.out.println("TEST PASSED");
        }  else {
            System.out.println("TEST FAILED");
        }

        Assert.assertEquals((receiveSuccess && sendSuccess), true);
    }
}
