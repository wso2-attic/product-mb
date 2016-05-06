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

package org.wso2.carbon.andes.tests.unit.rest.service;

import io.netty.handler.codec.http.HttpMethod;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.wso2.carbon.andes.services.AndesService;
import org.wso2.carbon.andes.services.DestinationManagerService;
import org.wso2.carbon.andes.services.DestinationManagerServiceImpl;
import org.wso2.carbon.andes.services.beans.DestinationManagementBeans;
import org.wso2.carbon.andes.services.exceptions.DestinationManagerException;
import org.wso2.msf4j.MicroservicesRunner;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import static org.mockito.Mockito.when;

/**
 * Test cases related to responses received from the REST service when managing destination resources.
 */
public class DestinationTestCase {
    private static final int PORT = 7575;
    private static final String BASE_URL = "http://localhost:" + PORT;
    DestinationManagerService destinationManagerService;

    /**
     * Initializing method.
     */
    @BeforeTest
    public void init() {
        destinationManagerService = new DestinationManagerServiceImpl();
    }

    /**
     * Invokes the service to get a method. Method will return a null. Therefore a 404 is expected.
     *
     * @throws DestinationManagerException
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void testGetDestination() throws DestinationManagerException, IOException, URISyntaxException,
            InterruptedException {
        String queueName = "MyQueue";
        DestinationManagementBeans destinationManagementBeans = Mockito.mock(DestinationManagementBeans.class);
        when(destinationManagementBeans.getDestination("amqp", "queue", queueName)).thenReturn(null);

        DestinationManagerServiceImpl destinationManagerService = new DestinationManagerServiceImpl
                (destinationManagementBeans);

        AndesService andesService = new AndesService();
        andesService.setDestinationManagerService(destinationManagerService);

        MicroservicesRunner microservicesRunner = new MicroservicesRunner(PORT).deploy(andesService);
        microservicesRunner.start();

        URL url = URI.create(BASE_URL).resolve("/mb/v1.0.0/amqp/destination-type/queue/name/MyQueue").toURL();
        HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
        urlConn.setRequestMethod(HttpMethod.GET.name());
        Assert.assertEquals(urlConn.getResponseCode(), 404, "Invalid status code received when creating a valid " +
                                                            "destination : " + url.toString());

        microservicesRunner.stop();

    }
}
