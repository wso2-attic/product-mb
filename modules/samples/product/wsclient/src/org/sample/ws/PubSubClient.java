/*
 * Copyright 2004,2005 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sample.ws;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;
import org.apache.axis2.AxisFault;
import org.apache.axis2.engine.AxisServer;
import org.wso2.carbon.event.client.broker.BrokerClient;
import org.wso2.carbon.event.client.broker.BrokerClientException;
import org.wso2.carbon.event.client.stub.generated.authentication.AuthenticationExceptionException;

import java.rmi.RemoteException;

public class PubSubClient {

    private AxisServer axisServer;
    private BrokerClient brokerClient;

    public void start() {
        try {

            System.setProperty("javax.net.ssl.trustStore", "../../repository/resources/security/wso2carbon.jks");
            System.setProperty("javax.net.ssl.trustStorePassword", "wso2carbon");

            this.axisServer = new AxisServer();
            this.axisServer.deployService(EventSinkService.class.getName());
            this.brokerClient = new BrokerClient("https://localhost:9443/services/EventBrokerService", "admin", "admin");

            // give time to start the simple http server
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }

        } catch (AxisFault axisFault) {
            System.out.println("Can not start the server");
        } catch (AuthenticationExceptionException e) {
            e.printStackTrace();
        }

    }

    public String subscribe() {
        // set the properties for ssl
        try {
            return this.brokerClient.subscribe("foo/bar" , "http://localhost:6060/axis2/services/EventSinkService/receive");
        } catch (BrokerClientException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void publish(){
        try {
            this.brokerClient.publish("foo/bar", getOMElementToSend());
        } catch (AxisFault axisFault) {
            axisFault.printStackTrace();
        }
    }

    public void unsubscribe(String subscriptionID){
        try {
            this.brokerClient.unsubscribe(subscriptionID);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }

    public void stop(){
        try {
            this.axisServer.stop();
        } catch (AxisFault axisFault) {
            axisFault.printStackTrace();
        }
    }

    public static void main(String[] args) {

        PubSubClient pubSubClient = new PubSubClient();
        pubSubClient.start();
        String subscriptionId = pubSubClient.subscribe();
        pubSubClient.publish();

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {}

        pubSubClient.unsubscribe(subscriptionId);
        pubSubClient.stop();

    }

    private OMElement getOMElementToSend() {
        OMFactory omFactory = OMAbstractFactory.getOMFactory();
        OMNamespace omNamespace = omFactory.createOMNamespace("http://ws.sample.org", "ns1");
        OMElement receiveElement = omFactory.createOMElement("receive", omNamespace);
        OMElement messageElement = omFactory.createOMElement("message", omNamespace);
        messageElement.setText("Test publish message");
        receiveElement.addChild(messageElement);
        return receiveElement;

    }
}
