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

package org.sample.transport;

import org.apache.axis2.AxisFault;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.ConfigurationContextFactory;
import org.apache.axis2.engine.AxisServer;
import org.sample.transport.stub.MessageReceiveServiceStub;


public class JMSTransportClient {

    private AxisServer axisServer;

    public void start(){

        try {

            ConfigurationContext configurationContext =
                    ConfigurationContextFactory.createConfigurationContextFromFileSystem(null,"conf/axis2.xml");
            this.axisServer = new AxisServer();
            this.axisServer.setConfigurationContext(configurationContext);
            this.axisServer.deployService(MessageReceiveService.class.getName());

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }

        } catch (AxisFault axisFault) {
            axisFault.printStackTrace();
        } catch (Exception e) {
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

    public void sendMessage(){

        try {


            ConfigurationContext configurationContext =
                    ConfigurationContextFactory.createConfigurationContextFromFileSystem(null, "conf/axis2_client.xml");

            MessageReceiveServiceStub stub = new MessageReceiveServiceStub(configurationContext,"http://localhost:8080/axis2/services/MessageReceiveService.MessageReceiveServiceHttpSoap11Endpoint/");

            //first send the inonly message
            stub.receive("Test message to receive ");

            // inout message
            String response = stub.echo("Test message to echo");
            System.out.println("Response ==> " + response);


            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        } catch (AxisFault axisFault) {
            axisFault.printStackTrace();
        } catch (java.rmi.RemoteException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        
        JMSTransportClient jmsTransportClient = new JMSTransportClient();
        jmsTransportClient.start();
        jmsTransportClient.sendMessage();
        jmsTransportClient.stop();
    }


}
