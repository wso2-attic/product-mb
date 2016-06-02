/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.carbon.transport.tests.mqtt.broker.v311.client;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.wso2.carbon.andes.transports.mqtt.broker.MqttChannel;
import org.wso2.carbon.andes.transports.mqtt.netty.protocol.messages.AbstractMessage;

/**
 * Would receive the responseMessage from the given channel, This mocks MQTT client interface
 */
public class ClientMessageReceiver {
    /**
     * Would hold the responseMessage the client would receive from the server
     */
    private AbstractMessage responseMessage;


    private MqttChannel mqttChannel;


    public ClientMessageReceiver() {
        //For each client created there should be a mock client channel
        ChannelHandlerContext mockClientConnection = createMockClientConnection();
        this.mqttChannel = new MqttChannel(mockClientConnection);
    }

    /**
     * Would return a mock client connection
     *
     * @return ChannelHandlerContext
     */
    private ChannelHandlerContext createMockClientConnection() {

        ChannelHandlerContext channel = Mockito.mock(ChannelHandlerContext.class);

        //Specifies to write the responseMessage to the given channel
        Mockito.when(channel.writeAndFlush(Matchers.anyObject())).thenAnswer(new Answer<ChannelFuture>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public ChannelFuture answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] inputValues = invocationOnMock.getArguments();
                receive((AbstractMessage) inputValues[0]);
                return null;
            }
        });

        return channel;

    }

    /**
     * Message which will be dispatched form the server to the client
     *
     * @param incomingMessage MQTT command responseMessage
     */
    private void receive(AbstractMessage incomingMessage) {
        this.responseMessage = incomingMessage;
    }

    public AbstractMessage getResponseMessage() {
        return responseMessage;
    }

    public MqttChannel getMqttChannel() {
        return mqttChannel;
    }
}
