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

package org.wso2.carbon.andes.transports.mqtt.handlers;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Triggers when a connection is idled for a specified duration
 * This will be triggered through idleStateMQTTHandler which should be defined prior to this
 */
@SuppressWarnings("unused")
public class IdleStateEventMqttHandler extends ChannelDuplexHandler {

    private static final Log log = LogFactory.getLog(IdleStateEventMqttHandler.class);

    /**
     * This will be triggered through the IdleStateEventHandler
     *
     * @param ctx holds the context of the channel responsible for the triggered event
     * @param evt type of the event ReaderIdle, WriterIdle or ALL_Idle
     * @throws Exception
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        //we're interested only in the idle state
        if (evt instanceof IdleStateEvent) {
            IdleState channelState = ((IdleStateEvent) evt).state();
            switch (channelState) {
                case ALL_IDLE:
                    //If there's no activity for reading or writing we close the channel
                    ctx.close();
                    log.warn("Closing the channel " + ctx.name() + " since there was no activity");
                    break;
                case WRITER_IDLE:
                    if (log.isDebugEnabled()) {
                        log.debug("The channel " + ctx.name() + " has no record of data being written");
                    }
                    break;
                case READER_IDLE:
                    if (log.isDebugEnabled()) {
                        log.debug("The channel " + ctx.name() + " has no record of data being read");
                    }
                    break;
                default:
                    log.warn("An unhandled state " + channelState + " recorded from channel " + ctx.name());
                    break;
            }
        }
    }
}
