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

package org.wso2.carbon.andes.transports.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.andes.transports.config.NettyServerContext;


/**
 * Each server should implement this, this class holds base methods required for any netty based server implementation
 * i.e MQTT, Websockets, AMQP
 */
public abstract class AbstractServer implements Server {

    /**
     * Defines the pool of threads which will accept TCP connections
     */
    private EventLoopGroup bossGroup;
    /**
     * Defines the pool of threads which will handle the message after the connection is accepted
     */
    private EventLoopGroup workerGroup;

    /**
     * Holds the status of I/O operations which happens in the server
     */
    private ChannelFuture future;

    private static final Log log = LogFactory.getLog(AbstractServer.class);

    /**
     * Creates pipeline specific to the server
     *
     * @param pipeline the list of handlers the server should take an incoming message
     */
    public abstract void createPipeline(ChannelPipeline pipeline);

    /**
     * Starts the MQTT server
     *
     * @param ctx the property values required for the server startup i.e hostname/port
     * @see NettyServerContext
     */
    public void start(NettyServerContext ctx) throws BrokerException {
        if (log.isDebugEnabled()) {
            log.debug("Initiating netty transport service with " + ctx.getHost() + ":" + ctx.getPort());
        }
        init(ctx);
    }

    /**
     * Stops the running server gracefully
     */
    public void stop() {

        if (bossGroup != null && workerGroup != null && future != null) {

            if (log.isDebugEnabled()) {
                log.debug("Shutting down the boss group");
            }

            final Future<?> bossGroupShutdownStatus = bossGroup.shutdownGracefully();
            bossGroupShutdownStatus.addListener((GenericFutureListener<Future<Object>>) bossGroupFuture -> {
                if (log.isDebugEnabled()) {
                    log.debug("Boss group shutdown is complete,initiating the worker group shutdown.");
                }
               final Future<?> workerGroupShutdownStatus = workerGroup.shutdownGracefully();
                workerGroupShutdownStatus.addListener((GenericFutureListener<Future<Object>>) workerGroupFuture -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Worker group shutdown is complete");
                    }

                });
            });

            log.info("Netty server has successfully shutdown");
        } else {
            log.error("Server has not being initialized properly, cannot shutdown the server");
        }


    }

    /**
     * Initializes MQTT server
     *
     * @param ctx server details
     */
    private void init(NettyServerContext ctx) throws BrokerException {
        try {
            //The thread group which accepts connections
            bossGroup = new NioEventLoopGroup(ctx.getAcceptanceThreads());
            //The thread group which handles when a given message is accepted
            workerGroup = new NioEventLoopGroup(ctx.getWorkerThreads());

            //Now we bootstrap the server
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class) //Instantiates a new channel per each socket connection
                    .childHandler(new ChannelPipelineSequence())
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            //We bind the server the particular address
            future = bootstrap.bind(ctx.getHost(), ctx.getPort()).sync();
            log.info(ctx.getProtocol() + " server started on " + ctx.getHost() + " port " + ctx.getPort());
        } catch (InterruptedException e) {
            //We clear the interrupted state
            Thread.currentThread().interrupt();

            if (log.isDebugEnabled()) {
                log.debug("Thread interrupted when starting the server ", e);
            }

        } catch (Exception ex) {
            String error = "Exception occurred when creating the pipeline ";
            log.error(error, ex);
            throw new BrokerException(error, ex);
        }
    }

    /**
     * Defines the set of handlers which should be executed when the respective event is triggered from the server
     * We use an inner class here, since its required to access the current state of the parent class object
     */
    private class ChannelPipelineSequence extends ChannelInitializer<SocketChannel> {

        @Override
        protected void initChannel(SocketChannel socketChannel) throws Exception {
            //This will initialise the pipeline based on the server which extends this
            createPipeline(socketChannel.pipeline());
        }
    }

}
