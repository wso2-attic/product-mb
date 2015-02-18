/*
*  Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.mb.integration.common.clients;

import org.apache.log4j.Logger;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientUtils;
import org.wso2.mb.integration.common.clients.operations.utils.JMSMessageHeader;
import org.wso2.mb.integration.common.clients.operations.utils.JMSMessageType;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.NamingException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The JMS message publisher used for creating a publisher and for publishing JMS messages.
 */
public class AndesJMSPublisher extends AndesJMSClient implements Runnable {
    /**
     * The logger used in logging information, warnings, errors and etc.
     */
    private static Logger log = Logger.getLogger(AndesJMSPublisher.class);

    /**
     * The configuration for the publisher
     */
    private AndesJMSPublisherClientConfiguration publisherConfig;

    /**
     * The amount of messages sent by the publisher
     */
    private AtomicLong sentMessageCount;

    /**
     * The timestamp at which the first message was published
     */
    private AtomicLong firstMessagePublishTimestamp;

    /**
     * The timestamp at which the last message was published
     */
    private AtomicLong lastMessagePublishTimestamp;

    /**
     * The connection which is used to create the JMS session
     */
    private Connection connection;

    /**
     * The session which is used to create the JMS message producer
     */
    private Session session;

    /**
     * The message producer which produces/sends messages
     */
    private MessageProducer sender;

    /**
     * Message content which is needed to be published. The value will depend on the configuration.
     */
    private String messageContentFromFile = null;

    /**
     * Creates a new JMS publisher with a given configuration.
     *
     * @param config The configuration
     * @throws NamingException
     * @throws JMSException
     */
    public AndesJMSPublisher(AndesJMSPublisherClientConfiguration config)
            throws NamingException, JMSException {
        super(config);

        // Initializing statistics calculation variable
        firstMessagePublishTimestamp = new AtomicLong();
        lastMessagePublishTimestamp = new AtomicLong();

        // Initializing message count variable.
        sentMessageCount = new AtomicLong();

        // Sets the configuration
        this.publisherConfig = config;

        // Creates a JMS connection, sessions and sender
        ConnectionFactory connFactory = (ConnectionFactory) super.getInitialContext().lookup(AndesClientConstants.CF_NAME);
        connection = connFactory.createConnection();
        connection.start();
        this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Destination destination = (Destination) super.getInitialContext().lookup(this.publisherConfig.getDestinationName());
        this.sender = this.session.createProducer(destination);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void startClient() throws JMSException, NamingException, IOException {
        //reading message content from file
        if (null != this.publisherConfig.getReadMessagesFromFilePath()) {
            this.getMessageContentFromFile();
        }

        Thread subscriberThread = new Thread(this);
        subscriberThread.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void stopClient() throws JMSException {
        try {
            long threadID = Thread.currentThread().getId();
            log.info("Closing publisher | ThreadID : " + threadID);
            if (this.sender != null) {
                this.sender.close();
                this.sender = null;
            }
            if (this.session != null) {
                this.session.close();
                this.session = null;
            }
            if (this.connection != null) {
                this.connection.close();
                this.connection = null;
            }
            log.info("Publisher closed | ThreadID : " + threadID);
        } catch (JMSException e) {
            log.error("Error while stopping the publisher.", e);
        }
    }

    /**
     * Reads message content from a file which is used as the message content to when publishing
     * messages.
     *
     * @throws IOException
     */
    public void getMessageContentFromFile() throws IOException {
        if (null != this.publisherConfig.getReadMessagesFromFilePath()) {
            BufferedReader br = new BufferedReader(new FileReader(this.publisherConfig.getReadMessagesFromFilePath()));
            try {
                StringBuilder sb = new StringBuilder();
                String line = br.readLine();

                while (line != null) {
                    sb.append(line);
                    sb.append('\n');
                    line = br.readLine();
                }

                // Remove the last appended next line since there is no next line.
                sb.replace(sb.length() - 1, sb.length() + 1, "");
                messageContentFromFile = sb.toString();
            } finally {
                br.close();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        try {
            Message message = null;
            long threadID = Thread.currentThread().getId();
            while (this.sentMessageCount.get() < this.publisherConfig.getNumberOfMessagesToSend()) {
                // Creating a JMS message
                if (JMSMessageType.TEXT == this.publisherConfig.getJMSMessageType()) {
                    if (null != this.publisherConfig.getReadMessagesFromFilePath()) {
                        message = this.session.createTextMessage(this.messageContentFromFile);
                    } else {
                        message = this.session.createTextMessage(MessageFormat.format(AndesClientConstants.PUBLISH_MESSAGE_FORMAT, this.sentMessageCount.get(), threadID));

                    }
                } else if (JMSMessageType.BYTE == this.publisherConfig.getJMSMessageType()) {
                    message = this.session.createBytesMessage();
                } else if (JMSMessageType.MAP == this.publisherConfig.getJMSMessageType()) {
                    message = this.session.createMapMessage();
                } else if (JMSMessageType.OBJECT == this.publisherConfig.getJMSMessageType()) {
                    message = this.session.createObjectMessage();
                } else if (JMSMessageType.STREAM == this.publisherConfig.getJMSMessageType()) {
                    message = this.session.createStreamMessage();
                }

                if (null != message) {

                    // Creates JMS message headers if needed. Used in JMS selectors
                    if (null != this.publisherConfig.getMessageHeader()) {
                        JMSMessageHeader messageHeader = this.publisherConfig.getMessageHeader();
                        if (null != messageHeader.getJmsCorrelationID()) {
                            message.setJMSCorrelationID(messageHeader.getJmsCorrelationID());
                        }
                        if (0L != messageHeader.getJmsTimestamp()) {
                            message.setJMSTimestamp(messageHeader.getJmsTimestamp());
                        }
                        if (null != messageHeader.getJmsType()) {
                            message.setJMSType(messageHeader.getJmsType());
                        }

                        if (0 < messageHeader.getStringProperties().size()) {
                            for (String key : messageHeader.getStringProperties().keySet()) {
                                message.setStringProperty(key, messageHeader.getStringProperties().get(key));
                            }
                        }
                        if (0 < messageHeader.getIntegerProperties().size()) {
                            for (String key : messageHeader.getIntegerProperties().keySet()) {
                                message.setIntProperty(key, messageHeader.getIntegerProperties().get(key));
                            }
                        }
                    }

                    // Sending messages
                    synchronized (this.sentMessageCount.getClass()) {
                        if (this.sentMessageCount.get() >= this.publisherConfig.getNumberOfMessagesToSend()) {
                            break;
                        }
                        this.sender.send(message, DeliveryMode.PERSISTENT, 0, this.publisherConfig.getJMSMessageExpiryTime());

                        this.sentMessageCount.incrementAndGet();
                    }

                    // TPS calculation
                    long currentTimeStamp = System.currentTimeMillis();
                    if (this.firstMessagePublishTimestamp.get() == 0) {
                        this.firstMessagePublishTimestamp.set(currentTimeStamp);
                    }

                    this.lastMessagePublishTimestamp.set(currentTimeStamp);
                    if (0 == this.sentMessageCount.get() % this.publisherConfig.getPrintsPerMessageCount()) {
                        // Logging the sent message details.
                        if (null != this.publisherConfig.getReadMessagesFromFilePath()) {
                            log.info("[SEND]" + " (FROM FILE) ThreadID:" +
                                     threadID + " Destination:" + this.publisherConfig.getExchangeType().getType() +
                                     "." + this.publisherConfig.getDestinationName() + " SentMessageCount:" +
                                     this.sentMessageCount.get() + " CountToSend:" +
                                     this.publisherConfig.getNumberOfMessagesToSend());
                        } else {
                            log.info("[SEND]" + " (INBUILT MESSAGE) ThreadID:" +
                                     threadID + " Destination:" + this.publisherConfig.getExchangeType().getType() +
                                     "." + this.publisherConfig.getDestinationName() + " SentMessageCount:" +
                                     this.sentMessageCount.get() + " CountToSend:" +
                                     this.publisherConfig.getNumberOfMessagesToSend());
                        }
                    }
                    // Writing statistics
                    if (null != this.publisherConfig.getFilePathToWriteStatistics()) {
                        String statisticsString = ",,,," + Long.toString(currentTimeStamp) + "," + Double.toString(this.getPublisherTPS());
                        AndesClientUtils.writeStatisticsToFile(statisticsString, this.publisherConfig.getFilePathToWriteStatistics());
                    }

                    // Delaying the publishing of messages
                    if (0 < this.publisherConfig.getRunningDelay()) {
                        try {
                            Thread.sleep(this.publisherConfig.getRunningDelay());
                        } catch (InterruptedException e) {
                            //silently ignore
                        }
                    }
                }
            }

            this.stopClient();
        } catch (JMSException e) {
            log.error("Error while publishing messages", e);
            throw new RuntimeException("JMSException : Error while publishing messages", e);
        }
    }

    /**
     * Gets the published message count.
     * @return The published message count.
     */
    public long getSentMessageCount() {
        return this.sentMessageCount.get();
    }

    /**
     * Gets the transactions per seconds for publisher.
     * @return The transactions per second.
     */
    public double getPublisherTPS() {
        if (0 == this.lastMessagePublishTimestamp.get() - this.firstMessagePublishTimestamp.get()) {
            return this.sentMessageCount.doubleValue() / (1D / 1000);
        } else {
            return this.sentMessageCount.doubleValue() / ((this.lastMessagePublishTimestamp.doubleValue() - this.firstMessagePublishTimestamp.doubleValue()) / 1000);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesJMSPublisherClientConfiguration getConfig() {
        return this.publisherConfig;
    }
}
