package org.wso2.mb.integration.common.clients;

import org.apache.log4j.Logger;
import org.wso2.mb.integration.common.clients.configurations.AndesJMSPublisherClientConfiguration;
import org.wso2.mb.integration.common.clients.operations.utils.AndesClientConstants;
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

public class AndesJMSPublisherClient extends AndesJMSClient implements Runnable {
    private static Logger log = Logger.getLogger(AndesJMSPublisherClient.class);

    private AndesJMSPublisherClientConfiguration publisherConfig;

    private Connection connection;
    private Session session;
    private MessageProducer sender;
    private String messageContentFromFile = null;

    public AndesJMSPublisherClient(AndesJMSPublisherClientConfiguration config)
            throws NamingException, JMSException {
        super(config);

        this.publisherConfig = (AndesJMSPublisherClientConfiguration) super.config;

        ConnectionFactory connFactory = (ConnectionFactory) super.getInitialContext().lookup(AndesClientConstants.CF_NAME);
        connection = connFactory.createConnection();
        connection.start();
        this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Destination destination = (Destination) super.getInitialContext().lookup(this.publisherConfig.getDestinationName());
        this.sender = this.session.createProducer(destination);

    }

    @Override
    public void startClient() throws JMSException, NamingException, IOException {
        //reading message content from file
        if(null != this.publisherConfig.getReadMessagesFromFilePath()){
            this.getMessageContentFromFile();
        }

        for (int i = 0; i < this.publisherConfig.getPublisherCount(); i++) {
            Thread subscriberThread = new Thread(this);
            subscriberThread.start();
        }
    }

    @Override
    public synchronized void stopClient() throws JMSException {
        try {
            log.info("Closing publisher");
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
            log.info("Publisher closed");
        } catch (JMSException e) {
            log.error("Error while stopping the publisher.", e);
        }
    }

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

    @Override
    public void run() {
        try {
            Message message = null;
            long threadID = Thread.currentThread().getId();
            while (super.sentMessageCount.get() < this.publisherConfig.getNumberOfMessagesToSend()) {
                if (JMSMessageType.TEXT == this.publisherConfig.getJMSMessageType()) {
                    if (null != this.publisherConfig.getReadMessagesFromFilePath()) {
                        message = this.session.createTextMessage(this.messageContentFromFile);
                    } else {
                        message = this.session.createTextMessage("Sending Message:" + super.sentMessageCount.get() + "" +
                                                                 " ThreadID:" + threadID);
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
                    synchronized (super.sentMessageCount.getClass()) {
                        if (super.sentMessageCount.get() >= this.publisherConfig.getNumberOfMessagesToSend()) {
                            break;
                        }
                        this.sender.send(message, DeliveryMode.PERSISTENT, 0, this.publisherConfig.getJMSMessageExpiryTime());
                        super.sentMessageCount.incrementAndGet();
                    }
                    if (0 == super.sentMessageCount.get() % this.publisherConfig.getPrintsPerMessageCount()) {
                        if(null != this.publisherConfig.getReadMessagesFromFilePath()){
                            log.info("(FROM FILE)" + "[DESTINATION SEND] ThreadID:" +
                                     threadID + " DestinationName:" +
                                     this.publisherConfig.getDestinationName() + " TotalMessageCount:" +
                                     super.sentMessageCount.get() + " CountToSend:" +
                                     this.publisherConfig.getNumberOfMessagesToSend());
                        }else {
                            log.info("(INBUILT MESSAGE) " + "[DESTINATION SEND] ThreadID:" +
                                     threadID + " DestinationName:" +
                                     this.publisherConfig.getDestinationName() + " TotalMessageCount:" +
                                     super.sentMessageCount.get() + " CountToSend:" +
                                     this.publisherConfig.getNumberOfMessagesToSend());
                        }
                    }
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
}
