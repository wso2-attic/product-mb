package org.wso2.mb.integration.common.clients;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.mb.integration.common.clients.operations.utils.*;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.TextMessage;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by hemikakodikara on 1/29/15.
 */
public class TestListener implements MessageListener {
    private static Log log = LogFactory.getLog(TestListener.class);

    private QueueConnection queueConnection;
    private QueueSession queueSession;
    private MessageConsumer queueReceiver;
    private AtomicInteger messageCount;
    private int localMessageCount;
    private int stopMessageCount;
    private int delayBetweenMessages;
    private int ackAfterEach;
    private int rollbackPerMessagecount;
    private int commitPerMessageCount;
    private final String queueName;
    private int printNumberOfMessagesPer = 1;
    private boolean isToPrintEachMessage = false;
    private String fileToWriteReceivedMessages;

    //private static final Logger log = Logger.getLogger(queue.QueueMessageListener.class);

    public TestListener(QueueConnection queueConnection, QueueSession queueSession,
                                MessageConsumer queueReceiver, String queue, AtomicInteger messageCounter,
                                int delayBetweenMessages, int printNumberOfMessagesPer,
                                boolean isToPrintEachMessage, String fileToWriteReceivedMessages, int stopAfter,
                                int ackAfterEach, int commitAfterEach, int rollbackAfterEach) {
        this.queueConnection = queueConnection;
        this.queueSession = queueSession;
        this.queueReceiver = queueReceiver;
        this.queueName = queue;
        this.printNumberOfMessagesPer = printNumberOfMessagesPer;
        this.isToPrintEachMessage = isToPrintEachMessage;
        this.fileToWriteReceivedMessages = fileToWriteReceivedMessages;
        this.stopMessageCount = stopAfter;
        this.ackAfterEach = ackAfterEach;
        this.commitPerMessageCount = commitAfterEach;
        this.rollbackPerMessagecount = rollbackAfterEach;
        this.delayBetweenMessages = delayBetweenMessages;
        this.messageCount = messageCounter;
        this.localMessageCount = 0;
    }

    public void onMessage(Message message) {
        messageCount.incrementAndGet();
        localMessageCount++;
        Message receivedMessage = message;
        try {

            String redelivery = "";
            if (message.getJMSRedelivered()) {
                redelivery = "REDELIVERED";
            } else {
                redelivery = "ORIGINAL";
            }
            if (messageCount.get() % printNumberOfMessagesPer == 0) {
                log.info("[QUEUE RECEIVE] ThreadID:" + Thread.currentThread().getId() + " queue:" + queueName + " " +
                         "localMessageCount:" + localMessageCount + " totalMessageCount:" + messageCount.get() + " max" +
                         " count:" + stopMessageCount);
            }
            if (receivedMessage instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) receivedMessage;
                if (isToPrintEachMessage) {
                    log.info("(count:" + messageCount.get() + "/threadID:" + Thread.currentThread().getId() + "/queue:" +
                             queueName + ") " + redelivery + " >> " + textMessage.getText());
                    AndesClientUtilsTemp.writeToFile(textMessage.getText(), fileToWriteReceivedMessages);
                }
            }

            if (messageCount.get() % ackAfterEach == 0) {
                if (queueSession.getAcknowledgeMode() == QueueSession.CLIENT_ACKNOWLEDGE) {
                    receivedMessage.acknowledge();
                    log.info("Acked message : " + receivedMessage.getJMSMessageID());
                }
            }

            //commit get priority
            if (messageCount.get() % commitPerMessageCount == 0) {
                queueSession.commit();
                log.info("Committed Queue Session");
            } else if (messageCount.get() % rollbackPerMessagecount == 0) {
                queueSession.rollback();
                log.info("Rollbacked Queue Session");
            }

            if (messageCount.get() >= stopMessageCount) {
                stopMessageListener();
                AndesClientUtilsTemp.sleepForInterval(200);
            }

            if (delayBetweenMessages != 0) {
                try {
                    Thread.sleep(delayBetweenMessages);
                } catch (InterruptedException e) {
                    //silently ignore
                }
            }
        } catch (NumberFormatException e) {
            log.error("Wrong inputs.", e);
        } catch (JMSException e) {
            log.error("JMS Exception", e);
        }
    }

    public AtomicInteger getMessageCount() {
        return messageCount;
    }

    public void setToRollbackSessionAtMessageCount(int messageCount) {
        rollbackPerMessagecount = messageCount;
    }

    public void setToCommitAtMessageCount(int messageCount) {
        commitPerMessageCount = messageCount;
    }

    public void setToStopAtMessageCount(int stopMessageCount) {
        this.stopMessageCount = stopMessageCount;
    }

    public void resetMessageCount() {
        this.messageCount.set(0);
    }

    public void stopMessageListener() {

        new Thread(new Runnable() {
            public void run() {
                try {
                    log.info("Closing subscriber");
                    queueReceiver.close();
                    queueSession.close();
                    queueConnection.stop();
                    queueConnection.close();
                    log.info("Done Closing subscriber");
                } catch (JMSException e) {
                    log.error("Error in closing the queue subscriber", e);
                }
            }
        }).start();
    }
}
