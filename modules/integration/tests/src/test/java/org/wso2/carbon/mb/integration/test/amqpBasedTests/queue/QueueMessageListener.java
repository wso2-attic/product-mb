package org.wso2.carbon.mb.integration.test.amqpBasedTests.queue;

import org.wso2.carbon.mb.integration.test.amqpBasedTests.utils.AndesClientUtils;
import javax.jms.*;
import java.util.concurrent.atomic.AtomicInteger;

public class QueueMessageListener implements MessageListener {
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

    public QueueMessageListener(QueueConnection queueConnection, QueueSession queueSession,
                                MessageConsumer queueReceiver, String queue, AtomicInteger messageCounter, int delayBetweenMessages, int printNumberOfMessagesPer,
                                boolean isToPrintEachMessage, String fileToWriteReceivedMessages, int stopAfter, int ackAfterEach, int commitAfterEach, int rollbackAfterEach) {
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
        this.localMessageCount =0;
    }

    public void onMessage(Message message) {
        messageCount.incrementAndGet();
        localMessageCount++;
        TextMessage receivedMessage = (TextMessage) message;
        try {

            String redelivery = "";
            if(message.getJMSRedelivered()) {
                redelivery = "REDELIVERED";
            }  else {
                redelivery = "ORIGINAL";
            }
            if(messageCount.get() % printNumberOfMessagesPer == 0) {
                System.out.println("[QUEUE RECEIVE] ThreadID:"+Thread.currentThread().getId()+" queue:"+queueName+" localMessageCount:"+localMessageCount+" totalMessageCount:" + messageCount.get() + " max count:" + stopMessageCount );
            }
            if(isToPrintEachMessage) {
                System.out.println("(count:"+messageCount.get()+"/threadID:"+Thread.currentThread().getId()+"/queue:"+queueName+") " + redelivery + " >> " + receivedMessage.getText());
                AndesClientUtils.writeToFile(receivedMessage.getText(), fileToWriteReceivedMessages);
            }

            if(messageCount.get() % ackAfterEach == 0) {
                if(queueSession.getAcknowledgeMode() == QueueSession.CLIENT_ACKNOWLEDGE) {
                    receivedMessage.acknowledge();
                    System.out.println("****Acked message***");
                }
            }

            //commit get priority
            if(messageCount.get() % commitPerMessageCount == 0) {
                queueSession.commit();
                System.out.println("Committed Queue Session");
            } else if(messageCount.get() % rollbackPerMessagecount == 0) {
                queueSession.rollback();
                System.out.println("Rollbacked Queue Session");
            }

            if(messageCount.get() >= stopMessageCount) {
                stopMessageListener();
                AndesClientUtils.sleepForInterval(200);
            }

            if(delayBetweenMessages != 0) {
                try {
                    Thread.sleep(delayBetweenMessages);
                } catch (InterruptedException e) {
                    //silently ignore
                }
            }
        } catch (NumberFormatException e) {
            System.out.println("Wrong inputs." + e);
        } catch (JMSException e) {
            System.out.println("JMS Exception" + e);
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
                    System.out.println("Closing subscriber");
                    queueReceiver.close();
                    queueSession.close();
                    queueConnection.stop();
                    queueConnection.close();
                    System.out.println("Done Closing subscriber");
                } catch (JMSException e) {
                    System.out.println("Error in closing the queue subscriber" + e);
                }
            }
        }).start();
    }
}
