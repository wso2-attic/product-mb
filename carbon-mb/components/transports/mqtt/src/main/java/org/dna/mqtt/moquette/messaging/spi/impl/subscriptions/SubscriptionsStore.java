package org.dna.mqtt.moquette.messaging.spi.impl.subscriptions;

import org.dna.mqtt.moquette.messaging.spi.IPersistentSubscriptionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Class to store subscriptions
 */
public class SubscriptionsStore {

    /**
     *
     * @param <T>
     */
    public static interface IVisitor<T> {
        void visit(org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode node);

        T getResult();
    }

    private static class DumpTreeVisitor implements IVisitor<String> {

        String s = "";

        public void visit(org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode node) {
            StringBuilder subscriptionsStr = new StringBuilder();
            for (org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription sub : node.subscriptions) {
                subscriptionsStr.append(sub.toString());
            }
            s += node.getToken() == null ? "" : node.getToken().toString();
            s += subscriptionsStr.toString() + "\n";
        }

        public String getResult() {
            return s;
        }
    }

    private static class SubscriptionTreeCollector implements IVisitor<List<org.dna.mqtt.moquette.messaging.spi.impl
            .subscriptions.Subscription>> {

        private List<org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription> allSubscriptions = new
                ArrayList<org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription>();

        public void visit(org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode node) {
            allSubscriptions.addAll(node.subscriptions());
        }

        public List<org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription> getResult() {
            return allSubscriptions;
        }
    }

    private org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode subscriptions = new org.dna.mqtt.moquette
            .messaging.spi.impl.subscriptions.TreeNode(null);
    private static final Logger LOG = LoggerFactory.getLogger(org.dna.mqtt.moquette.messaging.spi.impl.subscriptions
            .SubscriptionsStore.class);

    private IPersistentSubscriptionStore storageService;

    /**
     * Initialize basic store structures, like the FS storage to maintain
     * client's topics subscriptions
     */
    public void init(IPersistentSubscriptionStore storageService) {
        LOG.debug("init invoked");

        this.storageService = storageService;

        //reload any subscriptions persisted
        if (LOG.isDebugEnabled()) {
            LOG.debug("Reloading all stored subscriptions...subscription tree before {}", dumpTree());
        }

        for (org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription subscription : this.storageService
                .retrieveAllSubscriptions()) {
            LOG.debug("Re-subscribing {} to topic {}", subscription.getClientID(), subscription.getTopic());
            addDirect(subscription);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Finished loading. Subscription tree after {}", dumpTree());
        }
    }

    protected void addDirect(org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription newSubscription) {
        org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode current = findMatchingNode(newSubscription
                .topic);
        current.addSubscription(newSubscription);
    }

    private org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode findMatchingNode(String topic) {
        List<org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Token> tokens = new ArrayList<org.dna.mqtt
                .moquette.messaging.spi.impl.subscriptions.Token>();
        try {
            tokens = splitTopic(topic);
        } catch (ParseException ex) {
            //TODO handle the parse exception
            LOG.error(null, ex);
//            return;
        }

        org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode current = subscriptions;
        for (org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Token token : tokens) {
            org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode matchingChildren;

            //check if a children with the same token already exists
            if ((matchingChildren = current.childWithToken(token)) != null) {
                current = matchingChildren;
            } else {
                //create a new node for the newly inserted token
                matchingChildren = new org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode(current);
                matchingChildren.setToken(token);
                current.addChild(matchingChildren);
                current = matchingChildren;
            }
        }
        return current;
    }

    public void add(org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription newSubscription) {
        addDirect(newSubscription);

        //log the subscription
        String clientID = newSubscription.getClientID();
        storageService.addNewSubscription(newSubscription, clientID);
    }


    public void removeSubscription(String topic, String clientID) {
        org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode matchNode = findMatchingNode(topic);

        //search for the subscription to remove
        org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription toBeRemoved = null;
        for (org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription sub : matchNode.subscriptions()) {
            if (sub.topic.equals(topic) && sub.getClientID().equals(clientID)) {
                toBeRemoved = sub;
                break;
            }
        }

        if (toBeRemoved != null) {
            matchNode.subscriptions().remove(toBeRemoved);
        }
    }

    /**
     * Method written by WSO2 we need to get the subscriptions from the client id
     */
    public org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription getSubscriptions(String topic, String
            clientID) {
        org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription subscription = null;

        org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode matchNode = findMatchingNode(topic);

        for (org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription sub : matchNode.subscriptions()) {
            if (sub.topic.equals(topic) && sub.getClientID().equals(clientID)) {
                subscription = sub;
                break;
            }
        }

        return subscription;
    }

    /**
     * TODO implement testing
     */
    public void clearAllSubscriptions() {
        SubscriptionTreeCollector subsCollector = new SubscriptionTreeCollector();
        bfsVisit(subscriptions, subsCollector);

        List<org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription> allSubscriptions = subsCollector
                .getResult();
        for (org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription subscription : allSubscriptions) {
            removeSubscription(subscription.getTopic(), subscription.getClientID());
        }
    }

    /**
     * Visit the topics tree to remove matching subscriptions with clientID
     */
    public void removeForClient(String clientID) {
        subscriptions.removeClientSubscriptions(clientID);

        //remove from log all subscriptions
        storageService.removeAllSubscriptions(clientID);
    }

    public void deactivate(String clientID) {
        subscriptions.deactivate(clientID);
    }

    public void activate(String clientID) {
        LOG.debug("Activating subscriptions for clientID <{}>", clientID);
        subscriptions.activate(clientID);
    }

    /**
     * Given a topic string return the clients subscriptions that matches it.
     * Topic string can't contain character # and + because they are reserved to
     * listeners subscriptions, and not topic publishing.
     */
    public List<org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription> matches(String topic) {
        List<org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Token> tokens;
        try {
            tokens = splitTopic(topic);
        } catch (ParseException ex) {
            //TODO handle the parse exception
            LOG.error(null, ex);
            return Collections.EMPTY_LIST;
        }

        Queue<org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Token> tokenQueue = new LinkedBlockingDeque<org
                .dna.mqtt.moquette.messaging.spi.impl.subscriptions.Token>(tokens);
        List<org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription> matchingSubs = new ArrayList<org
                .dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription>();
        subscriptions.matches(tokenQueue, matchingSubs);
        return matchingSubs;
    }

    public boolean contains(Subscription sub) {
        return !matches(sub.topic).isEmpty();
    }

    public int size() {
        return subscriptions.size();
    }

    public String dumpTree() {
        DumpTreeVisitor visitor = new DumpTreeVisitor();
        bfsVisit(subscriptions, visitor);
        return visitor.getResult();
    }

    private void bfsVisit(org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode node, IVisitor visitor) {
        if (node == null) {
            return;
        }
        visitor.visit(node);
        for (org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode child : node.children) {
            bfsVisit(child, visitor);
        }
    }

    /**
     * Verify if the 2 topics matching respecting the rules of MQTT Appendix A
     */
    //TODO reimplement with iterators or with queues
    public static boolean matchTopics(String msgTopic, String subscriptionTopic) {
        try {
            List<Token> msgTokens = SubscriptionsStore.splitTopic(msgTopic);
            List<Token> subscriptionTokens = splitTopic(subscriptionTopic);
            int i = 0;
           Token subToken = null;
            for (; i < subscriptionTokens.size(); i++) {
                subToken = subscriptionTokens.get(i);
                if (subToken != Token.MULTI && subToken != Token.SINGLE) {
                    if (i >= msgTokens.size()) {
                        return false;
                    }
                    Token msgToken = msgTokens.get(i);
                    if (!msgToken.equals(subToken)) {
                        return false;
                    }
                } else {
                    if (subToken == Token.MULTI) {
                        return true;
                    }
                    // TODO : Address UCF_USELESS_CONTROL_FLOW
//                    if (subToken == Token.SINGLE) {
//                        //skip a step forward
//                    }
                }
            }

            return i == msgTokens.size();
        } catch (ParseException ex) {
            LOG.error(null, ex);
            throw new RuntimeException(ex);
        }
    }

    protected static List<Token> splitTopic(String topic)
            throws ParseException {
        List res = new ArrayList<Token>();
        String[] splitted = topic.split("/");

        if (splitted.length == 0) {
            res.add(Token.EMPTY);
        }

        for (int i = 0; i < splitted.length; i++) {
            String s = splitted[i];
            if (s.isEmpty()) {
//                if (i != 0) {
//                    throw new ParseException("Bad format of topic, expetec topic name between separators", i);
//                }
                res.add(Token.EMPTY);
            } else if (s.equals("#")) {
                //check that multi is the last symbol
                if (i != splitted.length - 1) {
                    throw new ParseException("Bad format of topic, the multi symbol (#) has to be the last one after "
                                             + "a separator", i);
                }
                res.add(Token.MULTI);
            } else if (s.contains("#")) {
                throw new ParseException("Bad format of topic, invalid subtopic name: " + s, i);
            } else if (s.equals("+")) {
                res.add(Token.SINGLE);
            } else if (s.contains("+")) {
                throw new ParseException("Bad format of topic, invalid subtopic name: " + s, i);
            } else {
                res.add(new Token(s));
            }
        }

        return res;
    }
}
