package org.dna.mqtt.moquette.messaging.spi.impl.subscriptions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

class TreeNode {

    private static class ClientIDComparator implements Serializable, Comparator<Subscription> {

        private static final long serialVersionUID = -6096113045649398353L;

        public int compare(org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription o1, org.dna.mqtt
                .moquette.messaging.spi.impl.subscriptions.Subscription o2) {
            return o1.getClientID().compareTo(o2.getClientID());
        }

    }

//    org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode parent;
    Token token;
    List<org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode> children = new ArrayList<org.dna.mqtt
            .moquette.messaging.spi.impl.subscriptions.TreeNode>();
    List<org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription> subscriptions = new ArrayList<org.dna
            .mqtt.moquette.messaging.spi.impl.subscriptions.Subscription>();

    TreeNode(org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode parent) {
//        this.parent = parent;
    }

    Token getToken() {
        return token;
    }

    void setToken(Token topic) {
        this.token = topic;
    }

    void addSubscription(org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription s) {
        //avoid double registering for same clientID, topic and QoS
        if (subscriptions.contains(s)) {
            return;
        }
        //remove existing subscription for same client and topic but different QoS
        int existingSubIdx = Collections.binarySearch(subscriptions, s, new ClientIDComparator());
        if (existingSubIdx >= 0) {
            subscriptions.remove(existingSubIdx);
        }

        subscriptions.add(s);
    }

    void addChild(org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode child) {
        children.add(child);
    }

    boolean isLeaf() {
        return children.isEmpty();
    }

    /**
     * Search for children that has the specified token, if not found return
     * null;
     */
    org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode childWithToken(Token token) {
        for (org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode child : children) {
            if (child.getToken().equals(token)) {
                return child;
            }
        }

        return null;
    }

    List<org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription> subscriptions() {
        return subscriptions;
    }

    void matches(Queue<Token> tokens, List<org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription>
            matchingSubs) {
        Token t = tokens.poll();

        //check if t is null <=> tokens finished
        if (t == null) {
            matchingSubs.addAll(subscriptions);
            //check if it has got a MULTI child and add its subscriptions
            for (org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode n : children) {
                if (n.getToken() == Token.MULTI || n.getToken() == Token.SINGLE) {
                    matchingSubs.addAll(n.subscriptions());
                }
            }

            return;
        }

        //we are on MULTI, than add subscriptions and return
        if (token == Token.MULTI) {
            matchingSubs.addAll(subscriptions);
            return;
        }

        for (org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode n : children) {
            if (n.getToken().match(t)) {
                //Create a copy of token, else if navigate 2 sibling it
                //consumes 2 elements on the queue instead of one
                n.matches(new LinkedBlockingQueue<Token>(tokens), matchingSubs);
                //TODO don't create a copy n.matches(tokens, matchingSubs);
            }
        }
    }

    /**
     * Return the number of registered subscriptions
     */
    int size() {
        int res = subscriptions.size();
        for (org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode child : children) {
            res += child.size();
        }
        return res;
    }

    void removeClientSubscriptions(String clientID) {
        //collect what to delete and then delete to avoid ConcurrentModification
        List<org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription> subsToRemove = new ArrayList<org
                .dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription>();
        for (org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription s : subscriptions) {
            if (s.clientId.equals(clientID)) {
                subsToRemove.add(s);
            }
        }

        for (org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription s : subsToRemove) {
            subscriptions.remove(s);
        }

        //go deep
        for (org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode child : children) {
            child.removeClientSubscriptions(clientID);
        }
    }

    /**
     * Deactivate all topic subscriptions for the given clientID.
     */
    void deactivate(String clientID) {
        for (org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription s : subscriptions) {
            if (s.clientId.equals(clientID)) {
                s.setActive(false);
            }
        }

        //go deep
        for (org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode child : children) {
            child.deactivate(clientID);
        }
    }

    /**
     * Activate all topic subscriptions for the given clientID.
     */
    public void activate(String clientID) {
        for (Subscription s : subscriptions) {
            if (s.clientId.equals(clientID)) {
                s.setActive(true);
            }
        }

        //go deep
        for (org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.TreeNode child : children) {
            child.activate(clientID);
        }

    }
}
