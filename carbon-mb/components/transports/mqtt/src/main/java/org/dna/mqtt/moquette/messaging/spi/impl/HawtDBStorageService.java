package org.dna.mqtt.moquette.messaging.spi.impl;

import org.dna.mqtt.moquette.MQTTException;
import org.dna.mqtt.moquette.messaging.spi.IMatchingCondition;
import org.dna.mqtt.moquette.messaging.spi.IStorageService;
import org.dna.mqtt.moquette.messaging.spi.impl.events.PublishEvent;
import org.dna.mqtt.moquette.messaging.spi.impl.storage.StoredPublishEvent;
import org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;
import org.dna.mqtt.moquette.server.Server;
import org.fusesource.hawtbuf.codec.StringCodec;
import org.fusesource.hawtdb.api.BTreeIndexFactory;
import org.fusesource.hawtdb.api.MultiIndexFactory;
import org.fusesource.hawtdb.api.PageFile;
import org.fusesource.hawtdb.api.PageFileFactory;
import org.fusesource.hawtdb.api.SortedIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Implementation of IStorageService backed by HawtDB
 */
public class HawtDBStorageService implements IStorageService {

    /**
     * TODO: Comment to be added
     */
    public static class StoredMessage implements Serializable {

        private static final long serialVersionUID = 7556000323488731448L;
        AbstractMessage.QOSType qos;
        byte[] payload;
        String topic;

        StoredMessage(byte[] message, AbstractMessage.QOSType qos, String topic) {
            this.qos = qos;
            payload = message;
            this.topic = topic;
        }

        AbstractMessage.QOSType getQos() {
            return qos;
        }

        ByteBuffer getPayload() {
            return (ByteBuffer) ByteBuffer.allocate(payload.length).put(payload).flip();
        }

        String getTopic() {
            return topic;
        }
    }


    private static final Logger LOG = LoggerFactory.getLogger(org.dna.mqtt.moquette.messaging.spi.impl
            .HawtDBStorageService.class);

    private MultiIndexFactory multiIndexFactory;
    private PageFileFactory pageFactory;

    //maps clientID to the list of pending messages stored
    //TODO move in a multimap because only Qos1 and QoS2 are stored here and they have messageID(key of secondary map)
    private SortedIndex<String, List<StoredPublishEvent>> persistentMessageStore;
    private SortedIndex<String, StoredMessage> retainedStore;
    //bind clientID+MsgID -> evt message published
    private SortedIndex<String, StoredPublishEvent> inflightStore;
    //bind clientID+MsgID -> evt message published
    private SortedIndex<String, StoredPublishEvent> qos2Store;

    //persistent Map of clientID, set of Subscriptions
    private SortedIndex<String, Set<Subscription>> persistentSubscriptions;

    public HawtDBStorageService() {
        String storeFile = Server.STORAGE_FILE_PATH;

        pageFactory = new PageFileFactory();
        File tmpFile;
        try {
            tmpFile = new File(storeFile);
            boolean newFile = tmpFile.createNewFile();
            if (!newFile) {
                throw new IOException("Unable to create new file for subscription storage");
            }
        } catch (IOException ex) {
            LOG.error(null, ex);
            throw new MQTTException("Can't create temp file for subscriptions storage [" + storeFile + "]", ex);
        }
        pageFactory.setFile(tmpFile);
        pageFactory.open();
        PageFile pageFile = pageFactory.getPageFile();
        multiIndexFactory = new MultiIndexFactory(pageFile);
    }


    public void initStore() {
        initRetainedStore();
        //init the message store for QoS 1/2 messages in clean sessions
        initPersistentMessageStore();
        initInflightMessageStore();
        initPersistentSubscriptions();
        initPersistentQoS2MessageStore();
    }

    private void initRetainedStore() {
        BTreeIndexFactory<String, StoredMessage> indexFactory = new BTreeIndexFactory<String, StoredMessage>();
        indexFactory.setKeyCodec(StringCodec.INSTANCE);

        retainedStore = (SortedIndex<String, StoredMessage>) multiIndexFactory.openOrCreate("retained", indexFactory);
    }


    private void initPersistentMessageStore() {
        BTreeIndexFactory<String, List<StoredPublishEvent>> indexFactory = new BTreeIndexFactory<String,
                List<StoredPublishEvent>>();
        indexFactory.setKeyCodec(StringCodec.INSTANCE);

        persistentMessageStore = (SortedIndex<String, List<StoredPublishEvent>>) multiIndexFactory.openOrCreate
                ("persistedMessages", indexFactory);
    }

    private void initPersistentSubscriptions() {
        BTreeIndexFactory<String, Set<Subscription>> indexFactory = new BTreeIndexFactory<String, Set<Subscription>>();
        indexFactory.setKeyCodec(StringCodec.INSTANCE);

        persistentSubscriptions = (SortedIndex<String, Set<Subscription>>) multiIndexFactory.openOrCreate
                ("subscriptions", indexFactory);
    }

    /**
     * Initialize the message store used to handle the temporary storage of QoS 1,2
     * messages in flight.
     */
    private void initInflightMessageStore() {
        BTreeIndexFactory<String, StoredPublishEvent> indexFactory = new BTreeIndexFactory<String,
                StoredPublishEvent>();
        indexFactory.setKeyCodec(StringCodec.INSTANCE);

        inflightStore = (SortedIndex<String, StoredPublishEvent>) multiIndexFactory.openOrCreate("inflight",
                indexFactory);
    }

    private void initPersistentQoS2MessageStore() {
        BTreeIndexFactory<String, StoredPublishEvent> indexFactory = new BTreeIndexFactory<String,
                StoredPublishEvent>();
        indexFactory.setKeyCodec(StringCodec.INSTANCE);

        qos2Store = (SortedIndex<String, StoredPublishEvent>) multiIndexFactory.openOrCreate("qos2Store", indexFactory);
    }

    public void storeRetained(String topic, ByteBuffer message, AbstractMessage.QOSType qos) {
        //TODO removed the retain entry since we will be maintaing a cluster specifc store in andes
 /*       if (!message.hasRemaining()) {
            //clean the message from topic
            retainedStore.remove(topic);
        } else {
            //store the message to the topic
            byte[] raw = new byte[message.remaining()];
            message.get(raw);
            retainedStore.put(topic, new StoredMessage(raw, qos, topic));
        }*/
    }

    public Collection<StoredMessage> searchMatching(IMatchingCondition condition) {
        LOG.debug("searchMatching scanning all retained messages, presents are {}", retainedStore.size());

        List<StoredMessage> results = new ArrayList<StoredMessage>();

        for (Map.Entry<String, StoredMessage> entry : retainedStore) {
            StoredMessage storedMsg = entry.getValue();
            if (condition.match(entry.getKey())) {
                results.add(storedMsg);
            }
        }

        return results;
    }

    public void storePublishForFuture(PublishEvent evt) {
        List<StoredPublishEvent> storedEvents;
        String clientID = evt.getClientID();
        if (!persistentMessageStore.containsKey(clientID)) {
            storedEvents = new ArrayList<StoredPublishEvent>();
        } else {
            storedEvents = persistentMessageStore.get(clientID);
        }
        storedEvents.add(convertToStored(evt));
        persistentMessageStore.put(clientID, storedEvents);
        //NB rewind the evt message content
        LOG.debug("Stored published message for client <{}> on topic <{}>", clientID, evt.getTopic());
    }

    public List<PublishEvent> retrivePersistedPublishes(String clientID) {
        List<StoredPublishEvent> storedEvts = persistentMessageStore.get(clientID);
        if (storedEvts == null) {
            return null;
        }
        List<PublishEvent> liveEvts = new ArrayList<PublishEvent>();
        for (StoredPublishEvent storedEvt : storedEvts) {
            liveEvts.add(convertFromStored(storedEvt));
        }
        return liveEvts;
    }

    public void cleanPersistedPublishMessage(String clientID, int messageID) {
        List<StoredPublishEvent> events = persistentMessageStore.get(clientID);
        if (events == null) {
            return;
        }
        StoredPublishEvent toRemoveEvt = null;
        for (StoredPublishEvent evt : events) {
            if (evt.getMessageID() == messageID) {
                toRemoveEvt = evt;
            }
        }
        events.remove(toRemoveEvt);
        persistentMessageStore.put(clientID, events);
    }

    public void cleanPersistedPublishes(String clientID) {
        persistentMessageStore.remove(clientID);
    }

    public void cleanInFlight(String msgID) {
        inflightStore.remove(msgID);
    }

    public void addInFlight(PublishEvent evt, String publishKey) {
        StoredPublishEvent storedEvt = convertToStored(evt);
        inflightStore.put(publishKey, storedEvt);
    }

    public void addNewSubscription(Subscription newSubscription, String clientID) {
        LOG.debug("addNewSubscription invoked with subscription {} for client {}", newSubscription, clientID);
        if (!persistentSubscriptions.containsKey(clientID)) {
            LOG.debug("clientID {} is a newcome, creating it's subscriptions set", clientID);
            persistentSubscriptions.put(clientID, new HashSet<Subscription>());
        }

        Set<Subscription> subs = persistentSubscriptions.get(clientID);
        if (!subs.contains(newSubscription)) {
            LOG.debug("updating clientID {} subscriptions set with new subscription", clientID);
            //TODO check the subs doesn't contain another subscription to the same topic with different
            Subscription existingSubscription = null;
            for (Subscription scanSub : subs) {
                if (newSubscription.getTopic().equals(scanSub.getTopic())) {
                    existingSubscription = scanSub;
                    break;
                }
            }
            if (existingSubscription != null) {
                subs.remove(existingSubscription);
            }
            subs.add(newSubscription);
            persistentSubscriptions.put(clientID, subs);
            LOG.debug("clientID {} subscriptions set now is {}", clientID, subs);
        }
    }

    public void removeAllSubscriptions(String clientID) {
        persistentSubscriptions.remove(clientID);
    }

    public List<Subscription> retrieveAllSubscriptions() {
        List<Subscription> allSubscriptions = new ArrayList<Subscription>();
        for (Map.Entry<String, Set<Subscription>> entry : persistentSubscriptions) {
            allSubscriptions.addAll(entry.getValue());
        }
        LOG.debug("retrieveAllSubscriptions returning subs {}", allSubscriptions);
        return allSubscriptions;
    }

    public void close() {
        LOG.debug("closing disk storage");
        try {
            pageFactory.close();
        } catch (IOException ex) {
            LOG.error(null, ex);
        }
    }

    /*-------- QoS 2  storage management --------------*/
    public void persistQoS2Message(String publishKey, PublishEvent evt) {
        LOG.debug("persistQoS2Message store pubKey {}, evt {}", publishKey, evt);
        qos2Store.put(publishKey, convertToStored(evt));
    }

    public void removeQoS2Message(String publishKey) {
        qos2Store.remove(publishKey);
    }

    public PublishEvent retrieveQoS2Message(String publishKey) {
        StoredPublishEvent storedEvt = qos2Store.get(publishKey);
        return convertFromStored(storedEvt);
    }

    private StoredPublishEvent convertToStored(PublishEvent evt) {
        StoredPublishEvent storedEvt = new StoredPublishEvent(evt);
        return storedEvt;
    }

    private PublishEvent convertFromStored(StoredPublishEvent evt) {
        PublishEvent liveEvt = null;
        if (null != evt) {
            byte[] message = evt.getMessage();
            ByteBuffer bbmessage = ByteBuffer.wrap(message);
            //bbmessage.flip();
            liveEvt = new PublishEvent(evt.getTopic(), evt.getQos(),
                    bbmessage, evt.isRetain(), evt.getClientID(), evt.getMessageID(), null);
        }
        return liveEvt;
    }
}
