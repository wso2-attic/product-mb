/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.wso2.sample.mqtt;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.wso2.sample.mqtt.model.Vehicle;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The mqtt client implementation for the sample.
 * This keeps hold of the latest data received in a topic related to sensors.
 */
public class AndesMQTTClient implements MqttCallback {

    private MqttClient mqttClient;

    // Latest temperature readings received from the server <topic, value>
    private final ConcurrentHashMap<String, String> latestTemperatureReadings = new ConcurrentHashMap<String, String>();

    // Latest Speed readings received from the server <topic, value>
    private final ConcurrentHashMap<String, String> latestSpeedReadings = new ConcurrentHashMap<String, String>();

    // Latest acceleration readings received from the server <topic, value>
    private final ConcurrentHashMap<String, String> latestAccelerationReadings = new ConcurrentHashMap<String,
            String>();

    /**
     * Create new mqtt client with the given clientId.
     *
     * @param clientId The unique client Id
     * @throws MqttException
     */
    public AndesMQTTClient(String clientId) throws MqttException {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        mqttClient = new MqttClient(MQTTSampleConstants.BROKER_URL, clientId,
                new MqttDefaultFilePersistence(MQTTSampleConstants.TMP_DIR + File.pathSeparator + clientId));
        mqttClient.setCallback(this);
        mqttClient.connect(options);
    }

    /**
     * Subscribe to a topic.
     *
     * @param topic The topic to subscribe
     * @param qos   The quality of service level
     * @throws MqttException
     */
    public void subscribe(String topic, int qos) throws MqttException {
        mqttClient.subscribe(topic, qos);
    }

    /**
     * Un-subscribe from a topic.
     *
     * @param topic The topic to un-subscribe from
     * @throws MqttException
     */
    public void unsubscribe(String topic) throws MqttException {
        mqttClient.unsubscribe(topic);
    }

    /**
     * Send a message to mqtt server.
     *
     * @param topic   The topic to send message to
     * @param message The message string to send
     * @param qos     The quality of service level
     * @throws MqttException
     */
    public void sendMessage(String topic, String message, int qos) throws MqttException {
        mqttClient.publish(topic, message.getBytes(), qos, false);
    }

    /**
     * Disconnect the mqtt client.
     *
     * @throws MqttException
     */
    public void disconnect() throws MqttException {
        mqttClient.disconnect();
    }

    @Override
    public void connectionLost(Throwable throwable) {
        // not implemented
    }

    /**
     * Handle received messages from mqtt server.
     * If the received message is from one of the vehicle sensors, keep the latest one in memory to be retrieved by a
     * third party.
     *
     * @param topic       The topic message received from
     * @param mqttMessage The mqtt message received
     * @throws Exception
     */
    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
        String sensorReading = mqttMessage.toString();
        if (topic.endsWith(Vehicle.ENGINETEMPERATURE)) {
            latestTemperatureReadings.put(topic, sensorReading);
        } else if (topic.endsWith(Vehicle.SPEED)) {
            latestSpeedReadings.put(topic, sensorReading);
        } else if (topic.endsWith(Vehicle.ACCELERATION)) {
            latestAccelerationReadings.put(topic, sensorReading);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
    }

    /**
     * Get last temperature readings received.
     *
     * @return Received temperatures <topic, value>
     */
    public ConcurrentHashMap<String, String> getLatestTemperatureReadings() {
        return latestTemperatureReadings;
    }

    /**
     * Get last speed readings.
     *
     * @return Received speed readings <topic, value>
     */
    public ConcurrentHashMap<String, String> getLatestSpeedReadings() {
        return latestSpeedReadings;
    }

    /**
     * Get last acceleration reading.
     *
     * @return Received acceleration readings <topic, value>
     */
    public ConcurrentHashMap<String, String> getLatestAccelerationReadings() {
        return latestAccelerationReadings;
    }
}
