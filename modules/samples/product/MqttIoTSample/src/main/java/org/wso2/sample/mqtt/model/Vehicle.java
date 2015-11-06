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

package org.wso2.sample.mqtt.model;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.wso2.sample.mqtt.AndesMQTTClient;
import org.wso2.sample.mqtt.MQTTSampleConstants;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * This represents a Vehicle and is responsible for collecting sensor data for itself and sending them to the server.
 */
public class Vehicle {

    private String vehicleId;
    private VehicleModel vehicleModel;

    private AndesMQTTClient mqttClient;

    private double engineTemperature; // Celsius

    private double speed; // km/h

    private double acceleration; // m/s^2

    public static final String ENGINE_TEMPERATURE = "engintemperature";
    public static final String SPEED = "speed";
    public static final String ACCELERATION = "acceleration";

    private final int qos = 0;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    private final ScheduledFuture statusUpdateSchedule;

    private final Log log = LogFactory.getLog(Vehicle.class);

    /**
     * Create a new vehicle initialising vehicleId, Model and sensor data update process.
     *
     * @param vehicleId    The vehicle Id to create
     * @param vehicleModel The model of the vehicle
     * @throws MqttException
     */
    public Vehicle(final String vehicleId, VehicleModel vehicleModel) throws MqttException {
        setVehicleId(vehicleId);
        setVehicleModel(vehicleModel);

        mqttClient = new AndesMQTTClient(vehicleId, true,
        						MQTTSampleConstants.DEFAULT_USER_NAME, 
        						MQTTSampleConstants.DEFAULT_PASSWORD);

        // send sensor statuses to the server periodically.
        statusUpdateSchedule = scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                try {
                    // Send temperature reading
                    mqttClient.sendMessage(generateTopicHierarchy(ENGINE_TEMPERATURE),
                            org.wso2.sample.mqtt.AndesMQTTClient.TEMPERATURE_PREFIX
                                    + String.valueOf(getEngineTemperature()), qos);

                    // Send speed reading
                    mqttClient.sendMessage(generateTopicHierarchy(SPEED),
                            org.wso2.sample.mqtt.AndesMQTTClient.SPEED_PREFIX
                                    + String.valueOf(getSpeed()), qos);

                    // Send acceleration reading
                    mqttClient.sendMessage(generateTopicHierarchy(ACCELERATION),
                            org.wso2.sample.mqtt.AndesMQTTClient.ACCELERATION_PREFIX
                                    + String.valueOf(getAcceleration()), qos);

                    log.info("Sensor readings of " + vehicleId + " sent to server.");
                } catch (MqttException e) {
                    log.error("Error sending sensor data to server.", e);
                }
            }
        }, 0, 1, TimeUnit.SECONDS);

        log.info("A " + vehicleModel.getVehicleType().getTypeName() + " of model " + vehicleModel.getModelName() + " " +
                "created. Id : " + vehicleId);
    }

    public String getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(String vehicleId) {
        this.vehicleId = vehicleId;
    }

    public VehicleModel getVehicleModel() {
        return vehicleModel;
    }

    public void setVehicleModel(VehicleModel vehicleModel) {
        this.vehicleModel = vehicleModel;
    }

    public double getEngineTemperature() {
        return engineTemperature;
    }

    public void setEngineTemperature(double engineTemperature) {
        this.engineTemperature = engineTemperature;
        log.info("Engine temperature of " + vehicleId + " updated to " + engineTemperature);
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
        log.info("Speed of " + vehicleId + " updated to " + speed);
    }

    public double getAcceleration() {
        return acceleration;
    }

    public void setAcceleration(double acceleration) {
        this.acceleration = acceleration;
        log.info("Acceleration of " + vehicleId + " updated to " + acceleration);
    }

    /**
     * Generate the hierarchy the vehicle should publish data to in mqtt.
     *
     * @param leafTopic The leaf of the topic hierarchy
     * @return The topic hierarchy that is feed-able to the broker
     */
    public String generateTopicHierarchy(String leafTopic) {
        return vehicleModel.getVehicleType().getTypeName() + "/" + vehicleModel.getModelName() + "/" + vehicleId +
                "/" + leafTopic;
    }

    /**
     * Stop sending sensor data to the server and disconnect clients.
     *
     * @throws MqttException
     */
    public void stopSensorDataUpload() throws MqttException {
        statusUpdateSchedule.cancel(true);
        scheduledExecutorService.shutdown();
        mqttClient.disconnect();
    }
}
