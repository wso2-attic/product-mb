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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.wso2.sample.mqtt.model.Vehicle;
import org.wso2.sample.mqtt.model.VehicleModel;
import org.wso2.sample.mqtt.model.VehicleType;

import java.util.*;
import java.util.concurrent.*;

/**
 * This samples demonstrates how WSO2 Message Broker MQTT can be used to publish data from running vehicles to a
 * central server and use that data to analyze and come to conclusions.
 * <p/>
 * The main class which executes the sample.
 * - Creates mock vehicle types, vehicle models and vehicles
 * - Updates sensor readings periodically in each vehicle with a random value mocking the sensor behaviours
 * - Read sensor data published by all vehicles via mqtt server and generate mock output scenarios.
 * ~ Real time speed of a given vehicle
 * ~ Real time average temperature of all the vehicles
 * ~ Real time maximum speed of a given vehicle type
 */
public class Main {

    private static final List<Vehicle> vehicleList = new ArrayList<Vehicle>();
    private static final Map<vehicleTypes, VehicleType> vehicleTypeMap = new HashMap<vehicleTypes, VehicleType>();
    private static final Set<VehicleModel> vehicleModelSet = new HashSet<VehicleModel>();

    private static final Random random = new Random();

    private static enum vehicleTypes {CAR, BIKE, VAN}

    private static AndesMQTTClient temperatureClient;
    private static AndesMQTTClient carClient;
    private static AndesMQTTClient harleySpeedClient;

    private static final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);
    private static ScheduledFuture vehicleStatusUpdater;
    private static ScheduledFuture vehicleStatusProcessor;

    /**
     * Time to run the sample in seconds
     */
    private static final int RUNTIME = 20000;

    private static final Log log = LogFactory.getLog(Main.class);

    /**
     * Executes vehicle population, mock sensor reading update and sensor data reading.
     *
     * @param args main command line arguments
     * @throws InterruptedException
     * @throws MqttException
     */
    public static void main(String[] args) throws InterruptedException, MqttException {
        populateVehicles();
        try {
            listenToVehicleSensorStatuses();
            scheduleMockVehicleStatusUpdate();

            // Let stats publish and stats processing commence for RUNTIME amount of time before exiting the sample
            Thread.sleep(RUNTIME);
            shutdown();
        } catch (MqttException e) {
            log.error("Error running the sample.", e);
        }
    }

    /**
     * Schedule to periodically update sensor readings of all vehicles.
     */
    private static void scheduleMockVehicleStatusUpdate() {
        // Update sensors with random values.
        vehicleStatusUpdater = scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                for (Vehicle vehicle : vehicleList) {
                    vehicle.setAcceleration(random.nextInt(360));
                    vehicle.setSpeed(random.nextInt(200));
                    vehicle.setEngineTemperature(random.nextInt(350));
                }

            }
        }, 0, 500, TimeUnit.MILLISECONDS);
    }

    /**
     * Populate vehicles types with CAR, BIKE and VAN.
     */
    private static void populateVehicleTypes() {
        // CAR
        VehicleType car = new VehicleType(vehicleTypes.CAR.name());

        // BIKE
        VehicleType bike = new VehicleType(vehicleTypes.BIKE.name());

        // VAN
        VehicleType van = new VehicleType(vehicleTypes.VAN.name());

        vehicleTypeMap.put(vehicleTypes.CAR, car);
        vehicleTypeMap.put(vehicleTypes.BIKE, bike);
        vehicleTypeMap.put(vehicleTypes.VAN, van);
    }

    /**
     * Populate vehicle models.
     * - 5 models of type CAR
     * - 5 models of type BIKE
     * - 3 models of type VAN
     */
    private static void populateVehicleModels() {
        populateVehicleTypes();

        VehicleType car = vehicleTypeMap.get(vehicleTypes.CAR);

        String bmwM3 = "BMWM3";
        String carreraGt = "PorscheCarreraGT";
        String mclarenF1 = "McLarnF1";
        String challenger = "DodgeChallenger";
        String mercielago = "LanborginiMercielago";

        vehicleModelSet.add(new VehicleModel(bmwM3, car));
        vehicleModelSet.add(new VehicleModel(carreraGt, car));
        vehicleModelSet.add(new VehicleModel(mclarenF1, car));
        vehicleModelSet.add(new VehicleModel(challenger, car));
        vehicleModelSet.add(new VehicleModel(mercielago, car));

        VehicleType bike = vehicleTypeMap.get(vehicleTypes.BIKE);

        String nightRod = "HarleyDavidsonNightRod";
        String h2r = "KawasakiH2R";
        String scrambler = "DucatiScramblerFullThrottle";
        String vfr = "HondaVFR800XCrossrunner";
        String gsx = "SuzukiGSX-S1000";

        vehicleModelSet.add(new VehicleModel(nightRod, bike));
        vehicleModelSet.add(new VehicleModel(h2r, bike));
        vehicleModelSet.add(new VehicleModel(scrambler, bike));
        vehicleModelSet.add(new VehicleModel(vfr, bike));
        vehicleModelSet.add(new VehicleModel(gsx, bike));

        VehicleType van = vehicleTypeMap.get(vehicleTypes.VAN);

        String odyssey = "HondaOdyssey";
        String grandCaravan = "DodgeGrandCaravan";
        String sedona = "KiaSedona";


        vehicleModelSet.add(new VehicleModel(odyssey, van));
        vehicleModelSet.add(new VehicleModel(grandCaravan, van));
        vehicleModelSet.add(new VehicleModel(sedona, van));
    }

    /**
     * Create mock vehicles, 1 per each model.
     *
     * @throws MqttException
     */
    private static void populateVehicles() throws MqttException {
        populateVehicleModels();

        int i = 0;

        // 1 vehicle per each vehicle model
        for (VehicleModel vehicleModel : vehicleModelSet) { //todo : i++
            vehicleList.add(new Vehicle("Vehicle" + i++, vehicleModel));
        }
    }

    /**
     * Read vehicle sensor updates from mqtt and output real time values.
     *
     * @throws MqttException
     */
    private static void listenToVehicleSensorStatuses() throws MqttException {
        temperatureClient = new AndesMQTTClient("temperatureClient", true, MQTTSampleConstants.DEFAULT_USER_NAME, MQTTSampleConstants.DEFAULT_PASSWORD);
        temperatureClient.subscribe("+/+/+/" + Vehicle.ENGINE_TEMPERATURE, 1);

        carClient = new AndesMQTTClient("carClient", true, MQTTSampleConstants.DEFAULT_USER_NAME, MQTTSampleConstants.DEFAULT_PASSWORD);
        carClient.subscribe(vehicleTypes.CAR.name() + "/#", 1);

        harleySpeedClient = new AndesMQTTClient("harleySpeedClient", true, MQTTSampleConstants.DEFAULT_USER_NAME, MQTTSampleConstants.DEFAULT_PASSWORD);
        harleySpeedClient.subscribe(vehicleTypes.BIKE.name() + "/HarleyDavidsonNightRod/#", 1);

        // Print real time sensor data each second
        vehicleStatusProcessor = scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            public void run() {

                {
                    StringBuilder outputString = new StringBuilder();
                    // Print the speed of Harley
                    ConcurrentHashMap<String, String> latestHarleyReadings = harleySpeedClient.getLatestSpeedReadings();
                    for (Map.Entry<String, String> entry : latestHarleyReadings.entrySet()) {
                        String latestReading = entry.getValue();
                        outputString.append("Latest Harley Speed Reading : ").append(latestReading);
                    }

                    // Print the average temperature of all vehicles
                    ConcurrentHashMap<String, String> latestTemperatureReadings = temperatureClient
                            .getLatestTemperatureReadings();
                    double totalTemperaturesSum = 0;
                    for (Map.Entry<String, String> entry : latestTemperatureReadings.entrySet()) {
                        String latestReading = entry.getValue();
                        if (!StringUtils.isEmpty(latestReading)) {
                            totalTemperaturesSum = totalTemperaturesSum + Double.parseDouble(latestReading);
                        }
                    }

                    outputString.append("\tLatest Average Temperature of All vehicles : ").append
                            (totalTemperaturesSum / latestTemperatureReadings.size());

                    // Print the CAR which has the maximum acceleration at the moment
                    ConcurrentHashMap<String, String> latestCarAccelerationReadings = carClient
                            .getLatestSpeedReadings();
                    double maxAcceleration = 0;
                    for (Map.Entry<String, String> entry : latestCarAccelerationReadings.entrySet()) {
                        String latestReading = entry.getValue();
                        if (latestReading != null && !"".equals(latestReading)) {
                            double latestAcceleration = Double.parseDouble(latestReading);
                            if (maxAcceleration < latestAcceleration) {
                                maxAcceleration = latestAcceleration;
                            }
                        }
                    }

                    outputString.append("\tCurrent Max car acceleration : ").append(maxAcceleration);

                    log.info(outputString);


                }
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    /**
     * Disconnect all mqtt clients and stop invoked schedules.
     *
     * @throws MqttException
     */
    private static void shutdown() throws MqttException {
        log.info("Stopping sample");
        temperatureClient.disconnect();
        carClient.disconnect();
        harleySpeedClient.disconnect();

        for (Vehicle vehicle : vehicleList) {
            vehicle.stopSensorDataUpload();
        }

        vehicleStatusUpdater.cancel(true);
        vehicleStatusProcessor.cancel(true);

        scheduledExecutorService.shutdown();
    }
}
