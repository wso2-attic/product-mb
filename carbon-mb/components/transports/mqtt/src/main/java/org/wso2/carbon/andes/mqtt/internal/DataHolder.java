package org.wso2.carbon.andes.mqtt.internal;

import org.wso2.andes.kernel.Andes;
import org.wso2.carbon.kernel.CarbonRuntime;

import java.util.logging.Logger;

/**
 * DataHolder to holds instances referenced through org.wso2.carbon.helloworld.internal.MqttTransportServiceComponent.
 *
 * @since 3.5.0-SNAPSHOT
 */
public class DataHolder {
    Logger logger = Logger.getLogger(DataHolder.class.getName());

    private static DataHolder instance = new DataHolder();
    private CarbonRuntime carbonRuntime;
    private Andes andesInstance;

    private DataHolder() {

    }

    /**
     * This returns the DataHolder instance.
     *
     * @return The DataHolder instance of this singleton class
     */
    public static DataHolder getInstance() {
        return instance;
    }

    /**
     * Returns the CarbonRuntime service which gets set through a service component.
     *
     * @return CarbonRuntime Service
     */
    public CarbonRuntime getCarbonRuntime() {
        return carbonRuntime;
    }

    /**
     * This method is for setting the CarbonRuntime service. This method is used by
     * MqttTransportServiceComponent.
     *
     * @param carbonRuntime The reference being passed through MqttTransportServiceComponent
     */
    public void setCarbonRuntime(CarbonRuntime carbonRuntime) {
        this.carbonRuntime = carbonRuntime;
    }

    public Andes getAndesInstance() {
        return andesInstance;
    }

    public void setAndesInstance(Andes andesInstance) {
        this.andesInstance = andesInstance;
    }
}
