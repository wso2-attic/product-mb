package org.wso2.carbon.andes.amqp.internal;

import org.wso2.andes.kernel.Andes;
import org.wso2.carbon.kernel.CarbonRuntime;

import java.util.logging.Logger;

/**
 * DataHolder to holds instances referenced through AMQP transport.
 *
 * @since 3.5.0-SNAPSHOT
 */
public class AMQPComponentDataHolder {
    Logger logger = Logger.getLogger(AMQPComponentDataHolder.class.getName());

    private static AMQPComponentDataHolder instance = new AMQPComponentDataHolder();
    private CarbonRuntime carbonRuntime;
    private Andes andesInstance;

    private AMQPComponentDataHolder() {

    }

    /**
     * This returns the DataHolder instance.
     *
     * @return The DataHolder instance of this singleton class
     */
    public static AMQPComponentDataHolder getInstance() {
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
     * ServiceComponent.
     *
     * @param carbonRuntime The reference being passed through ServiceComponent
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
