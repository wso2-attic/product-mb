package org.wso2.carbon.andes.internal;

import org.wso2.carbon.hazelcast.CarbonHazelcastAgent;
import org.wso2.carbon.kernel.CarbonRuntime;

import java.util.logging.Logger;

/**
 * DataHolder to hold org.wso2.carbon.kernel.CarbonRuntime instance referenced through
 * org.wso2.carbon.helloworld.internal.ServiceComponent.
 *
 * @since 3.5.0-SNAPSHOT
 */
public class AndesDataHolder {
    Logger logger = Logger.getLogger(AndesDataHolder.class.getName());

    private static AndesDataHolder instance = new AndesDataHolder();
    private CarbonHazelcastAgent carbonHazelcastAgent;

    private AndesDataHolder() {

    }

    /**
     * This returns the DataHolder instance.
     *
     * @return The DataHolder instance of this singleton class
     */
    public static AndesDataHolder getInstance() {
        return instance;
    }

    /**
     * Returns the CarbonRuntime service which gets set through a service component.
     *
     * @return CarbonRuntime Service
     */
    public CarbonHazelcastAgent getCarbonHazelcastAgent() {
        return carbonHazelcastAgent;
    }

    /**
     * This method is for setting the CarbonRuntime service. This method is used by
     * ServiceComponent.
     *
     * @param carbonRuntime The reference being passed through ServiceComponent
     */
    public void setHazelcastAgent(CarbonHazelcastAgent carbonRuntime) {
        this.carbonHazelcastAgent = carbonRuntime;
    }
}
