package org.wso2.carbon.andes.internal;

import java.util.logging.Logger;

/**
 * AndesDataHolder to hold org.wso2.carbon.kernel.CarbonRuntime instance referenced through
 * org.wso2.carbon.helloworld.internal.AndesServiceComponent.
 *
 * @since 3.5.0-SNAPSHOT
 */
public class AndesDataHolder {
    Logger logger = Logger.getLogger(AndesDataHolder.class.getName());

    private static AndesDataHolder instance = new AndesDataHolder();

    private AndesDataHolder() {

    }

    /**
     * This returns the AndesDataHolder instance.
     *
     * @return The AndesDataHolder instance of this singleton class
     */
    public static AndesDataHolder getInstance() {
        return instance;
    }

}
