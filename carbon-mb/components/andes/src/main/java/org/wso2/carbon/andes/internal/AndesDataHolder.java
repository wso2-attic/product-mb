/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.carbon.andes.internal;

import org.wso2.carbon.hazelcast.CarbonHazelcastAgent;

import java.util.logging.Logger;

/**
 * AndesDataHolder to hold {@link CarbonHazelcastAgent} instance referenced through {@link AndesServiceComponent}.
 *
 */
public class AndesDataHolder {
    Logger logger = Logger.getLogger(AndesDataHolder.class.getName());

    private static AndesDataHolder instance = new AndesDataHolder();
    private CarbonHazelcastAgent carbonHazelcastAgent;

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

    /**
     * Returns the {@link CarbonHazelcastAgent} service which gets set through a service component.
     *
     * @return {@link CarbonHazelcastAgent} Service
     */
    public CarbonHazelcastAgent getCarbonHazelcastAgent() {
        return carbonHazelcastAgent;
    }

    /**
     * This method is for setting the {@link CarbonHazelcastAgent} service. This method is used by
     * {@link AndesServiceComponent}.
     *
     * @param carbonHazelcastAgent The reference being passed through {@link AndesServiceComponent}
     */
    public void setHazelcastAgent(CarbonHazelcastAgent carbonHazelcastAgent) {
        this.carbonHazelcastAgent = carbonHazelcastAgent;
    }
}
