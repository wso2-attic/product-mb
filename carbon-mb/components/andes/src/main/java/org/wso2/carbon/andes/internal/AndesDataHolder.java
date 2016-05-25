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

import com.hazelcast.core.HazelcastInstance;
import org.wso2.carbon.datasource.core.api.DataSourceService;
import org.wso2.carbon.kernel.CarbonRuntime;
import org.wso2.carbon.security.caas.user.core.service.RealmService;

import java.util.logging.Logger;

/**
 * AndesDataHolder to hold {@link HazelcastInstance} instance referenced through {@link AndesServiceComponent}.
 *
 */
public class AndesDataHolder {
    Logger logger = Logger.getLogger(AndesDataHolder.class.getName());

    private static AndesDataHolder instance = new AndesDataHolder();
    private HazelcastInstance carbonHazelcastAgent;
    private CarbonRuntime carbonRuntime;
    private RealmService realmService;

    /**
     * The datasource service instance provided by OSGI.
     */
    private DataSourceService dataSourceService;

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
     * Returns the {@link HazelcastInstance} service which gets set through a service component.
     *
     * @return {@link HazelcastInstance} Service
     */
    public HazelcastInstance getCarbonHazelcastAgent() {
        return carbonHazelcastAgent;
    }

    /**
     * This method is for setting the {@link HazelcastInstance} service. This method is used by
     * {@link AndesServiceComponent}.
     *
     * @param carbonHazelcastAgent The reference being passed through {@link AndesServiceComponent}
     */
    public void setHazelcastInstance(HazelcastInstance carbonHazelcastAgent) {
        this.carbonHazelcastAgent = carbonHazelcastAgent;
    }


    /**
     * Get the data source service reference.
     *
     * @return The data source service instance
     */
    public DataSourceService getDataSourceService() {
        return dataSourceService;
    }

    /**
     * Initialize the data source service reference with a new reference.
     *
     * @param dataSourceManager The new data source service instance
     */
    public void setDataSourceService(DataSourceService dataSourceManager) {
        this.dataSourceService = dataSourceManager;
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

    public void setRealmService(RealmService realmService) {
        this.realmService = realmService;
    }

    public RealmService getRealmService() {
        return realmService;
    }
}
