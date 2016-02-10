/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.mb.migration;

/**
 * Class to represent the subscriptions stored in the database.
 */
public class Subscription {

    private String identifier;
    private String destinationType;
    private String subscriptionData;

    public Subscription(String id, String destination, String subData){
        identifier = id;
        destinationType = destination;
        subscriptionData = subData;
    }

    public String getIdentifier() {
        return identifier;
    }

    public String getDestinationType() {
        return destinationType;
    }

    public void setDestinationType(String aDestinationType) {
        this.destinationType = aDestinationType;
    }

    public String getSubscriptionData() {
        return subscriptionData;
    }

    public void setSubscriptionData(String newSubscriptionData) {
        this.subscriptionData = newSubscriptionData;
    }
}
