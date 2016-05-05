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

package org.wso2.carbon.andes.services.types;

import java.util.List;

/**
 * This class represent a cluster information object.
 */
public class ClusterInformation {
    private boolean isClusteringEnabled;
    private String nodeID;
    private String coordinatorAddress;
    private List<NodeInformation> nodeAddresses;

    public boolean isClusteringEnabled() {
        return isClusteringEnabled;
    }

    public void setClusteringEnabled(boolean clusteringEnabled) {
        isClusteringEnabled = clusteringEnabled;
    }

    public String getNodeID() {
        return nodeID;
    }

    public void setNodeID(String nodeID) {
        this.nodeID = nodeID;
    }

    public String getCoordinatorAddress() {
        return coordinatorAddress;
    }

    public void setCoordinatorAddress(String coordinatorAddress) {
        this.coordinatorAddress = coordinatorAddress;
    }

    public List<NodeInformation> getNodeAddresses() {
        return nodeAddresses;
    }

    public void setNodeAddresses(List<NodeInformation> nodeAddresses) {
        this.nodeAddresses = nodeAddresses;
    }
}
