/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.carbon.andes.mqtt;

import org.dna.mqtt.moquette.server.IAuthenticator;

/**
 * A dummy class which represents an MQTT authentication for moquette server.
 */
public class DummyAuthenticator implements IAuthenticator {

    /**
     * Returns valid only for admin/admin.
     *
     * @param username
     * @param password
     * @return
     */
    @Override
    public boolean checkValid(String username, String password) {
        boolean valid = false;

        if ("admin".equals(username) && "admin".equals(password)) {
            valid = true;
        }

        return valid;
    }
}
