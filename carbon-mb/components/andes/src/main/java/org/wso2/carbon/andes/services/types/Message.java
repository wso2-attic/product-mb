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

import java.util.Map;

/**
 * * This class represent a message information object.
 */
public class Message {
    private Long andesMsgMetadataId;
    private String destination;
    private Map<String, String> messageProperties;
    private String[] messageContent;

    public Long getAndesMsgMetadataId() {
        return andesMsgMetadataId;
    }

    public void setAndesMsgMetadataId(Long andesMsgMetadataId) {
        this.andesMsgMetadataId = andesMsgMetadataId;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public Map<String, String> getMessageProperties() {
        return messageProperties;
    }

    public void setMessageProperties(Map<String, String> messageProperties) {
        this.messageProperties = messageProperties;
    }

    public String[] getMessageContent() {
        return messageContent;
    }

    public void setMessageContent(String[] messageContent) {
        this.messageContent = messageContent;
    }
}
