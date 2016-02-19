/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.carbon.andes.services.types;

/**
 * Message representation class
 */
public class Message {

    private String msgProperties;
    private String contentType;
    private String[] messageContent;
    private String jmsMessageId;
    private String jmsCorrelationId;
    private String jmsType;
    private Boolean jmsReDelivered;
    private Integer jmsDeliveredMode;
    private Integer jmsPriority;
    private Long jmsTimeStamp;
    private Long jmsExpiration;
    private String dlcMsgDestination;
    private Long andesMsgMetadataId;

    public String getMsgProperties() {
        return msgProperties;
    }

    public void setMsgProperties(String msgProperties) {
        this.msgProperties = msgProperties;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public String[] getMessageContent() {
        return messageContent;
    }

    public void setMessageContent(String[] messageContent) {
        this.messageContent = messageContent;
    }

    public String getJMSMessageId() {
        return jmsMessageId;
    }

    public void setJMSMessageId(String jmsMessageId) {
        this.jmsMessageId = jmsMessageId;
    }

    public String getJMSCorrelationId() {
        return jmsCorrelationId;
    }

    public void setJMSCorrelationId(String jmsCorrelationId) {
        this.jmsCorrelationId = jmsCorrelationId;
    }

    public String getJMSType() {
        return jmsType;
    }

    public void setJMSType(String jmsType) {
        this.jmsType = jmsType;
    }

    public Boolean getJMSReDelivered() {
        return jmsReDelivered;
    }

    public void setJMSReDelivered(Boolean jmsReDelivered) {
        this.jmsReDelivered = jmsReDelivered;
    }

    public Integer getJMSDeliveredMode() {
        return jmsDeliveredMode;
    }

    public void setJMSDeliveredMode(Integer jmsDeliveredMode) {
        this.jmsDeliveredMode = jmsDeliveredMode;
    }

    public Integer getJMSPriority() {
        return jmsPriority;
    }

    public void setJMSPriority(Integer jmsPriority) {
        this.jmsPriority = jmsPriority;
    }

    public Long getJMSTimeStamp() {
        return jmsTimeStamp;
    }

    public void setJMSTimeStamp(Long jmsTimeStamp) {
        this.jmsTimeStamp = jmsTimeStamp;
    }

    public Long getJMSExpiration() {
        return jmsExpiration;
    }

    public void setJMSExpiration(Long jmsExpiration) {
        this.jmsExpiration = jmsExpiration;
    }

    public String getDlcMsgDestination() {
        return dlcMsgDestination;
    }

    public void setDlcMsgDestination(String dlcMsgDestination) {
        this.dlcMsgDestination = dlcMsgDestination;
    }

    public long getAndesMsgMetadataId() {
        return andesMsgMetadataId;
    }

    public void setAndesMsgMetadataId(long andesMsgMetadataId) {
        this.andesMsgMetadataId = andesMsgMetadataId;
    }
}
