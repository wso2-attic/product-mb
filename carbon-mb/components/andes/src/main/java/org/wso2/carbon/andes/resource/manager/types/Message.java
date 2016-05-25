package org.wso2.carbon.andes.resource.manager.types;

import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolType;

import java.util.Map;

/**
 *
 */
public class Message {
    private Long andesMsgMetadataId;
    private String destination;
    private Map<String, String> messageProperties;
    private String messageContent;
    private ProtocolType protocolType;
    private DestinationType destinationType;

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

    public String getMessageContent() {
        return messageContent;
    }

    public void setMessageContent(String messageContent) {
        this.messageContent = messageContent;
    }

    public ProtocolType getProtocolType() {
        return protocolType;
    }

    public void setProtocolType(ProtocolType protocolType) {
        this.protocolType = protocolType;
    }

    public DestinationType getDestinationType() {
        return destinationType;
    }

    public void setDestinationType(DestinationType destinationType) {
        this.destinationType = destinationType;
    }
}
