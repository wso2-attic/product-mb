package org.wso2.carbon.andes.resource.manager.utils;

import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.ProtocolType;
import org.wso2.carbon.andes.resource.manager.types.Message;

import java.util.Map;

/**
 *
 */
public class MessageConverterHelper {
    public Message getAndesMessageMetadataAsMessage(ProtocolType protocolType, DestinationType
            destinationType, AndesMessageMetadata andesMessageMetadata, Map<String, String> messageProperties, String
            messageContent) throws AndesException {
        Message message = new Message();
        message.setAndesMsgMetadataId(andesMessageMetadata.getMessageID());
        message.setDestination(andesMessageMetadata.getDestination());
        message.setMessageProperties(messageProperties);
        message.setMessageContent(messageContent);
        message.setProtocolType(protocolType);
        message.setDestinationType(destinationType);
        return message;
    }
}
