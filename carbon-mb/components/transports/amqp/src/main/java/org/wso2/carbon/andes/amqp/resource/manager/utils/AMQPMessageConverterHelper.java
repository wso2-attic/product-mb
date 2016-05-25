package org.wso2.carbon.andes.amqp.resource.manager.utils;

import com.gs.collections.impl.list.mutable.primitive.LongArrayList;
import com.gs.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.mina.common.ByteBuffer;
import org.wso2.andes.AMQException;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.framing.BasicContentHeaderProperties;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.disruptor.compression.LZ4CompressionHelper;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.transport.codec.BBDecoder;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.management.MBeanException;

/**
 *
 */
public class AMQPMessageConverterHelper {
    private static final String MIME_TYPE_TEXT_PLAIN = "text/plain";
    private static final String MIMI_TYPE_TEXT_XML = "text/xml";
    private static final String MIME_TYPE_APPLICATION_JAVA_OBJECT_STREAM = "application/java-object-stream";
    private static final String MIME_TYPE_AMQP_MAP = "amqp/map";
    private static final String MIME_TYPE_JMS_MAP_MESSAGE = "jms/map-message";
    private static final String MIME_TYPE_JMS_STREAM_MESSAGE = "jms/stream-message";
    private static final String MIME_TYPE_APPLICATION_OCTET_STREAM = "application/octet-stream";

    /**
     * Used to get configuration values related to compression and used to decompress message content
     */
    LZ4CompressionHelper lz4CompressionHelper = new LZ4CompressionHelper();


    /**
     * This is set when reading a byte array. The readBytes(byte[]) method supports multiple calls to read
     * a byte array in multiple chunks, hence this is used to track how much is left to be read
     */
    private int byteArrayRemaining = -1;


    private static final byte BOOLEAN_TYPE = (byte) 1;

    protected static final byte BYTE_TYPE = (byte) 2;

    protected static final byte BYTEARRAY_TYPE = (byte) 3;

    protected static final byte SHORT_TYPE = (byte) 4;

    protected static final byte CHAR_TYPE = (byte) 5;

    protected static final byte INT_TYPE = (byte) 6;

    protected static final byte LONG_TYPE = (byte) 7;

    protected static final byte FLOAT_TYPE = (byte) 8;

    protected static final byte DOUBLE_TYPE = (byte) 9;

    protected static final byte STRING_TYPE = (byte) 10;

    protected static final byte NULL_STRING_TYPE = (byte) 11;


    public Map<String, String> getJMSMessageProperties(AndesMessageMetadata andesMessageMetadata)
                                                                                                throws AndesException {
        try {
            Map<String, String> properties = new HashMap<>();
            //get AMQMessage from AndesMessageMetadata
            AMQMessage amqMessage = AMQPUtils.getAMQMessageFromAndesMetaData(andesMessageMetadata);
            //header amqMessageProperties from AMQMessage
            BasicContentHeaderProperties amqMessageProperties =
                    (BasicContentHeaderProperties) amqMessage.getContentHeaderBody().getProperties();
            //get custom header amqMessageProperties of AMQMessage
            for (String headerKey : amqMessageProperties.getHeaders().keys()) {
                properties.put(headerKey, amqMessageProperties.getHeaders().get(headerKey).toString());
            }
            properties.put("ContentType", amqMessageProperties.getContentTypeAsString());
            properties.put("MessageID", amqMessageProperties.getMessageIdAsString());
            properties.put("Redelivered", Boolean.FALSE.toString());
            properties.put("Timestamp", Long.toString(amqMessageProperties.getTimestamp()));

            return properties;
        } catch (AMQException e) {
            throw new AndesException("Error occurred when getting message properties.", e);
        }
    }

    public String getJMSMessageContent(AndesMessageMetadata andesMessageMetadata) throws AndesException {
        try {
            //get AMQMessage from AndesMessageMetadata
            AMQMessage amqMessage = AMQPUtils.getAMQMessageFromAndesMetaData(andesMessageMetadata);
            //content is constructing
            final int bodySize = (int) amqMessage.getSize();

            AndesMessagePart constructedContent = constructContent(bodySize, amqMessage);
            byte[] messageContent = constructedContent.getData();
            int position = constructedContent.getOffset();

            //if position did not proceed, there is an error receiving content. If not, decode content
            if (!((bodySize != 0) && (position == 0))) {
                return decodeContent(amqMessage, messageContent);
            }
        } catch (MBeanException e) {
            throw new AndesException("Error occurred when getting message content.", e);
        }

        return StringUtils.EMPTY;
    }

    /**
     * Method to construct message body of a single message.
     *
     * @param bodySize   Original content size of the message
     * @param amqMessage AMQMessage
     * @return Message content and last position of written data as an AndesMessagePart
     * @throws MBeanException
     */
    private AndesMessagePart constructContent(int bodySize, AMQMessage amqMessage) throws MBeanException {

        AndesMessagePart andesMessagePart;

        if (amqMessage.getMessageMetaData().isCompressed()) {
            /* If the current message was compressed by the server, decompress the message content and, get it as an
             * AndesMessagePart
             */
            LongArrayList messageToFetch = new LongArrayList();
            Long messageID = amqMessage.getMessageId();
            messageToFetch.add(messageID);

            try {
                LongObjectHashMap<List<AndesMessagePart>> contentListMap = MessagingEngine.getInstance()
                        .getContent(messageToFetch);
                List<AndesMessagePart> contentList = contentListMap.get(messageID);

                andesMessagePart = lz4CompressionHelper.getDecompressedMessage(contentList, bodySize);

            } catch (AndesException e) {
                throw new MBeanException(e, "Error occurred while construct the message content. Message ID:"
                                            + amqMessage.getMessageId());
            }
        } else {
            byte[] messageContent = new byte[bodySize];

            //Getting a buffer, to write data into the byte array and to the buffer at the same time
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(messageContent);

            int position = 0;

            while (position < bodySize) {
                position = position + amqMessage.getContent(buffer, position);

                //If position did not proceed, there is an error receiving content
                if ((0 == position)) {
                    break;
                }

                //The limit is setting to the current position and then the position of the buffer is setting to
                // zero
                buffer.flip();

                //The position of the buffer is setting to zero, the limit is setting to the capacity
                buffer.clear();
            }

            andesMessagePart = new AndesMessagePart();
            andesMessagePart.setData(messageContent);
            andesMessagePart.setOffSet(position);
        }

        return andesMessagePart;
    }

    /**
     * Method to decode content of a single message into text
     *
     * @param amqMessage     the message of which content need to be decoded
     * @param messageContent the byte array of message content to be decoded
     * @return A string array representing the decoded message content
     * @throws MBeanException
     */
    private String decodeContent(AMQMessage amqMessage, byte[] messageContent) throws MBeanException {

        try {
            //get encoding
            String encoding = amqMessage.getMessageHeader().getEncoding();
            if (encoding == null) {
                encoding = "UTF-8";
            }
            //get mime type of message
            String mimeType = amqMessage.getMessageHeader().getMimeType();
            // setting default mime type
            if (StringUtils.isBlank(mimeType)) {
                mimeType = MIME_TYPE_TEXT_PLAIN;
            }
            //create message content to readable text from ByteBuffer
            ByteBuffer wrapMsgContent = ByteBuffer.wrap(messageContent);
            String wholeMsg = "";

            //get TextMessage content to display
            switch (mimeType) {
                case MIME_TYPE_TEXT_PLAIN:
                case MIMI_TYPE_TEXT_XML:
                    wholeMsg = extractTextMessageContent(wrapMsgContent, encoding);
                    //get ByteMessage content to display
                    break;
                case MIME_TYPE_APPLICATION_OCTET_STREAM:
                    wholeMsg = extractByteMessageContent(wrapMsgContent, messageContent);
                    //get ObjectMessage content to display
                    break;
                case MIME_TYPE_APPLICATION_JAVA_OBJECT_STREAM:
                    wholeMsg = "This Operation is Not Supported!";
                    //get StreamMessage content to display
                    break;
                case MIME_TYPE_JMS_STREAM_MESSAGE:
                    wholeMsg = extractStreamMessageContent(wrapMsgContent, encoding);
                    //get MapMessage content to display
                    break;
                case MIME_TYPE_AMQP_MAP:
                case MIME_TYPE_JMS_MAP_MESSAGE:
                    wholeMsg = extractMapMessageContent(wrapMsgContent);
                    break;
                default:
                    wholeMsg = "This Operation is Not Supported!";
            }
            return wholeMsg;
        } catch (CharacterCodingException exception) {
            throw new MBeanException(exception, "Error occurred in browse queue.");
        }
    }

    /**
     * Extract MapMessage content from ByteBuffer
     *
     * @param wrapMsgContent ByteBuffer which contains data
     * @return extracted content as text
     */
    private String extractMapMessageContent(ByteBuffer wrapMsgContent) {
        wrapMsgContent.rewind();
        BBDecoder decoder = new BBDecoder();
        decoder.init(wrapMsgContent.buf());
        Map<String, Object> mapMassage = decoder.readMap();
        String wholeMsg = "";
        for (Map.Entry<String, Object> entry : mapMassage.entrySet()) {
            String mapName = entry.getKey();
            String mapVal = entry.getValue().toString();
            StringBuilder messageContentBuilder = new StringBuilder();
            wholeMsg = StringEscapeUtils.escapeHtml(messageContentBuilder.append(mapName).append(": ")
                    .append(mapVal).append(", ").toString()).trim();
        }
        return wholeMsg;
    }

    /**
     * Extract StreamMessage from ByteBuffer
     *
     * @param wrapMsgContent ByteBuffer which contains data
     * @param encoding message encoding
     * @return extracted content as text
     * @throws CharacterCodingException
     */
    private String extractStreamMessageContent(ByteBuffer wrapMsgContent, String encoding)
            throws CharacterCodingException {
        String wholeMsg;
        boolean eofReached = false;
        StringBuilder messageContentBuilder = new StringBuilder();

        while (!eofReached) {

            try {
                Object obj = readObject(wrapMsgContent, encoding);
                // obj could be null if the wire type is AbstractBytesTypedMessage.NULL_STRING_TYPE
                if (null != obj) {
                    messageContentBuilder.append(obj.toString()).append(", ");
                }
            } catch (JMSException e) {
                eofReached = true;
            }
        }

        wholeMsg = StringEscapeUtils.escapeHtml(messageContentBuilder.toString());
        return wholeMsg;
    }

    /**
     * Extract ByteMessage from ByteBuffer
     *
     * @param wrapMsgContent ByteBuffer which contains data
     * @param byteMsgContent byte[] of message content
     * @return extracted content as text
     */
    private String extractByteMessageContent(ByteBuffer wrapMsgContent, byte[] byteMsgContent) {
        String wholeMsg;
        if (byteMsgContent == null) {
            throw new IllegalArgumentException("byte array must not be null");
        }
        int count = (wrapMsgContent.remaining() >= byteMsgContent.length ?
                     byteMsgContent.length : wrapMsgContent.remaining());
        if (count == 0) {
            wholeMsg = String.valueOf(-1);
        } else {
            wrapMsgContent.get(byteMsgContent, 0, count);
            wholeMsg = String.valueOf(count);
        }
        return wholeMsg;
    }

    /**
     * Extract TextMessage from ByteBuffer
     *
     * @param wrapMsgContent ByteBuffer which contains data
     * @param encoding message encoding
     * @return extracted content as text
     * @throws CharacterCodingException
     */
    private String extractTextMessageContent(ByteBuffer wrapMsgContent, String encoding)
            throws CharacterCodingException {
        String wholeMsg;
        wholeMsg = wrapMsgContent.getString(Charset.forName(encoding).newDecoder());
        return wholeMsg;
    }



    /**
     * Read object from StreamMessage ByteBuffer content
     *
     * @param wrapMsgContent ByteBuffer which contains data
     * @param encoding message encoding
     * @return Object extracted from ByteBuffer
     * @throws JMSException
     * @throws CharacterCodingException
     */
    private Object readObject(ByteBuffer wrapMsgContent, String encoding) throws JMSException,
            CharacterCodingException {
        int position = wrapMsgContent.position();
        checkAvailable(1, wrapMsgContent);
        byte wireType = wrapMsgContent.get();
        Object result = null;
        try {
            switch (wireType) {
                case BOOLEAN_TYPE:
                    checkAvailable(1, wrapMsgContent);
                    result = wrapMsgContent.get() != 0;
                    break;
                case BYTE_TYPE:
                    checkAvailable(1, wrapMsgContent);
                    result = wrapMsgContent.get();
                    break;
                case BYTEARRAY_TYPE:
                    checkAvailable(4, wrapMsgContent);
                    int size = wrapMsgContent.getInt();
                    if (size == -1) {
                        result = null;
                    } else {
                        byteArrayRemaining = size;
                        byte[] bytesResult = new byte[size];
                        readBytesImpl(wrapMsgContent, bytesResult);
                        result = bytesResult;
                    }
                    break;
                case SHORT_TYPE:
                    checkAvailable(2, wrapMsgContent);
                    result = wrapMsgContent.getShort();
                    break;
                case CHAR_TYPE:
                    checkAvailable(2, wrapMsgContent);
                    result = wrapMsgContent.getChar();
                    break;
                case INT_TYPE:
                    checkAvailable(4, wrapMsgContent);
                    result = wrapMsgContent.getInt();
                    break;
                case LONG_TYPE:
                    checkAvailable(8, wrapMsgContent);
                    result = wrapMsgContent.getLong();
                    break;
                case FLOAT_TYPE:
                    checkAvailable(4, wrapMsgContent);
                    result = wrapMsgContent.getFloat();
                    break;
                case DOUBLE_TYPE:
                    checkAvailable(8, wrapMsgContent);
                    result = wrapMsgContent.getDouble();
                    break;
                case NULL_STRING_TYPE:
                    result = null;
                    break;
                case STRING_TYPE:
                    checkAvailable(1, wrapMsgContent);
                    result = wrapMsgContent.getString(Charset.forName(encoding).newDecoder());
                    break;
                default:
                    result = null;
            }
            return result;
        } catch (RuntimeException e) {
            wrapMsgContent.position(position);
            throw e;
        }
    }

    /**
     * Check that there is at least a certain number of bytes available to read
     *
     * @param length the number of bytes
     * @throws javax.jms.MessageEOFException if there are less than len bytes available to read
     */
    private void checkAvailable(int length, ByteBuffer byteBuffer) throws MessageEOFException {
        if (byteBuffer.remaining() < length) {
            throw new MessageEOFException("Unable to read " + length + " bytes");
        }
    }

    /**
     * Read byte[] array object and return length
     *
     * @param wrapMsgContent ByteBuffer which contains data
     * @param bytes byte[] object
     * @return length of byte[] array
     */
    private int readBytesImpl(ByteBuffer wrapMsgContent, byte[] bytes) {
        int count = (byteArrayRemaining >= bytes.length ? bytes.length : byteArrayRemaining);
        byteArrayRemaining -= count;
        if (count == 0) {
            return 0;
        } else {
            wrapMsgContent.get(bytes, 0, count);
            return count;
        }
    }
}
