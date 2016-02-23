package org.wso2.carbon.andes.transports.mqtt.protocol.decoders;

import io.netty.buffer.ByteBuf;
import io.netty.util.AttributeMap;
import org.wso2.carbon.andes.transports.mqtt.protocol.messages.ConnAckMessage;

import java.util.List;

/**
 * @author andrea
 */
class ConnAckDecoder extends DemuxDecoder {

    @Override
    void decode(AttributeMap ctx, ByteBuf in, List<Object> out) throws Exception {
        in.resetReaderIndex();
        //Common decoding part
        ConnAckMessage message = new ConnAckMessage();
        if (!decodeCommonHeader(message, 0x00, in)) {
            in.resetReaderIndex();
            return;
        }
        //skip reserved byte
        in.skipBytes(1);

        //read  return code
        message.setReturnCode(in.readByte());
        out.add(message);
    }

}
