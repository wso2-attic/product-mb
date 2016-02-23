/*
 * Copyright (c) 2012-2014 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package org.wso2.carbon.andes.transports.mqtt.protocol.messages;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author andrea
 */
public class SubAckMessage extends MessageIDMessage {

    private byte returnCode;

    // MQTT spec 3.1.1 specific return codes
    public static final byte FORBIDDEN_SUBSCRIPTION = 0x08;

    List<AbstractMessage.QOSType> types = new ArrayList<AbstractMessage.QOSType>();
    
    public SubAckMessage() {
        messageType = SUBACK;
    }

    public List<AbstractMessage.QOSType> types() {
        return types;
    }

    public void addType(AbstractMessage.QOSType type) {
        types.add(type);
    }

    public byte getreturnCode() {
        return returnCode;
    }

    public void setreturnCode(byte returnCode) {
        this.returnCode = returnCode;
    }
}
