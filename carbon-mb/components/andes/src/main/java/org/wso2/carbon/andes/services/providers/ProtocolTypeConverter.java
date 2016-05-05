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

package org.wso2.carbon.andes.services.providers;

import org.wso2.andes.kernel.ProtocolType;

import java.util.Locale;
import javax.ws.rs.ext.ParamConverter;

/**
 * Converts jax rs params to destionation enum
 *
 * @param <T> Protocol
 */

/**
 * Sample Interceptor which logs HTTP headers of the request.
 */
//@Component(
//        name = "org.wso2.mb.service.converters.PrototypeConverter",
//        service = ParamConverter.class,
//        immediate = true
//)
public class ProtocolTypeConverter implements ParamConverter<ProtocolType> {
    @Override
    public ProtocolType fromString(String protocolAsString) {
        return ProtocolType.valueOf(protocolAsString.toUpperCase(Locale.getDefault()));
    }

    @Override
    public String toString(ProtocolType protocolType) {
        if (protocolType == null) {
            return null;
        }
        return protocolType.toString();
    }
}
