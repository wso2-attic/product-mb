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

package org.wso2.carbon.andes.service.exceptions;

/**
 * Exception class for the rest server when a message is not found.
 */
public class MessageNotFoundException extends Exception {
    private static final long serialVersionUID = -767298281755192181L;

    /**
     * Creates an exception with given string.
     *
     * @param exceptionString The exception message content.
     */
    public MessageNotFoundException(String exceptionString) {
        super(exceptionString);
    }
}
