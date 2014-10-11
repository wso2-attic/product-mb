/*
*  Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.mb.integration.tests;

/***
 * This class contains all common values passed to test the expiration test cases.
 */
public class JMSTestConstants {

    public static final Integer DEFAULT_TOTAL_SEND_MESSAGE_COUNT = 1000;

    //These two properties define how much of a percentage % will be made by non-expiring and expiring messages.
    public static final Integer SEND_MESSAGE_PERCENTAGE_WITHOUT_EXPIRY = 60;
    public static final Integer SEND_MESSAGE_PERCENTAGE_WITH_EXPIRY = 40;

    public static final Integer SAMPLE_JMS_EXPIRATION = 100;
    public static final Integer DEFAULT_RECEIVER_RUN_TIME_IN_SECONDS = 30;
    public static final Integer DEFAULT_SENDER_RUN_TIME_IN_SECONDS = 20;

    public static final Integer STANDARD_DELAY_BETWEEN_MESSAGES = 0;

}
