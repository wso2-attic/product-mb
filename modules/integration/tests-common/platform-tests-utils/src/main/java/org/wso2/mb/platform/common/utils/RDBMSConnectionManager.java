/*
*  Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.mb.platform.common.utils;

import org.wso2.mb.platform.common.utils.exceptions.DataAccessUtilException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * This class is used for creating RDBMS Connection
 */
public class RDBMSConnectionManager {

    // Set database username
    private static final String username = "";
    // Set database password
    private static final String password = "";
    // Set JDBC URL
    private static final String url = "jdbc:mysql://localhost/WSO2_MB";
    // Set JDBC Driver
    private static final String driverName = "com.mysql.jdbc.Driver";

    /**
     * Get database connection.
     * @return database connection
     * @throws DataAccessUtilException
     */
    public static Connection getConnection() throws DataAccessUtilException {
        try {
            Class.forName(driverName);
            Connection conn = DriverManager.getConnection(url, username, password);
            return conn;
        } catch (SQLException e) {
            throw new DataAccessUtilException("SQL error occurred while getting message count for queue", e);
        } catch (ClassNotFoundException e1) {
            throw new DataAccessUtilException("JDBC driver not found", e1);
        }
    }
}
