/**
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *    OR LOSS OR CORRUPTION OF DATA.
 */
package org.apache.hive.jdbc;

import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.hive.jdbc.Utils.URL_PREFIX;

/**
 * Beacon Hive util to access protected methods.
 */
public final class BeaconHiveUtil {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconHiveUtil.class);

    private BeaconHiveUtil() {
    }

    private static final List<String> GET_BLACKLIST_PARAMS = Arrays.asList("serviceDiscoveryMode",
            "zooKeeperNamespace");

    public static List<String> getAllUrls(String zookeeperBasedHS2Url) throws BeaconException {
        List<String> jdbcConnectionUrlList = new ArrayList<>();
        List<Utils.JdbcConnectionParams> jdbcConnectionParams;
        try {
            jdbcConnectionParams = HiveConnection.getAllUrls(zookeeperBasedHS2Url);
            for (Utils.JdbcConnectionParams jdbcConnectionParam: jdbcConnectionParams) {
                StringBuilder connectionString = new StringBuilder();
                connectionString.append(URL_PREFIX)
                        .append(jdbcConnectionParam.getHost())
                        .append(BeaconConstants.COLON_SEPARATOR)
                        .append(jdbcConnectionParam.getPort())
                        .append("/");
                setParamsFromSessionVars(connectionString, jdbcConnectionParam.getSessionVars());

                jdbcConnectionUrlList.add(connectionString.toString());
                LOG.debug("Connection string: {}", connectionString);
            }
        } catch (Exception e) {
            throw new BeaconException(e);
        }
        return jdbcConnectionUrlList;
    }

    private static void setParamsFromSessionVars(StringBuilder connectionString, Map<String, String> sessionVars) {
        for (Map.Entry<String, String> entry: sessionVars.entrySet()) {
            if (!GET_BLACKLIST_PARAMS.contains(entry.getKey())) {
                connectionString.append(BeaconConstants.SEMICOLON_SEPARATOR)
                        .append(entry.getKey())
                        .append(BeaconConstants.EQUAL_SEPARATOR)
                        .append(entry.getValue());
            }
        }
    }
}
