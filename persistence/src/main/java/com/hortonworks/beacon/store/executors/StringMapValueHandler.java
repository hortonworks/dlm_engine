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

package com.hortonworks.beacon.store.executors;

import com.hortonworks.beacon.constants.BeaconConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.openjpa.jdbc.kernel.JDBCStore;
import org.apache.openjpa.jdbc.meta.ValueMapping;

import java.util.HashMap;
import java.util.Map;

/**
 * StringBlob handler for storing data as bytes.
 */
public class StringMapValueHandler extends org.apache.openjpa.jdbc.meta.strats.ByteArrayValueHandler {

    private static final StringMapValueHandler INSTANCE = new StringMapValueHandler();

    public static StringMapValueHandler getInstance() {
        return INSTANCE;
    }

    public Object toDataStoreValue(ValueMapping vm, Object val, JDBCStore store) {
        StringBuilder builder = new StringBuilder();
        if (val == null) {
            return builder.toString().getBytes();
        }

        Map<String, String> configuration = (Map<String, String>) val;

        for (Map.Entry<String, String> entry : configuration.entrySet()) {
            builder = builder.length() > 0 ? builder.append(BeaconConstants.SEMICOLON_SEPARATOR) : builder;
            builder.append(entry.getKey());
            builder.append(BeaconConstants.EQUAL_SEPARATOR);
            builder.append(entry.getValue());
        }
        return builder.toString().getBytes();
    }

    public Object toObjectValue(ValueMapping vm, Object val) {
        Map<String, String> configuration = new HashMap<>();
        if (val == null) {
            return configuration;
        }

        String conf = new String((byte[]) val);
        if (StringUtils.isNotBlank(conf)) {
            String[] strings = conf.split(BeaconConstants.SEMICOLON_SEPARATOR);
            for (String str : strings) {
                String[] pair = str.split(BeaconConstants.EQUAL_SEPARATOR);
                configuration.put(pair[0], pair[1]);
            }
        }
        return configuration;
    }
}
