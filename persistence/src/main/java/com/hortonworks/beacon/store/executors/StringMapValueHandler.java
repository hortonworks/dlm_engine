/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
