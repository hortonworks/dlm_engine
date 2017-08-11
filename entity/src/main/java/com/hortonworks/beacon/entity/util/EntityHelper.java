/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.entity.util;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Helper util class for Beacon resources.
 */
public final class EntityHelper {

    private EntityHelper() {
    }

    public static Properties getCustomProperties(final Properties properties, final Set<String> entityElements) {
        Properties customProperties = new Properties();
        for (Map.Entry<Object, Object> property : properties.entrySet()) {
            if (!entityElements.contains(property.getKey().toString().toLowerCase())) {
                customProperties.put(property.getKey(), property.getValue());
            }
        }
        return customProperties;
    }
}
