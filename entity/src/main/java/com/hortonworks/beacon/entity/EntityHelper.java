package com.hortonworks.beacon.entity;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Created by sramesh on 9/29/16.
 */
public class EntityHelper {
    public static Properties getCustomProperties(final Properties properties, final Set<String> entityElements) {
        Properties customProperties = new Properties();
        for (Map.Entry<Object, Object> property : properties.entrySet()) {
            if (!entityElements.contains(property.getKey().toString())) {
                customProperties.put(property.getKey(), property.getValue());
            }

        }
        return customProperties;
    }
}
