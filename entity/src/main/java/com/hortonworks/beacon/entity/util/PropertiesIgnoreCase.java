package com.hortonworks.beacon.entity.util;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

public final class PropertiesIgnoreCase extends Properties {
    public String getPropertyIgnoreCase(String key) {
        String value = getProperty(key);
        if (value != null) {
            return value;
        }

        // Not matching with the actual key then
        Set<Entry<Object, Object>> s = entrySet();
        Iterator<Entry<Object, Object>> it = s.iterator();
        while (it.hasNext()) {
            Entry<Object, Object> entry = it.next();
            if (key.equalsIgnoreCase((String) entry.getKey())) {
                return (String) entry.getValue();
            }
        }
        return null;
    }
}
