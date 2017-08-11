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

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

/**
 * Class to implement java util properties IgnoreCase.
 */
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
