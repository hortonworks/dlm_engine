/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.client.util;

import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.CloudCred.Config;
import com.hortonworks.beacon.client.CloudCredProperties;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.PropertiesIgnoreCase;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * CloudCred builder from request stream.
 */
public final class CloudCredBuilder {

    private CloudCredBuilder() {
    }

    public static CloudCred buildCloudCred(PropertiesIgnoreCase properties) throws BeaconException {

        String id = properties.getPropertyIgnoreCase(CloudCredProperties.ID.getName());
        if (StringUtils.isBlank(id)) {
            id = String.valueOf(UUID.randomUUID());
            properties.setProperty(CloudCredProperties.ID.getName(), id);
        }

        String name = properties.getPropertyIgnoreCase(CloudCredProperties.NAME.getName());
        String providerStr = properties.getPropertyIgnoreCase(CloudCredProperties.PROVIDER.getName());
        CloudCred.Provider provider = null;
        if (StringUtils.isNotBlank(providerStr)) {
            provider = CloudCred.Provider.valueOf(providerStr);
        }

        String authTypeStr = properties.getPropertyIgnoreCase(CloudCredProperties.AUTHTYPE.getName());
        CloudCred.AuthType authType = null;
        if (StringUtils.isNotBlank(authTypeStr)) {
            authType = CloudCred.AuthType.valueOf(authTypeStr);
        }

        Properties customProperties = EntityHelper.getCustomProperties(properties, CloudCredProperties.getElements());
        Map<Config, String> configs = new HashMap<>();

        for (Map.Entry<Object, Object> entry : customProperties.entrySet()) {
            configs.put(Config.forValue((String) entry.getKey()), (String) entry.getValue());
        }

        CloudCred cloudCred = new CloudCred();
        cloudCred.setId(id);
        cloudCred.setName(name);
        cloudCred.setProvider(provider);
        cloudCred.setAuthType(authType);
        cloudCred.setConfigs(configs);
        return cloudCred;
    }
}
