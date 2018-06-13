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
