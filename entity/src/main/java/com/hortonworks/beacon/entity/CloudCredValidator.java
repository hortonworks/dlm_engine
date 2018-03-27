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

package com.hortonworks.beacon.entity;

import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.CloudCred.Config;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.notification.BeaconNotification;
import com.hortonworks.beacon.util.StringFormat;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * Validator for cloud cred entity.
 */
public class CloudCredValidator extends EntityValidator<CloudCred> {

    private final BeaconNotification notification;

    public CloudCredValidator() {
        super(EntityType.CLOUDCRED);
        notification = new BeaconNotification();
    }

    @Override
    public void validate(CloudCred cloudCred) throws BeaconException {
        if (StringUtils.isBlank(cloudCred.getId())) {
            notification.addError("Cloud cred id is empty.");
        }

        if (StringUtils.isBlank(cloudCred.getName())) {
            notification.addError("Cloud cred name is empty.");
        }

        if (cloudCred.getProvider() == null || StringUtils.isBlank(cloudCred.getProvider().name())) {
            notification.addError("Cloud cred provider is empty.");
        }

        if (cloudCred.getAuthType() == null || StringUtils.isBlank(cloudCred.getAuthType().name())) {
            notification.addError("Cloud cred auth type is empty.");
        }

        validateConfigs(cloudCred);
        if (notification.hasErrors()) {
            throw new ValidationException(notification.errorMessage());
        }
    }

    private void validateConfigs(CloudCred cloudCred) {
        //Validate that the required configs are present for the auth type
        List<Config> requiredConfigs = cloudCred.getAuthType().getRequiredConfigs();
        Map<Config, String> configs = cloudCred.getConfigs();
        for (CloudCred.Config config : requiredConfigs) {
            if (!configs.containsKey(config) || configs.get(config) == null) {
                notification.addError(StringFormat.format("Missing parameter: {}", config.getName()));
            }
        }

        //Validate that the credential doesn't contain any password configs that are not required
        for (Config config : cloudCred.getConfigs().keySet()) {
            if (config.isPassword() && !requiredConfigs.contains(config)) {
                notification.addError(StringFormat.format(
                        "Contains password field '{}' which is not required", config.getName()));
            }
        }
    }
}
