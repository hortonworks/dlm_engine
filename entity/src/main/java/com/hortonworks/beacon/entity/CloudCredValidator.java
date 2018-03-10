/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
