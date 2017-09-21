/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.notification;

import com.hortonworks.beacon.constants.BeaconConstants;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to batch error messages together.
 */
public class BeaconNotification {

    private List<String> errors = new ArrayList<>();

    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    public void addError(String message) {
        errors.add(message);
    }

    public String errorMessage(String delimiter) {
        return StringUtils.join(errors, delimiter);
    }

    public String errorMessage() {
        return errorMessage(BeaconConstants.NEW_LINE);
    }
}
