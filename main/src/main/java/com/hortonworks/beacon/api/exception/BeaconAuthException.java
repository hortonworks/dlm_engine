/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.api.exception;

import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;

import java.io.IOException;

/**
 * Exception for Authentication filter(s).
 */
public class BeaconAuthException extends IOException {

    private static final BeaconLog LOG = BeaconLog.getLog(BeaconAuthException.class);

    public static BeaconAuthException newAPIException(String message, Object... objects) {
        BeaconAuthException bwe = new BeaconAuthException();
        LOG.error(MessageCode.MAIN_000075.name(), ResourceBundleService.getService().getString(message, objects));
        return bwe;
    }
}
