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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.util.StringFormat;

/**
 * Exception for Authentication filter(s).
 */
public class BeaconAuthException extends IOException {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconAuthException.class);

    public static BeaconAuthException newAPIException(String message, Object... objects) {
        BeaconAuthException bwe = new BeaconAuthException();
        LOG.error("Throwing web exception: {}", StringFormat.format(message, objects));
        return bwe;
    }
}
