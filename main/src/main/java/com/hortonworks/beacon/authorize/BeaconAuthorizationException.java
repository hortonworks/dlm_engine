/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.authorize;

import com.hortonworks.beacon.util.StringFormat;

/**
 * This class extends Exception class and shall be used for authorization module exception.
 */

public class BeaconAuthorizationException extends Exception {
    private static final long serialVersionUID = 1L;

    public BeaconAuthorizationException(String message) {
        super(message);
    }

    public BeaconAuthorizationException(String message, Throwable exception) {
        super(message, exception);
    }

    public BeaconAuthorizationException(String message, Throwable exception, boolean enableSuppression,
        boolean writableStackTrace) {
        super(message, exception, enableSuppression, writableStackTrace);
    }

    public BeaconAuthorizationException(BeaconAccessRequest request) {
        super("Unauthorized Request : " + request);
    }

    public BeaconAuthorizationException(String message, Object... objects) {
        this(StringFormat.format(message, objects));
    }

    public BeaconAuthorizationException(String message, Throwable exception, Object... objects) {
        this(StringFormat.format(message, objects), exception);
    }

    public BeaconAuthorizationException(String message, Throwable exception, boolean enableSuppression,
        boolean writableStackTrace, Object... objects) {
        this(StringFormat.format(message, objects), exception, enableSuppression,
            writableStackTrace);
    }
}
