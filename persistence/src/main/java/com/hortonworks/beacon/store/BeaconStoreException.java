/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.store;

import com.hortonworks.beacon.exceptions.BeaconException;

/**
 * Beacon Store exception handling.
 */
public class BeaconStoreException extends BeaconException {

    public BeaconStoreException(String message) {
        super(message);
    }

    public BeaconStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public BeaconStoreException(String message, Throwable cause, Object...objects) {
        super(cause, message, objects);
    }

    public BeaconStoreException(String message, Object...objects) {
        super(message, objects);
    }
}
