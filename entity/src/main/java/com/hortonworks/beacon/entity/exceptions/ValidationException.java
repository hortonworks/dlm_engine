/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.entity.exceptions;

import com.hortonworks.beacon.exceptions.BeaconException;

/**
 * Exception thrown when validation fails.
 */
public class ValidationException extends BeaconException {

    public ValidationException(String message) {
        super(message);
    }

    public ValidationException(Exception e) {
        super(e);
    }

    public ValidationException(String message, Exception e) {
        super(message, e);
    }

    public ValidationException(String message, Exception e, Object... objects) {
        super(message, e, objects);
    }

    public ValidationException(String message, Object... objects) {
        super(message, objects);
    }

}
