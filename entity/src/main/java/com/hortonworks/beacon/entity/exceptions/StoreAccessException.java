package com.hortonworks.beacon.entity.exceptions;

import com.hortonworks.beacon.exceptions.BeaconException;

/**
 * Exception when there in issue accessing the persistent store.
 */
public class StoreAccessException extends BeaconException {

    /**
     * @param e Exception
     */
    public StoreAccessException(String message, Exception e) {
        super(message, e);
    }

    public StoreAccessException(Exception e) {
        super(e);
    }
}