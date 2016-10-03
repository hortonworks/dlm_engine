package com.hortonworks.beacon.exceptions;

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