package com.hortonworks.beacon.exceptions;

/**
 * Exception to thrown when entity being sought for addition is already present in config store.
 */
public class EntityAlreadyExistsException extends BeaconException {

    public EntityAlreadyExistsException(Exception e) {
        super(e);
    }

    public EntityAlreadyExistsException(String message, Exception e) {
        super(message, e);
    }

    public EntityAlreadyExistsException(String message) {
        super(message);
    }
}
