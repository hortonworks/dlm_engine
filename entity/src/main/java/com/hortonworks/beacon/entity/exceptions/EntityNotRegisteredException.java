package com.hortonworks.beacon.entity.exceptions;


import com.hortonworks.beacon.exceptions.BeaconException;

/**
 * Exception thrown by beacon when entity is not registered already in config store.
 */
public class EntityNotRegisteredException extends BeaconException {

    public EntityNotRegisteredException(String message) {
        super(message);
    }
}
