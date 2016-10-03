package com.hortonworks.beacon.exceptions;

/**
 * Common Exception thrown
 */
public class BeaconException extends Exception {

    /**
     * @param e Exception
     */
    public BeaconException(Throwable e) {
        super(e);
    }

    public BeaconException(String message, Throwable e) {
        super(message, e);
    }

    /**
     * @param message - custom exception message
     */
    public BeaconException(String message) {
        super(message);
    }

    /**
     *
     */
    private static final long serialVersionUID = -1475818869309247014L;

}
