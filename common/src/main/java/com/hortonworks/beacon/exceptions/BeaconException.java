/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.exceptions;

import com.hortonworks.beacon.rb.ResourceBundleService;

/**
 * Common Exception thrown.
 */
public class BeaconException extends Exception {

    /**
     * @param e Exception
     */
    public BeaconException(Throwable e) {
        super(e);
    }

    public BeaconException(String message, Throwable e, Object...objects) {
        super(ResourceBundleService.getService().getString(message, objects), e);
    }

    /**
     * @param message - custom exception message
     */
    public BeaconException(String message, Object...objects) {
        super(ResourceBundleService.getService().getString(message, objects));
    }

    /**
     *
     */
    private static final long serialVersionUID = -1475818869309247014L;

}
