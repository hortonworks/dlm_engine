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


/**
 * This interface contains BeaconAutorizer related method signatures.
 */

public interface BeaconAuthorizer {


    /**
     * This method will load the policy file and would initialize the required data-structures.
     */
    void init();

    /**
     * This method is responsible to perform the actual authorization for every REST API call. It will check if
     * user can perform action on resource.
     */
    boolean isAccessAllowed(BeaconAccessRequest request) throws BeaconAuthorizationException;

    /**
     * This method is responsible to perform the cleanup and release activities. It must be called when you are done
     * with the Authorization activity and once it's called a restart would be required. Try to invoke this while
     * destroying the context.
     */
    void cleanUp();
}
