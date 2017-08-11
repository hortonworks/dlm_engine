/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.service;

import com.hortonworks.beacon.exceptions.BeaconException;

/**
 * Beacon service abstraction.
 */
public interface BeaconService {

    String getName();

    void init() throws BeaconException;

    void destroy() throws BeaconException;
}

