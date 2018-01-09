/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.scheduler.internal;

import java.io.Serializable;

import com.hortonworks.beacon.exceptions.BeaconException;

/**
 * Beacon admin job work interface.
 */
public interface AdminJob extends Serializable {

    String ADMIN_JOBS = "ADMIN_POLICY";
    String POLICY_STATUS = "ADMIN_POLICY_STATUS";
    String POLICY_DELETE = "ADMIN_POLICY_DELETE";

    void perform() throws BeaconException;
    String getName();
    String getGroup();
}
