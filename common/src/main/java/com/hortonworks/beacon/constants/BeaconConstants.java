/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.constants;

/**
 * Beacon constants.
 */
public final class BeaconConstants {

    private BeaconConstants() {
        // Disable construction.
    }

    /**
     * Constant for the configuration property that indicates the Name node principal.
     */
    public static final String NN_PRINCIPAL = "dfs.namenode.kerberos.principal";

    public static final int MAX_YEAR = 9999;
    public static final long DAY_IN_MS = 24 * 60 * 60 * 1000;
    public static final int MAX_DAY = 31;
    public static final long SERVER_START_TIME = System.currentTimeMillis();
    public static final String COLON_SEPARATOR = ":";
    public static final String COMMA_SEPARATOR = ",";
    public static final String CLUSTER_NAME_SEPARATOR_REGEX = "\\$";
    public static final String VALIDATION_QUERY = "select count(*) from beacon_sys";
}
