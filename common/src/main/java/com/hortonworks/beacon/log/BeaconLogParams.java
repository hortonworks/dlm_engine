/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.log;

import java.util.ArrayList;
import java.util.List;

/**
 * Supported Parameter for Beacon Log.
 */
public enum BeaconLogParams {
    POLICYNAME("POLICYNAME"),
    POLICYID("POLICYID"),
    INSTANCEID("INSTANCEID");

    private String name;

    private static final List<String> BEACON_LOG_PARAMS_LIST = new ArrayList<>();

    static {
        for (BeaconLogParams params : BeaconLogParams.values()) {
            BEACON_LOG_PARAMS_LIST.add(params.name());
        }
    }

    BeaconLogParams(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static boolean checkParams(String name) {
        return BEACON_LOG_PARAMS_LIST.contains(name);
    }

    public static int size() {
        return BEACON_LOG_PARAMS_LIST.size();
    }

    public static String getParam(int i) {
        return BEACON_LOG_PARAMS_LIST.get(i);
    }

    public static void clearParams() {
        BEACON_LOG_PARAMS_LIST.clear();
    }
}
