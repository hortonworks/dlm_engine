/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.util;

/**
 * Various replication policy types supported by beacon.
 */
public enum ReplicationType {
    HIVE("hive"),
    TEST("test"),
    FS("fs"),
    PLUGIN("plugin"),
    START("start"),
    END("end");

    private final String name;

    ReplicationType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
