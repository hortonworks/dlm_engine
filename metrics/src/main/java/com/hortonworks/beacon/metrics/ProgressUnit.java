/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.metrics;

/**
 * Unit for Replication Progress.
 */
public enum ProgressUnit {
    MAPTASKS("maptasks"),
    TABLE("table"),
    EVENTS("events");

    private String name;

    ProgressUnit(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
