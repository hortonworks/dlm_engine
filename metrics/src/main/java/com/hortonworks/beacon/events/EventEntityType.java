/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */


package com.hortonworks.beacon.events;

/**
 * Define Entity Type for events generated.
 */
public enum EventEntityType {
    SYSTEM("system"),
    CLUSTER("cluster"),
    POLICY("policy"),
    POLICYINSTANCE("policyinstance");

    private final String name;

    EventEntityType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
