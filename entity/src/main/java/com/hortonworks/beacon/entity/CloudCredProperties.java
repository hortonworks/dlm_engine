/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.entity;

import java.util.HashSet;
import java.util.Set;

/**
 * Properties list for cloud cred entity. All the extra properties will be stored as configuration.
 */
public enum CloudCredProperties {

    ID("id", "cloud cred entity id"),
    NAME("name", "cloud cred name"),
    PROVIDER("provider", "cloud provider");

    private final String name;
    private final String description;

    CloudCredProperties(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    private static Set<String> elements = new HashSet<>();
    static {
        for (CloudCredProperties prop : CloudCredProperties.values()) {
            elements.add(prop.getName());
        }
    }

    public static Set<String> getElements() {
        return elements;
    }
}
