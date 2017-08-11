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
 * Hive Action Types.
 */
public enum HiveActionType {
    EXPORT("export"),
    IMPORT("import");

    private String type;

    HiveActionType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
