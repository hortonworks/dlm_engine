/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.store.bean;

import java.io.Serializable;

/**
 * Composite primary key for Cluster entity.
 */
public class ClusterKey implements Serializable {

    private String name;
    private int version;

    public String getName() {
        return name;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClusterKey that = (ClusterKey) o;

        if (version != that.version) {
            return false;
        }

        return name != null ? name.equals(that.name) : that.name == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + version;
        return result;
    }

    public ClusterKey() {
    }

    public ClusterKey(String name, int version) {
        this.name = name;
        this.version = version;
    }
}
