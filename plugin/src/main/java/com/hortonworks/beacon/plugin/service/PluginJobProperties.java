/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.plugin.service;

/**
 * Plugin job properties.
 */
public enum PluginJobProperties {
    JOB_NAME("name", "Name of the job"),
    JOB_TYPE("type", "Type of plugin job"),
    JOBACTION_TYPE("actionType", "Type of job action"),
    SOURCE_CLUSTER("sourceCluster", "Source cluster name"),
    TARGET_CLUSTER("targetCluster", "Target cluster name"),
    DATASET("dataset", "Dataset to be worked on"),
    DATASET_TYPE("datasetType", "Type of dataset");

    private final String name;
    private final String description;
    private final boolean isRequired;

    PluginJobProperties(String name, String description) {
        this(name, description, true);
    }

    PluginJobProperties(String name, String description, boolean isRequired) {
        this.name = name;
        this.description = description;
        this.isRequired = isRequired;
    }

    public String getName() {
        return this.name;
    }

    public String getDescription() {
        return description;
    }

    public boolean isRequired() {
        return isRequired;
    }

    @Override
    public String toString() {
        return getName();
    }
}
