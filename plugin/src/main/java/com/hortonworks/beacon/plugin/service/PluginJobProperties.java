/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
