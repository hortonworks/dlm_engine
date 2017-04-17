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

package com.hortonworks.beacon.replication.hive;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.InstanceExecutionDetails;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Abstract class for HiveDR Implementation.
 */

public abstract class HiveDRImpl {

    private static final Logger LOG = LoggerFactory.getLogger(HiveDRImpl.class);

    private Properties properties = null;
    private String database;
    private InstanceExecutionDetails instanceExecutionDetails;

    public HiveDRImpl(ReplicationJobDetails details) {
        properties = details.getProperties();
        database = properties.getProperty(HiveDRProperties.SOURCE_DATABASE.getName());
        instanceExecutionDetails = new InstanceExecutionDetails();
    }

    String getDatabase() {
        return database;
    }

    public Properties getProperties() {
        return properties;
    }

    public String getJobExecutionContextDetails() throws BeaconException {
        LOG.info("Job status after replication : {}", getInstanceExecutionDetails().toJsonString());
        return getInstanceExecutionDetails().toJsonString();
    }

    InstanceExecutionDetails getInstanceExecutionDetails() {
        return instanceExecutionDetails;
    }

}
