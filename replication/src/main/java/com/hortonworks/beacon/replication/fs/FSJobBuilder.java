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

package com.hortonworks.beacon.replication.fs;

import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.JobBuilder;
import com.hortonworks.beacon.replication.ReplicationJobDetails;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * FileSystem Replication JobBuilder.
 */

public class FSJobBuilder extends JobBuilder {

    public List<ReplicationJobDetails> buildJob(ReplicationPolicy policy) throws BeaconException {
        // Add any default plugin relates jobs first


        Properties fsDRProperties = FSPolicyHelper.buildFSReplicationProperties(policy);
        FSPolicyHelper.validateFSReplicationProperties(fsDRProperties);

        String name = fsDRProperties.getProperty(ReplicationPolicy.ReplicationPolicyFields.NAME.getName());
        String type = fsDRProperties.getProperty(ReplicationPolicy.ReplicationPolicyFields.TYPE.getName());
        return Arrays.asList(new ReplicationJobDetails(type, name, type, fsDRProperties));
    }
}
