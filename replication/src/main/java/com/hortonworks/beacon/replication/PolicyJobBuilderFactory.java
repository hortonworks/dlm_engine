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

package com.hortonworks.beacon.replication;

import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.replication.fs.FSJobBuilder;
import com.hortonworks.beacon.replication.hive.HiveJobBuilder;
import com.hortonworks.beacon.util.ReplicationHelper;
import com.hortonworks.beacon.util.ReplicationType;

/**
 * Class to provide the Replication JobBuilder class.
 */
public final class PolicyJobBuilderFactory {

    private PolicyJobBuilderFactory() {}

    public static JobBuilder getJobBuilder(ReplicationPolicy policy) {
        ReplicationType replType = ReplicationHelper.getReplicationType(policy.getType());
        switch (replType) {
            case FS:
                return new FSJobBuilder();
            case HIVE:
                return new HiveJobBuilder();
            default:
                throw new IllegalArgumentException(
                    ResourceBundleService.getService().getString(MessageCode.COMM_000012.name(), policy.getType()));
        }
    }
}
