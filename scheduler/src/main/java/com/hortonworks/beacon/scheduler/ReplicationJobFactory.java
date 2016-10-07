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

package com.hortonworks.beacon.scheduler;

import com.hortonworks.beacon.scheduler.hdfs.HDFSReplicationJobDetails;
import com.hortonworks.beacon.scheduler.hdfssnapshot.HDFSSnapshotReplicationJobDetails;
import com.hortonworks.beacon.scheduler.hive.HiveReplicationJobDetails;

public final class ReplicationJobFactory {
    private ReplicationJobFactory() {
    }

    public static ReplicationJobDetails getReplicationType(String replType) {
        if (replType.equals(ReplicationType.HIVE.getName())) {
            return new HiveReplicationJobDetails();
        } else if (replType.equals(ReplicationType.HDFS.getName())) {
            return new HDFSReplicationJobDetails();
        } else if (replType.equals(ReplicationType.HDFSSNAPSHOT.getName())) {
            return new HDFSSnapshotReplicationJobDetails();
        } else {
            throw new IllegalArgumentException("Invalid replication type: " + replType);
        }
    }
}
