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

import com.hortonworks.beacon.scheduler.hdfs.HDFSDRImpl;
import com.hortonworks.beacon.scheduler.hdfssnapshot.HDFSSnapshotDRImpl;
import com.hortonworks.beacon.scheduler.hive.HiveDRImpl;

public class ReplicationImplFactory {

    private ReplicationImplFactory() {
    }

    public static DRReplication getReplicationImpl(ReplicationJobDetails details) {
        if ((details.getType()).equals(ReplicationType.HIVE.getName())) {
            return new HiveDRImpl(details);
        } else if ((details.getType()).equals(ReplicationType.HDFS.getName())) {
            return new HDFSDRImpl(details);
        } else if ((details.getType()).equals(ReplicationType.HDFSSNAPSHOT.getName())) {
            return new HDFSSnapshotDRImpl(details);
        } else {
            return null;
        }
    }
}
