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

package com.hortonworks.beacon.util;

import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;

/**
 * Replication utility classes.
 */
public final class ReplicationHelper {
    private static final BeaconLog LOG = BeaconLog.getLog(ReplicationHelper.class);

    private ReplicationHelper() {
    }

    public static ReplicationType getReplicationType(String type) {
        try {
            return ReplicationType.valueOf(type.toUpperCase());
        } catch (IllegalArgumentException ex) {
            LOG.error("{} is not valid replication type", type);
            throw new IllegalArgumentException(
                    ResourceBundleService.getService().getString(MessageCode.COMM_000014.name()));
        }
    }

}
