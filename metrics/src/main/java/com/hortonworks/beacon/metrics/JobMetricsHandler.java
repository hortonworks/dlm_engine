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

package com.hortonworks.beacon.metrics;

import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.rb.ResourceBundleService;
import com.hortonworks.beacon.util.ReplicationType;

/**
 * Job Metrics handler to initialize the required concrete class for obtaining
 * job counters.
 */
public final class JobMetricsHandler {
    private JobMetricsHandler() {
    }

    public static JobMetrics getMetricsType(ReplicationType replType) {
        JobMetrics jobMetrics;
        switch (replType) {
            case FS:
                jobMetrics = new FSReplicationMetrics();
                break;
            case HIVE:
                jobMetrics = new HiveDRMetrics();
                break;
            default:
                throw new IllegalArgumentException(ResourceBundleService.getService()
                        .getString(MessageCode.COMM_010009.name(), "Replication", replType.toString()));
        }
        return jobMetrics;
    }
}
