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

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.ReplicationJobDetails;

import java.util.List;

/**
 * Beacon scheduler interface.
 */
public interface BeaconScheduler {
    /**
     * start beacon scheduler
     * @throws BeaconException
     */
    void startScheduler() throws BeaconException;

    /**
     * check if scheduler is already started.
     * @return true if scheduler is already started, otherwise false
     * @throws BeaconException
     */
    boolean isStarted() throws BeaconException;

    /**
     * schedule a job
     * @param job job to schedule
     * @param recovery enable/disable recovery
     * @throws BeaconException
     */
    void scheduleJob(ReplicationJobDetails job, boolean recovery) throws BeaconException;

    /**
     * schedule chained jobs
     * @throws BeaconException
     */
    void scheduleChainedJobs(List<ReplicationJobDetails> jobs, boolean recovery) throws BeaconException;

    /**
     * stop running scheduler
     * @throws BeaconException
     */
    void stopScheduler() throws BeaconException;
}
