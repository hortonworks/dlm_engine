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

import java.util.Date;
import java.util.List;

/**
 * Beacon scheduler interface.
 */
public interface BeaconScheduler {
    /**
     * start beacon scheduler.
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
     * schedule a job.
     * @param jobs list of jobs to schedule in that order
     * @param recovery request recovery in case of failure situation
     * @param startTime start time for the jobs
     * @param endTime end time for the jobs
     * @param frequency frequency for jobs
     * @throws BeaconException
     */
    String schedulePolicy(List<ReplicationJobDetails> jobs, boolean recovery, String policyId, Date startTime,
                          Date endTime, int frequency) throws BeaconException;

    /**
     * stop running scheduler.
     * @throws BeaconException
     */
    void stopScheduler() throws BeaconException;

    /**
     * Delete a scheduled job.
     * @param id id of the job
     * @return true, if deleted.
     * @throws BeaconException
     */
    boolean deletePolicy(String id) throws BeaconException;

    /**
     * Suspend (pause) a job (policy).
     * @param id name of the job key
     * @throws BeaconException
     */
    void suspendPolicy(String id) throws BeaconException;

    /**
     * Resume a suspended (paused) job (policy).
     * @param id name of the job key
     * @throws BeaconException
     */
    void resumePolicy(String id) throws BeaconException;

    /**
     * Abort running policy instance.
     * @param id name of the job key
     */
    boolean abortInstance(String id) throws BeaconException;

    /**
     * Recovery of policy instance.
     * @param policyId policyId
     * @param offset offset of recovery job
     * @param recoverInstance instance being recovered
     * @return true, if recovery was successful.
     * @throws BeaconException
     */
    boolean recoverPolicyInstance(String policyId, String offset, String recoverInstance) throws BeaconException;

    /**
     * Rerun a policy instance.
     * @param policyId policyId
     * @param offset offset of recovery job
     * @param recoverInstance instance being recovered
     * @return true, if rerun scheduled successfully.
     * @throws BeaconException if any errors.
     */
    boolean rerunPolicyInstance(String policyId, String offset, String recoverInstance) throws BeaconException;
}
