/**
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *    OR LOSS OR CORRUPTION OF DATA.
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
