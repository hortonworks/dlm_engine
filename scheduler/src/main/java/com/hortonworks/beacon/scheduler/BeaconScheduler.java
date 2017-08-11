/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
