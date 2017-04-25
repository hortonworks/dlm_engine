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

package com.hortonworks.beacon.scheduler.quartz;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.nodes.NodeGenerator;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.scheduler.BeaconScheduler;
import org.quartz.JobDetail;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * BeaconScheduler API implementation for Quartz.
 */
public final class BeaconQuartzScheduler implements BeaconScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconQuartzScheduler.class);

    static final String START_NODE_GROUP = "0";
    static final String BEACON_SCHEDULER_JOB_LISTENER = "beaconSchedulerJobListener";
    private static final String BEACON_SCHEDULER_TRIGGER_LISTENER = "beaconSchedulerTriggerListener";

    private QuartzScheduler scheduler;

    private static final BeaconQuartzScheduler INSTANCE = new BeaconQuartzScheduler();

    private BeaconQuartzScheduler() {
        scheduler = QuartzScheduler.get();
    }

    public static BeaconQuartzScheduler get() {
        return INSTANCE;
    }

    @Override
    public void startScheduler(Properties properties) throws BeaconException {
        try {
            if (!isStarted()) {
                scheduler.startScheduler(new QuartzJobListener(BEACON_SCHEDULER_JOB_LISTENER),
                        new QuartzTriggerListener(BEACON_SCHEDULER_TRIGGER_LISTENER),
                        new QuartzSchedulerListener(), properties);
                LOG.info("Beacon scheduler started successfully.");
            } else {
                LOG.info("Instance of the Beacon scheduler is already running.");
            }
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    // using first job for creating trigger.
    @Override
    public String schedulePolicy(List<ReplicationJobDetails> jobs, boolean recovery, String policyId, Date startTime,
                                 Date endTime, int frequency) throws BeaconException {
        jobs = NodeGenerator.appendNodes(jobs);
        List<JobDetail> jobDetails = QuartzJobDetailBuilder.createJobDetailList(jobs, recovery, policyId);
        Trigger trigger = QuartzTriggerBuilder.createTrigger(policyId, START_NODE_GROUP, startTime, endTime,
                frequency);
        try {
            scheduler.scheduleChainedJobs(jobDetails, trigger);
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
        return policyId;
    }

    @Override
    public void stopScheduler() throws BeaconException {
        try {
            if (isStarted()) {
                scheduler.stopScheduler();
                LOG.info("Beacon scheduler shutdown successfully.");
            } else {
                LOG.info("Beacon scheduler is not running.");
            }
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    @Override
    public boolean isStarted() throws BeaconException {
        try {
            return scheduler.isStarted();
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    @Override
    public boolean deletePolicy(String id) throws BeaconException {
        LOG.info("Deleting the scheduled replication entity with id : {}", id);
        try {
            return scheduler.deleteJob(id, START_NODE_GROUP);
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    @Override
    public void suspendPolicy(String id) throws BeaconException {
        try {
            scheduler.suspendJob(id, START_NODE_GROUP);
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    @Override
    public void resumePolicy(String id) throws BeaconException {
        try {
            scheduler.resumeJob(id, START_NODE_GROUP);
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    @Override
    public boolean abortInstance(String id) throws BeaconException {
        try {
            return scheduler.interrupt(id, START_NODE_GROUP);
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    // For testing only.
    void clear() throws BeaconException {
        try {
            scheduler.clear();
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }
}
