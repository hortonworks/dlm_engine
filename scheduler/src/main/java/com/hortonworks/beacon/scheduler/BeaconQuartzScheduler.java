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
import com.hortonworks.beacon.scheduler.quartz.QuartzJobDetailFactory;
import com.hortonworks.beacon.scheduler.quartz.QuartzJobListener;
import com.hortonworks.beacon.scheduler.quartz.QuartzScheduler;
import com.hortonworks.beacon.scheduler.quartz.QuartzSchedulerListener;
import com.hortonworks.beacon.scheduler.quartz.QuartzTriggerFactory;
import com.hortonworks.beacon.scheduler.quartz.QuartzTriggerListener;
import org.quartz.JobDetail;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BeaconQuartzScheduler implements BeaconScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconQuartzScheduler.class);
    private QuartzScheduler scheduler;
    private QuartzJobDetailFactory jobDetailFactory;
    private QuartzTriggerFactory triggerFactory;

    public BeaconQuartzScheduler() {
        scheduler = new QuartzScheduler();
        jobDetailFactory = new QuartzJobDetailFactory();
        triggerFactory = new QuartzTriggerFactory();
    }

    @Override
    public void startScheduler() throws BeaconException {
        try {
            if (!isStarted()) {
                scheduler.startScheduler(new QuartzJobListener("quartzJobListener"),
                        new QuartzTriggerListener("quartzTriggerListener"),
                        new QuartzSchedulerListener());
                LOG.info("Scheduler started successfully.");
            }
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    @Override
    public String scheduleJob(ReplicationJobDetails job, boolean recovery) throws BeaconException {
        JobDetail jobDetail = jobDetailFactory.createJobDetail(job, recovery);
        Trigger trigger = triggerFactory.createTrigger(job);
        try {
            scheduler.scheduleJob(jobDetail, trigger);
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
        return jobDetail.getKey().getName();
    }

    // TODO Currently using first job for creating trigger
    @Override
    public void scheduleChainedJobs(List<ReplicationJobDetails> jobs, boolean recovery) throws BeaconException {
        List<JobDetail> jobDetails = jobDetailFactory.createJobDetailList(jobs, recovery);
        Trigger trigger = triggerFactory.createTrigger(jobs.get(0));
        try {
            scheduler.scheduleChainedJobs(jobDetails, trigger);
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    @Override
    public void stopScheduler() throws BeaconException {
        try {
            if (isStarted()) {
                scheduler.stopScheduler();
                LOG.info("Scheduler shutdown successfully.");
            } else {
                LOG.info("Scheduler is not running.");
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
    public boolean deleteJob(String name, String group) throws BeaconException {
        try {
            return scheduler.deleteJob(name, group);
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    @Override
    public void listJob(String name, String group) throws BeaconException {
        try {
            scheduler.listJob(name, group);
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    @Override
    public String addJob(ReplicationJobDetails job, boolean recovery) throws BeaconException {
        JobDetail jobDetail = jobDetailFactory.createJobDetail(job, recovery);
        try {
            scheduler.addJob(jobDetail, true);
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
        return jobDetail.getKey().getName();
    }

    @Override
    public void scheduleJob(String name, String group) throws BeaconException {
        try {
            scheduler.scheduleJob(name, group);
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }
}
