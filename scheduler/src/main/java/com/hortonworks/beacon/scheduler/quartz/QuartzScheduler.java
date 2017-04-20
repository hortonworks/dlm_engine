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

import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SchedulerListener;
import org.quartz.Trigger;
import org.quartz.TriggerListener;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.EverythingMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * Beacon scheduler's interaction with quartz.
 */
public final class QuartzScheduler {

    private static final String ASSERTION_MSG = "Group should be start node: " + BeaconQuartzScheduler.START_NODE_GROUP;
    private Scheduler scheduler;
    private static final QuartzScheduler INSTANCE = new QuartzScheduler();
    private static final Logger LOG = LoggerFactory.getLogger(QuartzScheduler.class);

    private QuartzScheduler() {
    }

    public static QuartzScheduler get() {
        return INSTANCE;
    }

    void startScheduler(JobListener jListener, TriggerListener tListener, SchedulerListener sListener,
                        Properties properties)
            throws SchedulerException {
        SchedulerFactory factory = new StdSchedulerFactory(properties);
        scheduler = factory.getScheduler();
        scheduler.getListenerManager().addJobListener(jListener, EverythingMatcher.allJobs());
        scheduler.getListenerManager().addTriggerListener(tListener, EverythingMatcher.allTriggers());
        scheduler.getListenerManager().addSchedulerListener(sListener);
        scheduler.start();
    }

    void stopScheduler() throws SchedulerException {
        if (scheduler != null && scheduler.isStarted()) {
            scheduler.shutdown(false);
        }
    }

    void scheduleJob(JobDetail jobDetail, Trigger trigger) throws SchedulerException {
        trigger = trigger.getTriggerBuilder().forJob(jobDetail).build();
        scheduler.scheduleJob(jobDetail, trigger);
        LOG.info("Job [key: {}] and trigger [key: {}] are scheduled.", jobDetail.getKey(), trigger.getJobKey());
    }

    void scheduleChainedJobs(List<JobDetail> jobs, Trigger trigger) throws SchedulerException {
        QuartzJobListener listener = (QuartzJobListener) scheduler.getListenerManager().
                getJobListener(BeaconQuartzScheduler.BEACON_SCHEDULER_JOB_LISTENER);
        for (int i = 1; i < jobs.size(); i++) {
            JobDetail firstJob = jobs.get(i - 1);
            JobDetail secondJob = jobs.get(i);
            listener.addJobChainLink(firstJob.getKey(), secondJob.getKey());
            scheduler.addJob(secondJob, false);
        }
        scheduler.scheduleJob(jobs.get(0), trigger);
        LOG.info("Job [key: {}] and trigger [key: {}] are scheduled.", jobs.get(0).getKey(), trigger.getKey());
    }

    boolean isStarted() throws SchedulerException {
        return scheduler != null && scheduler.isStarted() && !scheduler.isInStandbyMode() && !scheduler.isShutdown();
    }

    boolean deleteJob(String name, String group) throws SchedulerException {
        assert group.equals(BeaconQuartzScheduler.START_NODE_GROUP): ASSERTION_MSG;
        JobKey jobKey = new JobKey(name, group);
        interruptJob(jobKey);
        JobDetail jobDetail = scheduler.getJobDetail(jobKey);
        int numJobs = jobDetail.getJobDataMap().getInt(QuartzDataMapEnum.NO_OF_JOBS.getValue());
        boolean finalResult = true;
        // It should delete all the jobs (given policy id) added to the scheduler.
        for (int i = 0; i<numJobs; i++) {
            JobKey key = new JobKey(name, String.valueOf(i));
            boolean deleteJob = scheduler.deleteJob(key);
            LOG.info("Deleting job [key: {}, result: {}] from the scheduler.", key, deleteJob);
            finalResult = finalResult && deleteJob;
        }
        return finalResult;
    }

    JobDetail getJobDetail(String keyName, String keyGroup) throws SchedulerException {
        return scheduler.getJobDetail(new JobKey(keyName, keyGroup));
    }

    void suspendJob(String name, String group) throws SchedulerException {
        assert group.equals(BeaconQuartzScheduler.START_NODE_GROUP): ASSERTION_MSG;
        JobKey jobKey = new JobKey(name, group);
        JobDetail jobDetail = scheduler.getJobDetail(jobKey);
        if (jobDetail == null) {
            LOG.warn("No scheduled policy found for job key: [{}]", jobKey);
            throw new SchedulerException("No scheduled policy found.");
        }
        // This will suspend the next execution of the scheduled job, no effect on current job.
        scheduler.pauseJob(jobKey);
    }

    void resumeJob(String name, String group) throws SchedulerException {
        assert group.equals(BeaconQuartzScheduler.START_NODE_GROUP): ASSERTION_MSG;
        JobKey jobKey = new JobKey(name, group);
        JobDetail jobDetail = scheduler.getJobDetail(jobKey);
        if (jobDetail == null) {
            LOG.warn("No suspended policy found for job key: [{}]", jobKey);
            throw new SchedulerException("No suspended policy found.");
        }
        scheduler.resumeJob(jobKey);
    }

    // For testing only. To clear the jobs and triggers from Quartz.
    void clear() throws SchedulerException {
        scheduler.clear();
    }

    private void interruptJob(JobKey jobKey) throws SchedulerException {
        List<JobExecutionContext> currentlyExecutingJobs = scheduler.getCurrentlyExecutingJobs();
        for (JobExecutionContext executionContext : currentlyExecutingJobs) {
            JobKey key = executionContext.getJobDetail().getKey();
            // Comparing only name (policy id) as group will be different (offsets).
            if (jobKey.getName().equals(key.getName())) {
                LOG.info("Interrupt Job id: {}, group: {} from the currently running jobs.",
                        key.getName(), key.getGroup());
                scheduler.interrupt(key);
                break;
            }
        }
    }
}
