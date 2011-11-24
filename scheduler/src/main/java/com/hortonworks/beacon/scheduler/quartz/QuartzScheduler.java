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

package com.hortonworks.beacon.scheduler.quartz;

import com.hortonworks.beacon.scheduler.InstanceSchedulerDetail;
import com.hortonworks.beacon.scheduler.SchedulerCache;
import com.hortonworks.beacon.scheduler.internal.AdminJob;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.TriggerListener;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.matchers.NotMatcher;
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

    void initializeScheduler(JobListener jListener, TriggerListener tListener, Properties properties)
            throws SchedulerException {
        SchedulerFactory factory = new StdSchedulerFactory(properties);
        scheduler = factory.getScheduler();
        scheduler.getListenerManager().addJobListener(jListener,
                NotMatcher.not(GroupMatcher.<JobKey>jobGroupStartsWith(AdminJob.ADMIN_JOBS)));
        scheduler.getListenerManager().addTriggerListener(tListener,
                NotMatcher.not(GroupMatcher.<TriggerKey>groupStartsWith(AdminJob.ADMIN_JOBS)));
    }

    void startScheduler() throws SchedulerException {
        if (scheduler != null && !scheduler.isShutdown() && scheduler.isInStandbyMode()) {
            scheduler.start();
        }
    }

    void stopScheduler() throws SchedulerException {
        if (scheduler != null && scheduler.isStarted()) {
            scheduler.shutdown(false);
        }
    }

    public void scheduleJob(JobDetail jobDetail, Trigger trigger) throws SchedulerException {
        LOG.info("Job [key: {}] and trigger [key: {}] are being scheduled", jobDetail.getKey(), trigger.getJobKey());
        scheduler.scheduleJob(jobDetail, trigger);
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
        LOG.info("Job [key: {}] and trigger [key: {}] are being scheduled", jobs.get(0).getKey(), trigger.getKey());
        scheduler.scheduleJob(jobs.get(0), trigger);
    }

    boolean isStarted() throws SchedulerException {
        return scheduler != null && scheduler.isStarted() && !scheduler.isInStandbyMode() && !scheduler.isShutdown();
    }

    public boolean deleteJob(String name, String group) throws SchedulerException {
        JobKey jobKey = new JobKey(name, group);
        interruptJob(name, group);
        // This is for replication jobs.
        if (group.equals(BeaconQuartzScheduler.START_NODE_GROUP)) {
            JobDetail jobDetail = scheduler.getJobDetail(jobKey);
            boolean finalResult = true;
            if (jobDetail == null) {
                LOG.warn("Could not find job [{}] in the scheduler.", jobKey);
                return finalResult;
            }
            int numJobs = jobDetail.getJobDataMap().getInt(QuartzDataMapEnum.NO_OF_JOBS.getValue());
            // It should delete all the jobs (given policy id) added to the scheduler.
            for (int i = 0; i < numJobs; i++) {
                JobKey key = new JobKey(name, String.valueOf(i));
                boolean deleteJob = scheduler.deleteJob(key);
                LOG.info("Deleting job [key: {}, result: {}] from the scheduler.", key, deleteJob);
                finalResult = finalResult && deleteJob;
            }
            return finalResult;
        } else {
            LOG.info("Deleting job {}", jobKey);
            return scheduler.deleteJob(jobKey);
        }
    }

    JobDetail getJobDetail(String keyName, String keyGroup) throws SchedulerException {
        return scheduler.getJobDetail(new JobKey(keyName, keyGroup));
    }

    void suspendJob(String name, String group) throws SchedulerException {
        JobKey jobKey = new JobKey(name, group);
        JobDetail jobDetail = scheduler.getJobDetail(jobKey);
        if (jobDetail == null) {
            LOG.warn("Could not find job [{}] in the scheduler.", jobKey);
            throw new SchedulerException("No scheduled policy found.");
        }
        // This will suspend the next execution of the scheduled job, no effect on current job.
        LOG.info("Pausing job {}", jobKey);
        scheduler.pauseJob(jobKey);
    }

    void resumeJob(String name, String group) throws SchedulerException {
        JobKey jobKey = new JobKey(name, group);
        JobDetail jobDetail = scheduler.getJobDetail(jobKey);
        if (jobDetail == null) {
            LOG.warn("Could not find job [{}] in the scheduler.", jobKey);
            throw new SchedulerException("No suspended policy found");
        }
        LOG.info("Resuming job {}", jobKey);
        scheduler.resumeJob(jobKey);
    }

    // For testing only. To clear the jobs and triggers from Quartz.
    void clear() throws SchedulerException {
        scheduler.clear();
    }

    private boolean interruptJob(String name, String group) throws SchedulerException {
        List<JobExecutionContext> currentlyExecutingJobs = scheduler.getCurrentlyExecutingJobs();
        // This is for replication jobs.
        if (BeaconQuartzScheduler.START_NODE_GROUP.equals(group)) {
            for (JobExecutionContext executionContext : currentlyExecutingJobs) {
                JobKey key = executionContext.getJobDetail().getKey();
                // Comparing only name (policy id) as group will be different (offsets).
                if (name.equals(key.getName())) {
                    LOG.info("Interrupt Job id: {}, group: {} from the currently running jobs.",
                            key.getName(), key.getGroup());
                    return scheduler.interrupt(key);
                }
            }
        } else {
            JobKey jobKey = new JobKey(name, group);
            LOG.info("Interrupting job {}", jobKey);
            return scheduler.interrupt(jobKey);
        }
        return false;
    }

    boolean interrupt(String name, String group) throws SchedulerException {
        assert group.equals(BeaconQuartzScheduler.START_NODE_GROUP): ASSERTION_MSG;
        return interruptJob(name, group);
    }

    public boolean checkExists(String name, String group) throws SchedulerException {
        JobKey jobKey = new JobKey(name, group);
        return scheduler.checkExists(jobKey);
    }

    boolean recoverPolicyInstance(String name, String group, String recoverInstance) throws SchedulerException {
        SchedulerCache cache = SchedulerCache.get();
        try {
            JobKey jobKey = new JobKey(name, group);
            JobDetail jobDetail = scheduler.getJobDetail(jobKey);
            if (jobDetail == null) {
                LOG.warn("Could not find job [{}] in the scheduler.", jobKey);
                return false;
            }
            jobDetail.getJobDataMap().put(QuartzDataMapEnum.RECOVER_INSTANCE.getValue(), recoverInstance);
            jobDetail.getJobDataMap().put(QuartzDataMapEnum.IS_RECOVERY.getValue(), true);
            synchronized (cache) {
                boolean exists = cache.exists(name);
                if (!exists) {
                    cache.insert(name, new InstanceSchedulerDetail());
                    scheduler.addJob(jobDetail, true);
                    scheduler.triggerJob(jobKey);
                }
                // TODO what to do, if any policy id is already present into the cache.
                // though, in real-time, it should not happen.
                return !exists;
            }
        } catch (SchedulerException e) {
            cache.remove(name);
            throw e;
        }
    }

    boolean rerunPolicyInstance(String name, String group, String recoverInstance) throws SchedulerException {
        return recoverPolicyInstance(name, group, recoverInstance);
    }
}
