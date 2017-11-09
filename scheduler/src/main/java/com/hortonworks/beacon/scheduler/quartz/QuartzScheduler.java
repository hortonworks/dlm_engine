/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.scheduler.quartz;

import java.util.List;
import java.util.Properties;

import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SchedulerListener;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.TriggerListener;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.matchers.NotMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.scheduler.InstanceSchedulerDetail;
import com.hortonworks.beacon.scheduler.SchedulerCache;
import com.hortonworks.beacon.scheduler.internal.AdminJob;

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

    void initializeScheduler(JobListener jListener, TriggerListener tListener, SchedulerListener sListener,
                        Properties properties)
            throws SchedulerException {
        SchedulerFactory factory = new StdSchedulerFactory(properties);
        scheduler = factory.getScheduler();
        scheduler.getListenerManager().addJobListener(jListener,
                NotMatcher.not(GroupMatcher.<JobKey>jobGroupStartsWith(AdminJob.ADMIN_JOBS)));
        scheduler.getListenerManager().addTriggerListener(tListener,
                NotMatcher.not(GroupMatcher.<TriggerKey>groupStartsWith(AdminJob.ADMIN_JOBS)));
        scheduler.getListenerManager().addSchedulerListener(sListener);
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
        scheduler.pauseJob(jobKey);
    }

    void resumeJob(String name, String group) throws SchedulerException {
        JobKey jobKey = new JobKey(name, group);
        JobDetail jobDetail = scheduler.getJobDetail(jobKey);
        if (jobDetail == null) {
            LOG.warn("Could not find job [{}] in the scheduler.", jobKey);
            throw new SchedulerException("No suspended policy found");
        }
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
