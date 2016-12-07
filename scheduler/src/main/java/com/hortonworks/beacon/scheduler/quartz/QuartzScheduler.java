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

import com.hortonworks.beacon.store.JobStatus;
import com.hortonworks.beacon.store.bean.ChainedJobsBean;
import com.hortonworks.beacon.store.bean.JobInstanceBean;
import com.hortonworks.beacon.store.executors.ChainedJobsExecutor;
import com.hortonworks.beacon.utils.SchedulerUtils;
import org.quartz.JobDetail;
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

public class QuartzScheduler {

    private Scheduler scheduler;
    private static final QuartzScheduler INSTANCE = new QuartzScheduler();

    private static final Logger LOG = LoggerFactory.getLogger(QuartzScheduler.class);

    private QuartzScheduler() {
        try {
            SchedulerUtils.createDBSchema();
        } catch (Exception e) {
            LOG.error("Unable to initialize DB", e);
        }
    }

    public static QuartzScheduler get() {
        return INSTANCE;
    }

    void startScheduler(JobListener jListener, TriggerListener tListener, SchedulerListener sListener) throws SchedulerException {
        SchedulerFactory factory = new StdSchedulerFactory();
        scheduler = factory.getScheduler();
        scheduler.getListenerManager().addJobListener(jListener, EverythingMatcher.allJobs());
        scheduler.getListenerManager().addTriggerListener(tListener, EverythingMatcher.allTriggers());
        scheduler.getListenerManager().addSchedulerListener(sListener);
        scheduler.start();
    }

    void stopScheduler() throws SchedulerException {
        if (scheduler != null && scheduler.isStarted()) {
            scheduler.shutdown(true);
        }
    }

    void scheduleJob(JobDetail jobDetail, Trigger trigger) throws SchedulerException {
        trigger = trigger.getTriggerBuilder().forJob(jobDetail).build();
        //scheduler.addJob(jobDetail, true);
        scheduler.scheduleJob(jobDetail, trigger);
        LOG.info("Job [key: {}] and trigger [key: {}] are scheduled.",
                jobDetail.getKey(), trigger.getJobKey());
    }

    void scheduleChainedJobs(List<JobDetail> jobs, Trigger trigger) throws SchedulerException {
        QuartzJobListener listener = (QuartzJobListener) scheduler.getListenerManager().getJobListener("quartzJobListener");
        for (int i = 1; i < jobs.size(); i++) {
            JobDetail firstJob = jobs.get(i-1);
            JobDetail secondJob = jobs.get(i);
            listener.addJobChainLink(firstJob.getKey(), secondJob.getKey());
            ChainedJobsBean bean = new ChainedJobsBean(firstJob.getKey().getName(),
                    firstJob.getKey().getGroup(), secondJob.getKey().getName(), secondJob.getKey().getGroup());
            ChainedJobsExecutor executor = new ChainedJobsExecutor(bean);
            executor.execute();
            scheduler.addJob(secondJob, false);
        }
        scheduler.scheduleJob(jobs.get(0), trigger);
        LOG.info("Job [key: {}] and trigger [key: {}] are scheduled.",
                jobs.get(0).getKey(), trigger.getKey());
    }

    void addJob(JobDetail jobDetail, boolean replace) throws SchedulerException {
        scheduler.addJob(jobDetail, replace);
        LOG.info("Added Job [key: {}] to the scheduler.", jobDetail.getKey());
    }

    boolean isStarted() throws SchedulerException {
        return scheduler != null && scheduler.isStarted() && !scheduler.isInStandbyMode() && !scheduler.isShutdown();
    }

    boolean deleteJob(String name, String group) throws SchedulerException {
        JobKey jobKey = new JobKey(name, group);
        LOG.info("Deleting Job [key: {}] from the scheduler.", jobKey);
        return scheduler.deleteJob(jobKey);
    }

    JobInstanceBean listJob(String name, String group) throws SchedulerException {
        LOG.info("Listing instances for entity name : {}, type : {} ", name, group);
        JobInstanceBean instanceBean = new JobInstanceBean();
        JobKey jobKey = new JobKey(name, group);

        List<Trigger> triggers = (List<Trigger>) scheduler.getTriggersOfJob(jobKey);

        if (triggers.size() > 0) {
            LOG.info("[jobName] : " + name + " [groupName] : " + group + " - " + triggers.get(0).getNextFireTime());
            instanceBean.setId(null);
            instanceBean.setName(name);
            instanceBean.setType(group);
            instanceBean.setStartTime(triggers.get(0).getNextFireTime().getTime());
            instanceBean.setEndTime(0L);
            instanceBean.setStatus(JobStatus.WAITING.name());
            instanceBean.setDuration(0L);
            instanceBean.setMessage("");
            return instanceBean;
        } else {
            return null;
        }
    }

    JobDetail getJobDetail(String keyName, String keyGroup) throws SchedulerException {
        return scheduler.getJobDetail(new JobKey(keyName, keyGroup));
    }

    void suspendJob(String name, String group) throws SchedulerException {
        JobKey jobKey = new JobKey(name, group);
        scheduler.pauseJob(jobKey);
    }

    void resumeJob(String name, String group) throws SchedulerException {
        JobKey jobKey = new JobKey(name, group);
        scheduler.resumeJob(jobKey);
    }

    // For testing only. To clear the jobs and triggers from Quartz.
    void clear() throws SchedulerException {
        scheduler.clear();
    }
}
