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

import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.store.bean.ChainedJobsBean;
import com.hortonworks.beacon.store.executors.ChainedJobsExecutor;
import org.quartz.JobDataMap;
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

    /**
     * We should have only one instance of the Scheduler running.
     * TODO: singleton implementation required.
     */
    private static Scheduler scheduler;

    private static final Logger LOG = LoggerFactory.getLogger(QuartzScheduler.class);

    public void startScheduler(JobListener jListener, TriggerListener tListener, SchedulerListener sListener) throws SchedulerException {
        SchedulerFactory factory = new StdSchedulerFactory();
        scheduler = factory.getScheduler();
        scheduler.getListenerManager().addJobListener(jListener, EverythingMatcher.allJobs());
        scheduler.getListenerManager().addTriggerListener(tListener, EverythingMatcher.allTriggers());
        scheduler.getListenerManager().addSchedulerListener(sListener);
        scheduler.start();
    }

    public void stopScheduler() throws SchedulerException {
        if (scheduler != null && scheduler.isStarted()) {
            scheduler.shutdown(true);
        }
    }

    public void scheduleJob(JobDetail jobDetail, Trigger trigger) throws SchedulerException {
        scheduler.addJob(jobDetail, true);
        trigger = trigger.getTriggerBuilder().forJob(jobDetail).build();
        scheduler.scheduleJob(trigger);
        LOG.info("Job [key: {}] and trigger [key: {}] are scheduled.",
                jobDetail.getKey(), trigger.getJobKey());
    }

    public void scheduleChainedJobs(List<JobDetail> jobs, Trigger trigger) throws SchedulerException {
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

    public void addJob(JobDetail jobDetail, boolean replace) throws SchedulerException {
        scheduler.addJob(jobDetail, replace);
        LOG.info("Added Job [key: {}] to the scheduler.", jobDetail.getKey());
    }

    public boolean isStarted() throws SchedulerException {
        return scheduler != null && scheduler.isStarted() && !scheduler.isShutdown();
    }

    public boolean deleteJob(String name, String group) throws SchedulerException {
        JobKey jobKey = new JobKey(name, group);
        LOG.info("Deleting Job [key: {}] from the scheduler.", jobKey);
        return scheduler.deleteJob(jobKey);
    }

    public ReplicationJobDetails listJob(String name, String group) throws SchedulerException {
        JobKey jobKey = new JobKey(name, group);
        JobDetail jobDetail = scheduler.getJobDetail(jobKey);
        JobDataMap jobDataMap = jobDetail.getJobDataMap();
        return (ReplicationJobDetails) jobDataMap.get(QuartzDataMapEnum.DETAILS.getValue());
    }

    public JobDetail getJobDetail(String keyName, String keyGroup) throws SchedulerException {
        return scheduler.getJobDetail(new JobKey(keyName, keyGroup));
    }

    public void suspendJob(String name, String group) throws SchedulerException {
        JobKey jobKey = new JobKey(name, group);
        scheduler.pauseJob(jobKey);
    }

    public void resumeJob(String name, String group) throws SchedulerException {
        JobKey jobKey = new JobKey(name, group);
        scheduler.resumeJob(jobKey);
    }
}
