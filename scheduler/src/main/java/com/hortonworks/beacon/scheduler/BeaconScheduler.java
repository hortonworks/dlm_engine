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

import org.quartz.JobDetail;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SchedulerListener;
import org.quartz.Trigger;
import org.quartz.TriggerListener;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.EverythingMatcher;
import org.quartz.listeners.JobChainingJobListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BeaconScheduler {

    /**
     * We should have only one instance of the Scheduler running.
     * TODO: singleton implementation required.
     */
    private static Scheduler scheduler;

    private static final Logger LOG = LoggerFactory.getLogger(BeaconScheduler.class);

    public void startScheduler(JobListener jListener, TriggerListener tListener, SchedulerListener sListener) {
        try {
            SchedulerFactory factory = new StdSchedulerFactory();
            scheduler = factory.getScheduler();
            scheduler.getListenerManager().addJobListener(jListener, EverythingMatcher.allJobs());
            scheduler.getListenerManager().addTriggerListener(tListener, EverythingMatcher.allTriggers());
            scheduler.getListenerManager().addSchedulerListener(sListener);
            scheduler.start();
            LOG.info("Scheduler started successfully.");
        } catch (SchedulerException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
            // This will be fixed once we have proper error codes defined.
        }
    }

    public void stopScheduler() throws SchedulerException {
        if (scheduler != null && scheduler.isStarted()) {
            scheduler.shutdown(false);
            LOG.info("Scheduler shutdown successfully.");
        }
    }

    public void scheduleJob(JobDetail jobDetail, Trigger trigger) throws SchedulerException {
        scheduler.scheduleJob(jobDetail, trigger);
        LOG.info("Job [key: {}] and trigger [key: {}] are scheduled.",
                jobDetail.getKey(), trigger.getJobKey());
    }

    public void scheduleChainedJobs(List<JobDetail> jobs, Trigger trigger) throws SchedulerException {
        BeaconJobListener listener = (BeaconJobListener) scheduler.getListenerManager().getJobListener("");
        for (int i = 1; i < jobs.size(); i++) {
            JobDetail firstJob = jobs.get(i-1);
            JobDetail secondJob = jobs.get(i);
            listener.addJobChainLink(firstJob.getKey(), secondJob.getKey());
            scheduler.addJob(secondJob, false);
        }
        scheduler.scheduleJob(jobs.get(0), trigger);
        LOG.info("Job [key: {}] and trigger [key: {}] are scheduled.",
                jobs.get(0).getKey(), trigger.getKey());
    }

    public void addJob(JobDetail jobDetail, boolean replace) throws SchedulerException {
        scheduler.addJob(jobDetail, replace);
    }
}
