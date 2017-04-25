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

package com.hortonworks.beacon.scheduler.internal;

import com.hortonworks.beacon.scheduler.quartz.QuartzDataMapEnum;
import org.quartz.InterruptableJob;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Quartz based implementation of AdminJob.
 */
public class SchedulableAdminJob implements InterruptableJob {

    private AtomicReference<Thread> runningThread = new AtomicReference<>();
    private static final Logger LOG = LoggerFactory.getLogger(SchedulableAdminJob.class);
    private AdminJob adminJob;

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        LOG.info("Interrupt received for job [{}].",
                adminJob != null ? adminJob.getClass().getSimpleName() : "SchedulableAdminJob");
        Thread thread = runningThread.get();
        if (thread != null) {
            thread.interrupt();
        }
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        this.runningThread.set(Thread.currentThread());
        JobDetail jobDetail = context.getJobDetail();
        adminJob = (AdminJob) jobDetail.getJobDataMap().get(QuartzDataMapEnum.ADMIN_JOB.getValue());
        JobKey jobKey = jobDetail.getKey();
        try {
            boolean result = adminJob.perform();
            if (result) {
                LOG.info("AdminJob [{}] is completed successfully. Removing the scheduled job.",
                        adminJob.getClass().getSimpleName());
                Scheduler scheduler = context.getScheduler();
                scheduler.deleteJob(jobKey);
            }
        } catch (Throwable e) {
            LOG.error("AdminJob [{}] error message: {}", adminJob.getClass().getSimpleName(), e.getMessage(), e);
        }
    }
}
