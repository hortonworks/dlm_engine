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

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.listeners.JobListenerSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class QuartzJobListener extends JobListenerSupport {

    private static final Logger LOG = LoggerFactory.getLogger(QuartzJobListener.class);
    private String name;
    private Map<JobKey, JobKey> chainLinks;


    public QuartzJobListener(String name) {
        this.name = name;
        chainLinks = new HashMap<>();
    }

    public String getName() {
        return name;
    }

    @Override
    public void jobToBeExecuted(JobExecutionContext context) {
        LOG.info("Job [key: {}] to be executed.", context.getJobDetail().getKey());
    }

    @Override
    public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
        JobKey sj = chainLinks.get(context.getJobDetail().getKey());

        if(sj == null) {
            return;
        }

        LOG.info("Job '" + context.getJobDetail().getKey() + "' will now chain to Job '" + sj + "'");

        try {
            context.getScheduler().triggerJob(sj);
        } catch(SchedulerException se) {
            getLog().error("Error encountered during chaining to Job '" + sj + "'", se);
        }
    }

    void addJobChainLink(JobKey firstJob, JobKey secondJob) {
        if(firstJob == null || secondJob == null) {
            throw new IllegalArgumentException("Key cannot be null!");
        }

        if(firstJob.getName() == null || secondJob.getName() == null) {
            throw new IllegalArgumentException("Key cannot have a null name!");
        }
        LOG.info("Job [key: {}] is chained with Job [key: {}]", firstJob, secondJob);
        chainLinks.put(firstJob, secondJob);
    }
}
