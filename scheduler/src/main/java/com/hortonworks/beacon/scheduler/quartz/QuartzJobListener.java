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

import com.hortonworks.beacon.nodes.NodeGenerator;
import com.hortonworks.beacon.replication.InstanceExecutionDetails;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.util.ReplicationHelper;
import com.hortonworks.beacon.common.job.JobStatus;
import com.hortonworks.beacon.store.bean.ChainedJobsBean;
import com.hortonworks.beacon.store.executors.ChainedJobsExecutor;
import com.hortonworks.beacon.store.executors.ChainedJobsExecutor.ChainedJobQuery;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor.PolicyInstanceQuery;
import org.apache.commons.lang3.StringUtils;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.listeners.JobListenerSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Beacon extended implementation for JobListenerSupport.
 */
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
        checkParallelExecution();
        handleStartNodeForInstance(context);
    }

    private void checkParallelExecution() {
        // TODO check and prevent parallel execution execution of the job instance.
        // Though it should be handled by DisallowConcurrentExecution automatically.
    }

    private void handleStartNodeForInstance(JobExecutionContext context) {
        if (context.getJobDetail().getKey().getGroup().equals(NodeGenerator.START_NODE)) {
            JobDetail jobDetail = context.getJobDetail();
            JobDataMap jobDataMap = jobDetail.getJobDataMap();
            int count = jobDataMap.getInt(QuartzDataMapEnum.COUNTER.getValue());
            count++;
            jobDataMap.put(QuartzDataMapEnum.COUNTER.getValue(), count);
            PolicyInstanceBean bean = createJobInstance(context);
            PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
            executor.execute();
            //TODO insert the job instances into table.
        }
    }

    @Override
    public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
        updateJobInstance(context, jobException);
        // In case of failure, do not schedule next chained job.
        if (jobException != null) {
            return;
        }

        JobKey sj = chainLinks.get(context.getJobDetail().getKey());

        if (sj == null && !context.getJobDetail().getJobDataMap().getBoolean(QuartzDataMapEnum.ISCHAINED.getValue())) {
            return;
        } else if (sj == null) {
            sj = getNextJobFromStore(context.getJobDetail().getKey());
            chainLinks.put(context.getJobDetail().getKey(), sj);
        }

        LOG.info("Job '" + context.getJobDetail().getKey() + "' will now chain to Job '" + sj + "'");
        try {
            context.getScheduler().triggerJob(sj);
        } catch (SchedulerException se) {
            getLog().error("Error encountered during chaining to Job '" + sj + "'", se);
        }

    }

    private JobKey getNextJobFromStore(JobKey key) {
        ChainedJobsBean bean = new ChainedJobsBean();
        bean.setFirstJobName(key.getName());
        bean.setFirstJobGroup(key.getGroup());
        ChainedJobsExecutor executor = new ChainedJobsExecutor(bean);
        ChainedJobsBean jobBean = executor.executeSelectQuery(ChainedJobQuery.GET_SECOND_JOB);
        JobKey jobKey = new JobKey(jobBean.getSecondJobName(), jobBean.getSecondJobGroup());
        return jobKey;
    }

    private void updateJobInstance(JobExecutionContext context, JobExecutionException jobException) {
        PolicyInstanceBean bean = new PolicyInstanceBean();

        if (context.getResult() == null) {
            LOG.error("Job execution context: {}", context);
        } else {
            LOG.info("Update job instance with context : {}", context.getResult());
            InstanceExecutionDetails details = new InstanceExecutionDetails((String) context.getResult());
            if (details.getJobStatus().equals(JobStatus.SUCCESS.name())) {
                bean.setStatus(JobStatus.SUCCESS.name());
                bean.setMessage(details.getJobMessage());
            } else {
                bean.setStatus(JobStatus.FAILED.name());
                if (jobException == null) {
                    bean.setMessage(truncateExceptionMessage(details.getJobMessage()));
                } else {
                    bean.setMessage(truncateExceptionMessage(jobException.getMessage()));
                }
            }

            LOG.info("Setting : {} : job execution type", details.getJobExecutionType());
            if (StringUtils.isNotBlank(details.getJobExecutionType())) {
                bean.setJobExecutionType(details.getJobExecutionType().toLowerCase());
            }
        }

        bean.setEndTime(new Date());

        JobDetail jobDetail = context.getJobDetail();
        JobDataMap jobDataMap = jobDetail.getJobDataMap();
        int count = jobDataMap.getInt(QuartzDataMapEnum.COUNTER.getValue());

        bean.setId(context.getJobDetail().getKey().getName() + "@" + count);

        PolicyInstanceExecutor executor = new PolicyInstanceExecutor(bean);
        executor.executeUpdate(PolicyInstanceQuery.UPDATE_POLICY_INSTANCE);
    }

    void addJobChainLink(JobKey firstJob, JobKey secondJob) {
        if (firstJob == null || secondJob == null) {
            throw new IllegalArgumentException("Key cannot be null!");
        }

        if (firstJob.getName() == null || secondJob.getName() == null) {
            throw new IllegalArgumentException("Key cannot have a null name!");
        }
        LOG.info("Job [key: {}] is chained with Job [key: {}]", firstJob, secondJob);
        chainLinks.put(firstJob, secondJob);
    }

    private PolicyInstanceBean createJobInstance(JobExecutionContext context) {
        PolicyInstanceBean bean = new PolicyInstanceBean();
        JobDetail jobDetail = context.getJobDetail();
        JobDataMap jobDataMap = jobDetail.getJobDataMap();
        int count = jobDataMap.getInt(QuartzDataMapEnum.COUNTER.getValue());
        ReplicationJobDetails job = (ReplicationJobDetails) jobDataMap.get(QuartzDataMapEnum.DETAILS.getValue());

        String jobName = jobDetail.getKey().getName();
        bean.setId(jobName + "@" + count);
        bean.setPolicyId(jobName);
        // TODO (abafna) do not understand this.
        String type = ReplicationHelper.getReplicationType(job.getType()).getName();
        bean.setJobExecutionType(type);
        bean.setStartTime(new Date());
        bean.setStatus(JobStatus.RUNNING.name());
        return bean;
    }

    private String truncateExceptionMessage(String jobExceptionMessage) {
        return (jobExceptionMessage.length() > 4000)
                ? jobExceptionMessage.substring(0, 3899) + " ..."
                : jobExceptionMessage;
    }
}
