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

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.replication.ReplicationType;
import com.hortonworks.beacon.scheduler.BeaconScheduler;
import com.hortonworks.beacon.store.bean.JobInstanceBean;
import com.hortonworks.beacon.store.bean.PolicyInfoBean;
import com.hortonworks.beacon.store.executors.JobInstanceExecutor;
import com.hortonworks.beacon.store.executors.JobInstanceExecutor.JobInstanceQuery;
import com.hortonworks.beacon.store.executors.PolicyInfoExecutor;
import com.hortonworks.beacon.store.executors.PolicyInfoExecutor.PolicyInfoQuery;
import org.quartz.JobDetail;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class BeaconQuartzScheduler implements BeaconScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconQuartzScheduler.class);
    private QuartzScheduler scheduler;
    private QuartzJobDetailBuilder jobDetailBuilder;
    private QuartzTriggerBuilder triggerBuilder;
    private boolean testMode = true;
    private static BeaconQuartzScheduler INSTANCE = new BeaconQuartzScheduler();

    private BeaconQuartzScheduler() {
        scheduler = QuartzScheduler.get();
        jobDetailBuilder = new QuartzJobDetailBuilder();
        triggerBuilder = new QuartzTriggerBuilder();
    }

    public static BeaconQuartzScheduler get() {
        return INSTANCE;
    }

    @Override
    public void startScheduler() throws BeaconException {
        try {
            if (!isStarted()) {
                scheduler.startScheduler(new QuartzJobListener("quartzJobListener"),
                        new QuartzTriggerListener("quartzTriggerListener"),
                        new QuartzSchedulerListener());
                LOG.info("Beacon scheduler started successfully.");
            } else {
                LOG.info("Instance of the Beacon scheduler is already running.");
            }
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    @Override
    public String scheduleJob(ReplicationJobDetails job, boolean recovery) throws BeaconException {
        JobDetail jobDetail = jobDetailBuilder.createJobDetail(job, recovery);
        Trigger trigger = triggerBuilder.createTrigger(job);
        try {
            scheduler.scheduleJob(jobDetail, trigger);
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
        return jobDetail.getKey().getName();
    }

    // TODO Currently using first job for creating trigger
    @Override
    public List<String> scheduleChainedJobs(List<ReplicationJobDetails> jobs, boolean recovery) throws BeaconException {
        List<JobDetail> jobDetails = jobDetailBuilder.createJobDetailList(jobs, recovery);
        Trigger trigger = triggerBuilder.createTrigger(jobs.get(0));
        try {
            scheduler.scheduleChainedJobs(jobDetails, trigger);
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
        List<String> jobNames = new ArrayList<>();
        for (JobDetail jobDetail : jobDetails) {
            jobNames.add(jobDetail.getKey().getName());
        }
        return jobNames;
    }

    @Override
    public void stopScheduler() throws BeaconException {
        try {
            if (isStarted()) {
                scheduler.stopScheduler();
                LOG.info("Beacon scheduler shutdown successfully.");
            } else {
                LOG.info("Beacon scheduler is not running.");
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
    public boolean deleteJob(String name, String type) throws BeaconException {
        LOG.info("Deleting the scheduled replication entity with name : {} type : {} ", name, type);
        try {
            String jobType = ReplicationType.valueOf(type).getName();
            boolean deleteJob = scheduler.deleteJob(name, jobType);
            if (deleteJob) {
                List<JobInstanceBean> beanList = listJob(name, type);
                for (JobInstanceBean bean : beanList) {
                    bean.setDeleted(1);
                    JobInstanceExecutor executor = new JobInstanceExecutor(bean);
                    executor.executeUpdate(JobInstanceQuery.SET_DELETED);
                }
            }
            return deleteJob;
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    @Override
    public List<JobInstanceBean> listJob(String name, String type) throws BeaconException {
        LOG.info("Listing job instances for [name: {}, type: {}]", name, type);
        type = ReplicationType.valueOf(type).getName();
        JobInstanceBean bean = new JobInstanceBean();
        bean.setJobName(name);
        bean.setJobGroup(type);
        bean.setDeleted(0);
        JobInstanceExecutor executor = new JobInstanceExecutor(bean);
        List<JobInstanceBean> beanList = executor.executeSelectQuery(JobInstanceQuery.SELECT_JOB_INSTANCE);
        try {
            JobInstanceBean instanceBean = scheduler.listJob(name, type);
            if (instanceBean != null) {
                beanList.add(instanceBean);
            }
        } catch (SchedulerException sce) {
            LOG.error("Exception occurred while listing instances for name : {}, type : {}, {}",name, type, sce);
        }
        LOG.info("Listing job instances for [name: {}, type: {}, size: {}]", name, type, beanList.size());
        return beanList;
    }

    @Override
    public String addJob(ReplicationJobDetails job, boolean recovery) throws BeaconException {
        JobDetail jobDetail = jobDetailBuilder.createJobDetail(job, recovery);
        try {
            scheduler.addJob(jobDetail, true);
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
        return jobDetail.getKey().getName();
    }

    @Override
    public void scheduleJob(String name, String type) throws BeaconException {
        try {
            type = ReplicationType.valueOf(type).getName();
            JobDetail jobDetail = scheduler.getJobDetail(name, type);
            ReplicationJobDetails job = (ReplicationJobDetails) jobDetail.getJobDataMap().get(QuartzDataMapEnum.DETAILS.getValue());
            Trigger trigger = triggerBuilder.createTrigger(job);
            scheduler.scheduleJob(jobDetail, trigger);
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    @Override
    public void suspendJob(String name, String type) throws BeaconException {
        try {
            type = ReplicationType.valueOf(type).getName();
            scheduler.suspendJob(name, type);
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    @Override
    public void resumeJob(String name, String type) throws BeaconException {
        try {
            type = ReplicationType.valueOf(type).getName();
            scheduler.resumeJob(name, type);
        } catch (SchedulerException e) {
            throw new BeaconException(e.getMessage(), e);
        }
    }

    @Override
    public String getPolicyStatus(String name, String type) {
        type = ReplicationType.valueOf(type.toUpperCase()).getName();
        PolicyInfoBean bean = new PolicyInfoBean();
        bean.setName(name);
        bean.setType(type);
        PolicyInfoExecutor executor = new PolicyInfoExecutor(bean);
        PolicyInfoBean resultBean = executor.executeSingleSelectQuery(PolicyInfoQuery.SELECT_POLICY_INFO);
        return resultBean.getStatus();
    }
}
