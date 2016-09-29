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

import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.JobKey.jobKey;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

public class QuartzReplication {
    SchedulerFactory factory;
    Scheduler scheduler;

    public void createScheduler() throws SchedulerException {

        factory = new StdSchedulerFactory();
        scheduler = factory.getScheduler();
        scheduler.getListenerManager().addSchedulerListener(new ReplSchedulerListener());
        scheduler.getListenerManager().addTriggerListener((new ReplTriggerListener("replication")));
        scheduler.getListenerManager().addJobListener(new ReplJobListener("replication"));
    }

    public void startScheduler() throws SchedulerException {
        scheduler.start();
    }

    public void stopScheduler() throws SchedulerException {
        if (scheduler.isStarted()) {
            scheduler.shutdown(false);
        }
    }

    public void createReplicationJob(String name, ReplicationJobDetails details) throws SchedulerException {
        JobDataMap dataMap = getDataMap(details);
        dataMap.put("name", details.getName());

        JobDetail replicationJob = newJob(ReplicationJob.class)
                .withIdentity(details.getName() + "_job", "replication")
                .storeDurably()
                .usingJobData(dataMap)
                .build();
        scheduler.addJob(replicationJob, true);
    }

    public void scheduleJob(ReplicationJobDetails details) throws  SchedulerException {
        Trigger trigger = newTrigger()
                .withIdentity(details.getName() + "_trigger", "replication")
                .startNow()
                .forJob(jobKey(details.getName() + "_job", "replication"))
                .withSchedule(simpleSchedule()
                        .withIntervalInSeconds(details.getFrequency())
                        .repeatForever())
                .build();
        scheduler.scheduleJob(trigger);
    }

    public JobDataMap getDataMap(ReplicationJobDetails details) {
        JobDataMap map = new JobDataMap();
        map.put("details", details);
        return map;
    }

    public static void main(String args[]) throws Exception {
        QuartzReplication repl = new QuartzReplication();
        repl.createScheduler();
        ReplicationJobDetails details = new ReplicationJobDetails("test",1, "jdbc:hive:src", "jdbc:hive:target");
        repl.createReplicationJob("test", details);
        repl.scheduleJob(details);
        repl.startScheduler();
        Thread.sleep(10 * 1000);
        repl.stopScheduler();
    }
}
