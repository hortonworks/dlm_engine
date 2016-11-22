package com.hortonworks.beacon.scheduler.quartz;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuartzTestJob implements Job {

    private static final Logger LOGGER = LoggerFactory.getLogger(QuartzTestJob.class);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        LOGGER.info("Executing quartz test job class.");
    }
}