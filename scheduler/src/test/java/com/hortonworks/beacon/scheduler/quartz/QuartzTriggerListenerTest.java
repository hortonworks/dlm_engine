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

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.scheduler.SchedulerInitService;
import com.hortonworks.beacon.scheduler.SchedulerStartService;
import com.hortonworks.beacon.service.BeaconStoreService;
import com.hortonworks.beacon.service.ServiceManager;
import com.hortonworks.beacon.service.Services;
import com.hortonworks.beacon.tools.BeaconDBSetup;
import com.hortonworks.beacon.util.ReplicationType;
import org.quartz.SchedulerException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Tests for quartz trigger listener.
 */
public class QuartzTriggerListenerTest {

    private static final String JOB_IDENTIFIER = "job-identifier";
    private static final String NAME = "test-job";
    private static final String POLICY_ID = "dataCenter-Cluster-0-1488946092144-000000001";

    private static final List<String> DEFAULT_SERVICES = new ArrayList<String>() {
        {
            add(SchedulerInitService.SERVICE_NAME);
            add(BeaconStoreService.SERVICE_NAME);
        }
    };

    private static final List<String> DEPENDENT_SERVICES = new ArrayList<String>() {
        {
            add(SchedulerStartService.SERVICE_NAME);
        }
    };

    @BeforeClass
    public void setup() throws Exception {
        createDBSchema();
        ServiceManager.getInstance().initialize(DEFAULT_SERVICES, DEPENDENT_SERVICES);
    }

    @AfterClass
    public void teardown() throws BeaconException {
        ServiceManager.getInstance().destroy();
    }

    @Test
    public void testDanglingJobRemoved() throws BeaconException, InterruptedException, SchedulerException {
        SchedulerInitService service = Services.get().getService(SchedulerInitService.SERVICE_NAME);
        BeaconQuartzScheduler scheduler = service.getScheduler();
        Date startTime = new Date(System.currentTimeMillis() + 2*1000);
        scheduler.schedulePolicy(getReplicationJob(), false, POLICY_ID, startTime, null, 10);
        boolean exists = scheduler.checkExists(POLICY_ID, BeaconQuartzScheduler.START_NODE_GROUP);
        Assert.assertTrue(exists);
        Thread.sleep(5*1000);
        exists = scheduler.checkExists(POLICY_ID, BeaconQuartzScheduler.START_NODE_GROUP);
        Assert.assertFalse(exists);
    }

    private void createDBSchema() throws Exception {
        String currentDir = System.getProperty("user.dir");
        File hsqldbFile = new File(currentDir, "../src/sql/tables_hsqldb.sql");
        BeaconConfig.getInstance().getDbStore().setSchemaDirectory(hsqldbFile.getParent());
        BeaconDBSetup.setupDB();
    }

    private List<ReplicationJobDetails> getReplicationJob() {
        List<ReplicationJobDetails> jobDetailsList = new ArrayList<>();
        ReplicationJobDetails detail = new ReplicationJobDetails(JOB_IDENTIFIER, NAME, ReplicationType.TEST.getName(),
                null);
        jobDetailsList.add(detail);
        return jobDetailsList;
    }
}