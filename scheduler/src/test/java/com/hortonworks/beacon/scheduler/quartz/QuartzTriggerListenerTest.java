/**
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *    OR LOSS OR CORRUPTION OF DATA.
 */

package com.hortonworks.beacon.scheduler.quartz;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.ReplicationJobDetails;
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
            add(BeaconStoreService.class.getName());
        }
    };

    private static final List<String> DEPENDENT_SERVICES = new ArrayList<String>() {
        {
            add(BeaconQuartzScheduler.class.getName());
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
        BeaconQuartzScheduler scheduler = Services.get().getService(BeaconQuartzScheduler.class);
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
