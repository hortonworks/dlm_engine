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

package com.hortonworks.beacon.scheduler;

import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.client.entity.Notification;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.entity.Retry;
import com.hortonworks.beacon.entity.util.PolicyDao;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.scheduler.internal.StoreCleanupService;
import com.hortonworks.beacon.service.BeaconStoreService;
import com.hortonworks.beacon.service.ServiceManager;
import com.hortonworks.beacon.store.BeaconStoreException;
import com.hortonworks.beacon.store.bean.InstanceJobBean;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.store.executors.InstanceJobExecutor;
import com.hortonworks.beacon.store.executors.PolicyExecutor;
import com.hortonworks.beacon.store.executors.PolicyInstanceExecutor;
import com.hortonworks.beacon.tools.BeaconDBSetup;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Date;
import java.util.Properties;

/**
 * Test class for {@link StoreCleanupService}.
 */
public class StoreCleanupServiceTest {

    private StoreCleanupService storeCleanupService = StoreCleanupService.get();

    @BeforeClass
    public void setupClass() throws Exception {
        BeaconDBSetup.setupDB();
        ServiceManager.getInstance().initialize(Collections.singletonList(BeaconStoreService.class.getName()), null);
    }

    @AfterClass
    public void teardown() throws BeaconException {
        ServiceManager.getInstance().destroy();
    }

    public void createSinglePolicyDataSet(String name) throws BeaconStoreException {
        RequestContext.setInitialValue();
        RequestContext.get().startTransaction();
        savePolicy(name);
        PolicyExecutor policyExecutor = new PolicyExecutor(new PolicyBean(name));
        PolicyBean saved = policyExecutor.getActivePolicy();
        createPolicyInstances(saved.getId(), 5);
        RequestContext.get().commitTransaction();
    }

    private void createPolicyInstances(String policyId, int count) {
        for (int i = 0; i < count; i++) {
            String instanceId = savePolicyInstance(policyId, i);
            createInstanceJobs(instanceId, 5);
        }
    }

    private void createInstanceJobs(String instanceId, int i) {
        StoreHelper.insertJobInstance(instanceId, 5);
    }

    private void savePolicy(String name) throws BeaconStoreException {
        ReplicationPolicy replicationPolicy = new ReplicationPolicy();
        replicationPolicy.setNotification(new Notification("type", "to"));
        replicationPolicy.setRetry(new Retry());
        replicationPolicy.setCustomProperties(new Properties());
        replicationPolicy.setName(name);
        PolicyDao policyDao = new PolicyDao();
        policyDao.persistPolicy(replicationPolicy);

    }

    private String savePolicyInstance(String policyId, int identifier) {
        String instanceId = StoreHelper.insertPolicyInstance(policyId, identifier, "SUCCEEDED");
        return instanceId;

    }

    @Test
    public void testSinglePolicyCleanup() throws BeaconStoreException {
        createSinglePolicyDataSet("test");
        validateBeforeCleanup(1, 5, 25);
        RequestContext.setInitialValue();
        markPolicyDeleted("test");
        storeCleanupService.call();
        validatePostCleanup(0, 0, 0);
    }

    @Test
    public void testMultiPolicyCleanup() throws BeaconStoreException {
        for (int i=0; i < 3; i++) {
            createSinglePolicyDataSet("test" + i);
        }
        validateBeforeCleanup(3, 15, 75);
        RequestContext.setInitialValue();
        markPolicyDeleted("test0");
        markPolicyDeleted("test1");
        storeCleanupService.call();
        validatePostCleanup(1, 5, 25);
        RequestContext.setInitialValue();
        markPolicyDeleted("test2");
        storeCleanupService.call();
        validatePostCleanup(0, 0, 0);

    }
    private void validatePostCleanup(long policyExpected, long instanceExpected,
                                     long jobsExpected) throws BeaconStoreException {
        RequestContext.setInitialValue();
        RequestContext.get().startTransaction();
        PolicyExecutor policyExecutor = new PolicyExecutor(new PolicyBean());
        int policyCount = policyExecutor.getPolicies(PolicyExecutor.PolicyQuery.GET_ALL_POLICY).size();
        PolicyInstanceExecutor policyInstanceExecutor = new PolicyInstanceExecutor(new PolicyInstanceBean());
        int instanceCount = policyInstanceExecutor
                .executeSelectQuery(PolicyInstanceExecutor.PolicyInstanceQuery.GET_ALL_POLICY_INSTANCES).size();
        InstanceJobExecutor instanceJobExecutor = new InstanceJobExecutor(new InstanceJobBean());
        int jobCount = instanceJobExecutor
                .executeSelect(InstanceJobExecutor.InstanceJobQuery.GET_ALL_INSTANCE_JOBS).size();
        Assert.assertEquals(policyExpected, policyCount);
        Assert.assertEquals(instanceExpected, instanceCount);
        Assert.assertEquals(jobsExpected, jobCount);
        RequestContext.get().commitTransaction();
        RequestContext.get().clear();
    }

    private void markPolicyDeleted(String name) {
        RequestContext.get().startTransaction();
        PolicyBean bean = new PolicyBean(name);
        bean.setStatus(JobStatus.DELETED.name());
        bean.setRetirementTime(new Date());
        PolicyExecutor executor = new PolicyExecutor(bean);
        executor.executeUpdate(PolicyExecutor.PolicyQuery.DELETE_POLICY);
        RequestContext.get().commitTransaction();
    }

    private void validateBeforeCleanup(long policyExpected, long instanceExpected,
                                       long jobsExpected) throws BeaconStoreException {
        RequestContext.setInitialValue();
        RequestContext.get().startTransaction();
        PolicyExecutor policyExecutor = new PolicyExecutor(new PolicyBean());
        int policyCount = policyExecutor.getPolicies(PolicyExecutor.PolicyQuery.GET_ALL_POLICY).size();
        PolicyInstanceExecutor policyInstanceExecutor = new PolicyInstanceExecutor(new PolicyInstanceBean());
        int instanceCount = policyInstanceExecutor
                .executeSelectQuery(PolicyInstanceExecutor.PolicyInstanceQuery.GET_ALL_POLICY_INSTANCES).size();
        InstanceJobExecutor instanceJobExecutor = new InstanceJobExecutor(new InstanceJobBean());
        int jobCount = instanceJobExecutor
                .executeSelect(InstanceJobExecutor.InstanceJobQuery.GET_ALL_INSTANCE_JOBS).size();
        Assert.assertEquals(policyExpected, policyCount);
        Assert.assertEquals(instanceExpected, instanceCount);
        Assert.assertEquals(jobsExpected, jobCount);
        RequestContext.get().commitTransaction();
        RequestContext.get().clear();
    }

}
