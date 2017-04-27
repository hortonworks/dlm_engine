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

package com.hortonworks.beacon.events;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.store.bean.EventBean;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.store.executors.EventsExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.persistence.EntityManager;
import java.util.Date;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Beacon Events test.
 */

public class BeaconEventsTest {

    private static final Logger LOG = LoggerFactory.getLogger(BeaconEventsTest.class);

    private static final String POLICY_NAME = "fsRepl";
    private static final String POLICY_ID = "/NYC/source/"+POLICY_NAME+"/0/1490791/0001";

    private static EventBean systemEventBean;
    private static EventBean clusterEventBean;
    private static EventBean policyEventBean;
    private static EventBean policyInstanceEventBean;

    private static EventsExecutor executor;
    private static EntityManager entityManager;

    @BeforeClass
    public void setUp() throws BeaconException {
        executor = mock(EventsExecutor.class);
        entityManager = mock(EntityManager.class);

        executor.setEntityManager(entityManager);

        systemEventBean = BeaconEvents.createSystemEventsBean(Events.BEACON_STARTED.getId(),
                System.currentTimeMillis(), EventStatus.STARTED,
                "localhost", "beacon-started");
        clusterEventBean = BeaconEvents.createClusterEventsBean(Events.CLUSTER_ENTITY_SUBMITTED.getId(),
                System.currentTimeMillis(), EventStatus.SUBMITTED, "cluster-entity-submitted");

        policyEventBean = BeaconEvents.createPolicyEventsBean(Events.POLICY_SUBMITTED.getId(),
                System.currentTimeMillis(), EventStatus.SUBMITTED,
                "Policy Submitted", createPolicyBean());

        policyInstanceEventBean = BeaconEvents.createPolicyInstanceEventsBean(Events.POLICY_SUBMITTED.getId(),
                System.currentTimeMillis(), EventStatus.SUBMITTED,
                "Policy Submitted", createPolicyInstanceBean());

        when(executor.addEvents(systemEventBean)).thenReturn(systemEventBean);
        when(executor.addEvents(clusterEventBean)).thenReturn(clusterEventBean);
        when(executor.addEvents(policyEventBean)).thenReturn(policyEventBean);
        when(executor.addEvents(policyInstanceEventBean)).thenReturn(policyInstanceEventBean);
    }

    @Test
    public void testSystemEvents() throws BeaconException {
        EventBean actual = executor.addEvents(systemEventBean);

        Assert.assertEquals(systemEventBean, actual);

        Assert.assertEquals(systemEventBean.getPolicyId(), actual.getPolicyId());
        Assert.assertEquals(systemEventBean.getInstanceId(), actual.getInstanceId());
        Assert.assertEquals(systemEventBean.getEventId(), actual.getEventId());
        Assert.assertEquals(systemEventBean.getEventStatus(), actual.getEventStatus());
        Assert.assertEquals(systemEventBean.getEventMessage(), actual.getEventMessage());
        Assert.assertEquals(systemEventBean.getEventTimeStamp(), actual.getEventTimeStamp());
    }

    @Test
    public void testClusterEvents() throws BeaconException {
        EventBean actual = executor.addEvents(clusterEventBean);

        Assert.assertEquals(clusterEventBean, actual);

        Assert.assertEquals(clusterEventBean.getPolicyId(), actual.getPolicyId());
        Assert.assertEquals(clusterEventBean.getInstanceId(), actual.getInstanceId());
        Assert.assertEquals(clusterEventBean.getEventId(), actual.getEventId());
        Assert.assertEquals(clusterEventBean.getEventStatus(), actual.getEventStatus());
        Assert.assertEquals(clusterEventBean.getEventMessage(), actual.getEventMessage());
        Assert.assertEquals(clusterEventBean.getEventTimeStamp(), actual.getEventTimeStamp());
    }

    @Test
    public void testPolicyEvents() throws BeaconException {
        EventBean actual = executor.addEvents(policyEventBean);

        Assert.assertEquals(policyEventBean, actual);

        Assert.assertEquals(policyEventBean.getPolicyId(), actual.getPolicyId());
        Assert.assertEquals(policyEventBean.getInstanceId(), actual.getInstanceId());
        Assert.assertEquals(policyEventBean.getEventId(), actual.getEventId());
        Assert.assertEquals(policyEventBean.getEventStatus(), actual.getEventStatus());
        Assert.assertEquals(policyEventBean.getEventMessage(), actual.getEventMessage());
        Assert.assertEquals(policyEventBean.getEventTimeStamp(), actual.getEventTimeStamp());
    }

    @Test
    public void testPolicyInstanceEvents() throws BeaconException {
        EventBean actual = executor.addEvents(policyInstanceEventBean);

        Assert.assertEquals(policyInstanceEventBean, actual);

        Assert.assertEquals(policyInstanceEventBean.getPolicyId(), actual.getPolicyId());
        Assert.assertEquals(policyInstanceEventBean.getInstanceId(), actual.getInstanceId());
        Assert.assertEquals(policyInstanceEventBean.getEventId(), actual.getEventId());
        Assert.assertEquals(policyInstanceEventBean.getEventStatus(), actual.getEventStatus());
        Assert.assertEquals(policyInstanceEventBean.getEventMessage(), actual.getEventMessage());
        Assert.assertEquals(policyInstanceEventBean.getEventTimeStamp(), actual.getEventTimeStamp());
    }

    private PolicyBean createPolicyBean() {
        PolicyBean bean = new PolicyBean();
        bean.setName(POLICY_NAME);
        bean.setId(POLICY_ID);
        bean.setVersion(1);
        bean.setCreationTime(new Date());
        bean.setLastModifiedTime(new Date());
        bean.setChangeId(1);
        bean.setRetirementTime(null);
        bean.setStatus(JobStatus.SUBMITTED.name());

        LOG.info("PolicyBean for name: [{}], type: [{}] stored.", bean.getName(), bean.getType());
        return bean;
    }

    private PolicyInstanceBean createPolicyInstanceBean() {
        PolicyInstanceBean bean = new PolicyInstanceBean();
        bean.setPolicyId(POLICY_ID);
        bean.setInstanceId(POLICY_ID+"@"+"1");
        bean.setStartTime(new Date());
        bean.setRunCount(0);
        bean.setStatus(JobStatus.SUCCESS.name());
        bean.setCurrentOffset(0);

        return bean;
    }
}
