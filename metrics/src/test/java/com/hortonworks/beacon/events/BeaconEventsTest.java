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

package com.hortonworks.beacon.events;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.job.JobStatus;
import com.hortonworks.beacon.service.BeaconStoreService;
import com.hortonworks.beacon.service.ServiceManager;
import com.hortonworks.beacon.store.bean.EventBean;
import com.hortonworks.beacon.store.bean.PolicyBean;
import com.hortonworks.beacon.store.bean.PolicyInstanceBean;
import com.hortonworks.beacon.store.executors.EventsExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
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
    private static final String SOURCECLUSTER = "source";
    private static final String TARGETCLUSTER = "target";
    private static final String SOURCEDATASET = "testDataset";

    private static EventBean systemEventBean;
    private static EventBean clusterEventBean;
    private static EventBean policyEventBean;
    private static EventBean policyInstanceEventBean;

    private static EventsExecutor executor;

    @BeforeClass
    public void setUp() throws BeaconException {
        ServiceManager.getInstance().initialize(Arrays.asList(BeaconStoreService.class.getName()), null);
        executor = mock(EventsExecutor.class);
        systemEventBean = BeaconEvents.createEventsBean(Events.STARTED, EventEntityType.SYSTEM);
        clusterEventBean = BeaconEvents.createEventsBean(Events.SUBMITTED, EventEntityType.CLUSTER, createCluster());

        policyEventBean = BeaconEvents.createEventsBean(Events.SUBMITTED, EventEntityType.POLICY,
                createPolicyBean(), getEventInfo());

        policyInstanceEventBean = BeaconEvents.createEventsBean(Events.SUCCEEDED, EventEntityType.POLICYINSTANCE,
                createPolicyInstanceBean());

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
        Assert.assertEquals(systemEventBean.getEventMessage(), actual.getEventMessage());
        Assert.assertEquals(systemEventBean.getEventSeverity(), actual.getEventSeverity());
        Assert.assertEquals(systemEventBean.getEventTimeStamp(), actual.getEventTimeStamp());
    }

    @Test
    public void testClusterEvents() throws BeaconException {
        EventBean actual = executor.addEvents(clusterEventBean);

        Assert.assertEquals(clusterEventBean, actual);

        Assert.assertEquals(clusterEventBean.getPolicyId(), actual.getPolicyId());
        Assert.assertEquals(clusterEventBean.getInstanceId(), actual.getInstanceId());
        Assert.assertEquals(clusterEventBean.getEventId(), actual.getEventId());
        Assert.assertEquals(clusterEventBean.getEventMessage(), actual.getEventMessage());
        Assert.assertEquals(clusterEventBean.getEventSeverity(), actual.getEventSeverity());
        Assert.assertEquals(clusterEventBean.getEventTimeStamp(), actual.getEventTimeStamp());
    }

    @Test
    public void testPolicyEvents() throws BeaconException {
        EventBean actual = executor.addEvents(policyEventBean);

        Assert.assertEquals(policyEventBean, actual);

        Assert.assertEquals(policyEventBean.getPolicyId(), actual.getPolicyId());
        Assert.assertEquals(policyEventBean.getInstanceId(), actual.getInstanceId());
        Assert.assertEquals(policyEventBean.getEventId(), actual.getEventId());
        Assert.assertEquals(policyEventBean.getEventMessage(), actual.getEventMessage());
        Assert.assertEquals(policyEventBean.getEventSeverity(), actual.getEventSeverity());
        Assert.assertEquals(policyEventBean.getEventTimeStamp(), actual.getEventTimeStamp());
        Assert.assertEquals(policyEventBean.getEventInfo(), actual.getEventInfo());
    }

    @Test
    public void testPolicyInstanceEvents() throws BeaconException {
        EventBean actual = executor.addEvents(policyInstanceEventBean);

        Assert.assertEquals(policyInstanceEventBean, actual);

        Assert.assertEquals(policyInstanceEventBean.getPolicyId(), actual.getPolicyId());
        Assert.assertEquals(policyInstanceEventBean.getInstanceId(), actual.getInstanceId());
        Assert.assertEquals(policyInstanceEventBean.getEventId(), actual.getEventId());
        Assert.assertEquals(policyInstanceEventBean.getEventMessage(), actual.getEventMessage());
        Assert.assertEquals(policyInstanceEventBean.getEventSeverity(), actual.getEventSeverity());
        Assert.assertEquals(policyInstanceEventBean.getEventTimeStamp(), actual.getEventTimeStamp());
    }

    @Test
    public void testEventInfo() {
        String jsonString = createEventInfo().toJsonString();
        Assert.assertNotNull(jsonString);

        EventInfo eventInfo = EventInfo.getEventInfo(jsonString);
        Assert.assertEquals(eventInfo.getSourceCluster(), SOURCECLUSTER);
        Assert.assertEquals(eventInfo.getTargetCluster(), TARGETCLUSTER);
        Assert.assertEquals(eventInfo.getSourceDataset(), SOURCEDATASET);
    }

    private Cluster createCluster() {
        return new Cluster(new Cluster.Builder(SOURCECLUSTER, "source test cluster",
                "http://localhost:25968")
                .fsEndpoint("hdfs://localhost:8020")
                .peers(TARGETCLUSTER));
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

    private EventInfo createEventInfo() {
        EventInfo eventInfo = new EventInfo();
        eventInfo.updateEventsInfo(SOURCECLUSTER, TARGETCLUSTER, SOURCEDATASET, false);
        return eventInfo;
    }

    private EventInfo getEventInfo() {
        EventInfo eventInfo = new EventInfo();
        eventInfo.updateEventsInfo("source", "target",
                "testdataset", false);
        return eventInfo;
    }
}
