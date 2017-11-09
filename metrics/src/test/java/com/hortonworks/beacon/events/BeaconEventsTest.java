/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
        ServiceManager.getInstance().initialize(Arrays.asList(BeaconStoreService.SERVICE_NAME), null);
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
                "hdfs://localhost:8020", "http://localhost:25968")
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
