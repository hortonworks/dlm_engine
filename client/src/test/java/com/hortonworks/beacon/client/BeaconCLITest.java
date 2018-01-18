/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.client;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.hortonworks.beacon.client.cli.BeaconCLI;
import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;

/**
 * Test for beacon cli, uses mocks.
 */
public class BeaconCLITest {

    @Mock
    private BeaconClient beaconClient;
    private BeaconCLI cli;

    @BeforeClass
    public void setup() {
        MockitoAnnotations.initMocks(this);
        cli = new BeaconCLI(beaconClient);
    }

    @Test
    public void testAdminOperations() throws Exception {
        cli.processCommand("beacon -status".split(" "));
        verify(beaconClient).getServiceStatus();

        cli.processCommand("beacon -version".split(" "));
        verify(beaconClient).getServiceVersion();
    }

    @Test
    public void testClusterOperations() throws Exception {
        cli.processCommand("-cluster src -submit -config file".split(" "));
        verify(beaconClient).submitCluster("src", "file");

        cli.processCommand("-cluster -list".split(" "));
        verify(beaconClient).getClusterList("name", "name", "ASC", 0, 10);

        when(beaconClient.getClusterStatus("src")).thenReturn(Entity.EntityStatus.SUBMITTED);
        cli.processCommand("-cluster src -status".split(" "));
        verify(beaconClient).getClusterStatus("src");

        cli.processCommand("-cluster src -pair".split(" "));
        verify(beaconClient).pairClusters("src", false);

        cli.processCommand("-cluster src -delete".split(" "));
        verify(beaconClient).deleteCluster("src");

        cli.processCommand("-cluster -help".split(" "));
    }

    @Test
    public void testPolicyCommands() throws Exception {
        cli.processCommand("-policy firstpolicy -submitSchedule -config file".split(" "));
        verify(beaconClient).submitAndScheduleReplicationPolicy("firstpolicy", "file");

        cli.processCommand("-policy -list".split(" "));
        verify(beaconClient).getPolicyList("name", "name", null, "ASC", 0, 10);

        when(beaconClient.getPolicyStatus("firstpolicy")).thenReturn(Entity.EntityStatus.SUBMITTED);
        cli.processCommand("-policy firstpolicy -status".split(" "));
        verify(beaconClient).getPolicyStatus("firstpolicy");

        cli.processCommand("-policy firstpolicy -delete".split(" "));
        verify(beaconClient).deletePolicy("firstpolicy", false);

        PolicyInstanceList listResult = getRandomInstanceList();
        when(beaconClient.listPolicyInstances("firstpolicy")).thenReturn(listResult);
        cli.processCommand("-policy firstpolicy -instancelist".split(" "));
        verify(beaconClient).listPolicyInstances("firstpolicy");

        cli.processCommand("-policy -help".split(" "));
    }

    @Test
    public void testResourceCommands() throws Exception {
        cli.processCommand("-dataset -listfs /".split(" "));
        verify(beaconClient).listFiles("/");

        cli.processCommand("-dataset -listdb".split(" "));
        verify(beaconClient).listFiles("/");
    }

    public PolicyInstanceList getRandomInstanceList() {
        PolicyInstanceList randomInstanceList =
                new PolicyInstanceList(new ArrayList<PolicyInstanceList.InstanceElement>(), 10);
        return randomInstanceList;
    }
}
