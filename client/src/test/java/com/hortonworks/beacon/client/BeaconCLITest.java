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
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.client.resource.StatusResult;

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
        when(beaconClient.getStatus()).thenReturn("RUNNING");
        cli.processCommand("beacon -status".split(" "));
        verify(beaconClient).getStatus();

        when(beaconClient.getVersion()).thenReturn("0.1");
        cli.processCommand("beacon -version".split(" "));
        verify(beaconClient).getVersion();
    }

    @Test
    public void testClusterOperations() {
        APIResult apiResult = getRandomAPIResult();
        when(beaconClient.submitCluster("src", "file")).thenReturn(apiResult);
        cli.processCommand("-cluster src -submit -config file".split(" "));
        verify(beaconClient).submitCluster("src", "file");

        cli.processCommand("-cluster -list".split(" "));
        verify(beaconClient).getClusterList("name", "name", "ASC", 0, 10);

        StatusResult statusResult = getRandomStatusResult();
        when(beaconClient.getClusterStatus("src")).thenReturn(statusResult);
        cli.processCommand("-cluster src -status".split(" "));
        verify(beaconClient).getClusterStatus("src");

        when(beaconClient.pairClusters("src", false)).thenReturn(apiResult);
        cli.processCommand("-cluster src -pair".split(" "));
        verify(beaconClient).pairClusters("src", false);

        when(beaconClient.deleteCluster("src")).thenReturn(apiResult);
        cli.processCommand("-cluster src -delete".split(" "));
        verify(beaconClient).deleteCluster("src");

        cli.processCommand("-cluster -help".split(" "));
    }

    @Test
    public void testPolicyCommands() {
        APIResult apiResult = getRandomAPIResult();

        when(beaconClient.submitAndScheduleReplicationPolicy("firstpolicy", "file")).thenReturn(apiResult);
        cli.processCommand("-policy firstpolicy -submitSchedule -config file".split(" "));
        verify(beaconClient).submitAndScheduleReplicationPolicy("firstpolicy", "file");

        cli.processCommand("-policy -list".split(" "));
        verify(beaconClient).getPolicyList("name", "name", null, "ASC", 0, 10);

        StatusResult statusResult = getRandomStatusResult();
        when(beaconClient.getPolicyStatus("firstpolicy")).thenReturn(statusResult);
        cli.processCommand("-policy firstpolicy -status".split(" "));
        verify(beaconClient).getPolicyStatus("firstpolicy");

        when(beaconClient.deletePolicy("firstpolicy", false)).thenReturn(apiResult);
        cli.processCommand("-policy firstpolicy -delete".split(" "));
        verify(beaconClient).deletePolicy("firstpolicy", false);

        PolicyInstanceList listResult = getRandomInstanceList();
        when(beaconClient.listPolicyInstances("firstpolicy")).thenReturn(listResult);
        cli.processCommand("-policy firstpolicy -instancelist".split(" "));
        verify(beaconClient).listPolicyInstances("firstpolicy");

        cli.processCommand("-policy -help".split(" "));
    }

    public APIResult getRandomAPIResult() {
        APIResult randomAPIResult = new APIResult(APIResult.Status.SUCCEEDED, "message");
        return randomAPIResult;
    }

    public StatusResult getRandomStatusResult() {
        StatusResult randomResult = new StatusResult("name", "RUNNING");
        return randomResult;
    }

    public PolicyInstanceList getRandomInstanceList() {
        PolicyInstanceList randomInstanceList =
                new PolicyInstanceList(new ArrayList<PolicyInstanceList.InstanceElement>(), 10);
        return randomInstanceList;
    }
}
