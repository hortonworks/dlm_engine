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

package com.hortonworks.beacon.client;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import java.util.ArrayList;

import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.util.CloudCredBuilder;
import com.hortonworks.beacon.util.PropertiesIgnoreCase;
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

        cli.processCommand("-cluster src -unpair".split(" "));
        verify(beaconClient).unpairClusters("src", false);

        cli.processCommand("-cluster src -delete".split(" "));
        verify(beaconClient).deleteCluster("src");

        cli.processCommand("-cluster src -update -config file".split(" "));
        verify(beaconClient).updateCluster("src", "file");

        cli.processCommand("-cluster -help".split(" "));
    }

    @Test
    public void testPolicyCommands() throws Exception {
        cli.processCommand("-policy firstpolicy -submitSchedule -config file".split(" "));
        verify(beaconClient).submitAndScheduleReplicationPolicy("firstpolicy", "file");

        cli.processCommand("-policy firstpolicy -dryrun -config file".split(" "));
        verify(beaconClient).dryrunPolicy("firstpolicy", "file");

        cli.processCommand("-policy -list".split(" "));
        verify(beaconClient).getPolicyList("name", "name", null, "ASC", 0, 10);

        when(beaconClient.getPolicyStatus("firstpolicy")).thenReturn(Entity.EntityStatus.SUBMITTED);
        cli.processCommand("-policy firstpolicy -status".split(" "));
        verify(beaconClient).getPolicyStatus("firstpolicy");

        cli.processCommand("-policy firstpolicy -delete".split(" "));
        verify(beaconClient).deletePolicy("firstpolicy", false);

        cli.processCommand("-policy firstpolicy -abort".split(" "));
        verify(beaconClient).abortPolicyInstance("firstpolicy");

        PolicyInstanceList listResult = getRandomInstanceList();
        when(beaconClient.listPolicyInstances("firstpolicy")).thenReturn(listResult);
        cli.processCommand("-policy firstpolicy -instancelist".split(" "));
        verify(beaconClient).listPolicyInstances("firstpolicy");

        cli.processCommand("-policy -help".split(" "));
    }

    @Test
    public void testCloudCredCommands() throws Exception {
        File configFile = getTempCloudCredConfigFile();
        String cmdStr = "-cloudcred -submit -config " + configFile.getAbsolutePath();
        PropertiesIgnoreCase propertiesIgnoreCase = new PropertiesIgnoreCase();
        FileInputStream fis = new FileInputStream(configFile);
        propertiesIgnoreCase.load(fis);
        fis.close();
        CloudCred cloudCred = CloudCredBuilder.buildCloudCred(propertiesIgnoreCase);

        cli.processCommand(cmdStr.split(" "));
        verify(beaconClient).submitCloudCred(cloudCred);

        cli.processCommand("-cloudcred -list".split(" "));
        verify(beaconClient).listCloudCred("name", "name", "ASC", 0,  10);

        cli.processCommand("-cloudcred someCloudCredID -delete".split(" "));
        verify(beaconClient).deleteCloudCred("someCloudCredID");

        cmdStr = "-cloudcred someCloudCredID -update -config " + configFile.getAbsolutePath();
        cli.processCommand(cmdStr.split(" "));
        verify(beaconClient).updateCloudCred("someCloudCredID", cloudCred);

        cli.processCommand("-cloudcred someCloudCredID -get".split(" "));
        verify(beaconClient).getCloudCred("someCloudCredID");

        cli.processCommand("-cloudcred someCloudCredID -validate -path cloudPath".split(" "));
        verify(beaconClient).validateCloudPath("someCloudCredID", "cloudPath");

        cli.processCommand("-cloudcred -help".split(" "));
    }

    @Test
    public void testResourceCommands() throws Exception {
        cli.processCommand("-listfs /".split(" "));
        verify(beaconClient).listFiles("/");

        cli.processCommand("-listdb".split(" "));
        verify(beaconClient).listFiles("/");

        cli.processCommand("-user".split(" "));
        verify(beaconClient).getUserPrivileges();
    }

    public PolicyInstanceList getRandomInstanceList() {
        PolicyInstanceList randomInstanceList =
                new PolicyInstanceList(new ArrayList<PolicyInstanceList.InstanceElement>(), 10);
        return randomInstanceList;
    }

    private File getTempCloudCredConfigFile() throws IOException {
        StringBuilder configContent = new StringBuilder();
        configContent.append("name=testCCname\n")
                     .append("provider=AWS\n")
                     .append("aws.access.key=testKey\n")
                     .append("aws.secret.key=testSecret\n")
                     .append("authtype=AWS_ACCESSKEY\n");
        File tmpConfigFile = File.createTempFile("beacon-", ".properties");
        Writer writer = new FileWriter(tmpConfigFile);
        BufferedWriter bufferedWriter = new BufferedWriter(writer);
        bufferedWriter.write(configContent.toString());
        bufferedWriter.close();
        tmpConfigFile.deleteOnExit();
        return tmpConfigFile;
    }
}
