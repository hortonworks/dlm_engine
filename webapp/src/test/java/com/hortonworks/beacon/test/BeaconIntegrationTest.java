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

package com.hortonworks.beacon.test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.BeaconWebClient;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.replication.fs.MiniHDFSClusterUtil;

/**
 * Base class for setup and teardown IT test cluster.
 */
public class BeaconIntegrationTest {

    protected static final String SOURCE_CLUSTER = "dc$source-cluster";
    protected static final String TARGET_CLUSTER = "target-cluster";
    protected static final String OTHER_CLUSTER = "dc$other-cluster";
    private static List<String> sourceJVMOptions = new ArrayList<>();
    private static List<String> targetJVMOptions = new ArrayList<>();
    protected static final int SOURCE_PORT = 8021;
    protected static final int TARGET_PORT = 8022;

    static {
        String commonOptions = "-Dlog4j.configuration=beacon-log4j.xml -Dbeacon.version="
                + System.getProperty(BeaconConstants.BEACON_VERSION_CONST)
                + " -Dbeacon.log.appender=FILE";
        sourceJVMOptions.add(commonOptions + " -Dbeacon.log.filename=beacon-application.log." + SOURCE_CLUSTER);

        targetJVMOptions.add(commonOptions + " -Dbeacon.log.filename=beacon-application.log." + TARGET_CLUSTER);
    }

    private Process sourceCluster;
    private Process targetCluster;
    private Properties sourceProp;
    private Properties targetProp;

    protected BeaconClient sourceClient;
    protected BeaconClient targetClient;

    public BeaconIntegrationTest() throws IOException {
        sourceProp = BeaconTestUtil.getProperties("beacon-source-server.properties");
        targetProp = BeaconTestUtil.getProperties("beacon-target-server.properties");
    }

    @BeforeMethod
    public void setupBeaconServers(Method testMethod) throws Exception {
        String sourceExtraClassPath;
        String submitHAClusterTestName = "testSubmitHACluster";
        if (!testMethod.getName().equals(submitHAClusterTestName)) {
            sourceExtraClassPath = System.getProperty("user.dir") + "/src/test/resources/source/:";
        } else {
            sourceExtraClassPath = System.getProperty("user.dir") + "/src/test/resources/sourceHA/:";
        }
        sourceCluster = ProcessHelper.startNew(StringUtils.join(sourceJVMOptions, " "),
                EmbeddedBeaconServer.class.getName(), sourceExtraClassPath,
                new String[]{"beacon-source-server.properties"});

        targetCluster = ProcessHelper.startNew(StringUtils.join(targetJVMOptions, " "),
                EmbeddedBeaconServer.class.getName(), System.getProperty("user.dir")
                        + "/src/test/resources/tgt/:", new String[]{"beacon-target-server.properties"});

        sourceClient = new BeaconWebClient(getSourceBeaconServer());
        targetClient = new BeaconWebClient(getTargetBeaconServer());
    }

    @AfterMethod
    public void teardownBeaconServers() throws Exception {
        ProcessHelper.killProcess(sourceCluster);
        ProcessHelper.killProcess(targetCluster);
    }

    public String getSourceBeaconServer() {
        return "http://" + sourceProp.getProperty("beacon.host") + ":" + sourceProp.getProperty("beacon.port");
    }

    public String getTargetBeaconServer() {
        return "http://" + targetProp.getProperty("beacon.host") + ":" + targetProp.getProperty("beacon.port");
    }

    public String getTargetBeaconServerHostName() {
        return targetProp.getProperty("beacon.host");
    }

    /**
     * I am keeping the port option for potential future use.  For now all callers use 0 for this.
     * @param port
     * @param path
     * @return
     * @throws Exception
     */
    protected MiniDFSCluster startMiniHDFS(int port, String path) throws Exception {
        return MiniHDFSClusterUtil.initMiniDfs(port, new File(path));
    }

    protected void shutdownMiniHDFS(MiniDFSCluster dfsCluster) {
        if (dfsCluster != null) {
            dfsCluster.shutdown(true);
        }
    }
}
