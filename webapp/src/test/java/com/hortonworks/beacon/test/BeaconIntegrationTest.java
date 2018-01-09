/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
