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

import com.hortonworks.beacon.replication.fs.MiniHDFSClusterUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Base class for setup and teardown IT test cluster.
 */
public class BeaconIntegrationTest {

    protected static final String SOURCE_CLUSTER = "dc$source-cluster";
    protected static final String TARGET_CLUSTER = "target-cluster";
    protected static final String OTHER_CLUSTER = "dc$other-cluster";
    protected static String beaconTestBaseDir = System.getProperty("beacon.test.dir",
            System.getProperty("user.dir"));
    private static final String LOG_DIR;
    private static List<String> sourceJVMOptions = new ArrayList<>();
    private static List<String> targetJVMOptions = new ArrayList<>();
    private static List<String> otherJVMOptions = new ArrayList<>();

    static {
        beaconTestBaseDir = beaconTestBaseDir + "/target/";
        LOG_DIR = beaconTestBaseDir + "log/";
        System.setProperty("beacon.log.dir", LOG_DIR);

        sourceJVMOptions.add("-Dbeacon.log.dir=" + LOG_DIR + SOURCE_CLUSTER);

        targetJVMOptions.add("-Dbeacon.log.dir=" + LOG_DIR + TARGET_CLUSTER);

        otherJVMOptions.add("-Dbeacon.log.dir=" + LOG_DIR + OTHER_CLUSTER);
    }

    private Process sourceCluster;
    private Process targetCluster;
    private Process otherCluster;
    private Properties sourceProp;
    private Properties targetProp;
    private Properties otherProp;

    public BeaconIntegrationTest() throws IOException {
        sourceProp = BeaconTestUtil.getProperties("beacon-source-server.properties");
        targetProp = BeaconTestUtil.getProperties("beacon-target-server.properties");
        otherProp = BeaconTestUtil.getProperties("beacon-other-server.properties");
    }

    @BeforeMethod
    public void setupBeaconServers() throws Exception {
        sourceCluster = ProcessHelper.startNew(StringUtils.join(sourceJVMOptions, " "),
                EmbeddedBeaconServer.class.getName(),
                new String[]{"beacon-source-server.properties"});

        targetCluster = ProcessHelper.startNew(StringUtils.join(targetJVMOptions, " "),
                EmbeddedBeaconServer.class.getName(),
                new String[]{"beacon-target-server.properties"});

        otherCluster = ProcessHelper.startNew(StringUtils.join(otherJVMOptions, " "),
                EmbeddedBeaconServer.class.getName(),
                new String[]{"beacon-other-server.properties"});
    }

    @AfterMethod
    public void teardownBeaconServers() throws Exception {
        ProcessHelper.killProcess(sourceCluster);
        ProcessHelper.killProcess(targetCluster);
        ProcessHelper.killProcess(otherCluster);
    }

    public String getSourceBeaconServer() {
        return "http://" + sourceProp.getProperty("beacon.host") + ":" + sourceProp.getProperty("beacon.port");
    }

    public String getTargetBeaconServer() {
        return "http://" + targetProp.getProperty("beacon.host") + ":" + targetProp.getProperty("beacon.port");
    }

    public String getOtherBeaconServer() {
        return "http://" + otherProp.getProperty("beacon.host") + ":" + otherProp.getProperty("beacon.port");
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
