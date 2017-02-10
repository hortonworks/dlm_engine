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

    protected static final String SOURCE_CLUSTER = "source-cluster";
    protected static final String TARGET_CLUSTER = "target-cluster";
    protected static final String OTHER_CLUSTER = "other-cluster";
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
        sourceJVMOptions.add("-Dorg.quartz.properties=quartz-source.properties");

        targetJVMOptions.add("-Dbeacon.log.dir=" + LOG_DIR + TARGET_CLUSTER);
        targetJVMOptions.add("-Dorg.quartz.properties=quartz-target.properties");

        otherJVMOptions.add("-Dbeacon.log.dir=" + LOG_DIR + OTHER_CLUSTER);
        otherJVMOptions.add("-Dorg.quartz.properties=quartz-other.properties");
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
    public void setupBeaconClusters() throws Exception {
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
    public void teardownCluster() throws Exception {
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
