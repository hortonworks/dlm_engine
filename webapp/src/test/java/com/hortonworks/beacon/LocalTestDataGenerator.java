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

package com.hortonworks.beacon;

import com.amazonaws.services.s3.AmazonS3Client;
import com.hortonworks.beacon.api.HdfsAdminFactory;
import com.hortonworks.beacon.api.LocalBeaconClient;
import com.hortonworks.beacon.api.ResourceBaseTest;
import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.S3Operation;
import com.hortonworks.beacon.entity.S3OperationFactory;
import com.hortonworks.beacon.entity.util.hive.HiveClientFactory;
import com.hortonworks.beacon.entity.util.hive.HiveMetadataClient;
import com.hortonworks.beacon.entity.util.hive.HiveServerClient;
import com.hortonworks.beacon.metrics.FSReplicationMetrics;
import com.hortonworks.beacon.replication.fs.DistCpFactory;
import com.hortonworks.beacon.scheduler.quartz.BeaconQuartzScheduler;
import com.hortonworks.beacon.service.BeaconStoreService;
import com.hortonworks.beacon.service.ServiceManager;
import com.hortonworks.beacon.tools.BeaconDBSetup;
import com.hortonworks.beacon.util.FileSystemClientFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.EncryptionZoneIterator;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.tools.DistCp;
import org.apache.hive.jdbc.HiveStatement;
import org.mockito.Matchers;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test data generator for local testing.
 */
public class LocalTestDataGenerator extends TestDataGenerator {

    @Override
    public void init() throws Exception {
        MockitoAnnotations.initMocks(this);
        BeaconDBSetup.setupDB();
        List<String> defaultServices = Arrays.asList(BeaconStoreService.class.getName());
        List<String> dependentServices = Arrays.asList(BeaconQuartzScheduler.class.getName());
        ServiceManager.getInstance().initialize(defaultServices, dependentServices);
        initCustomMocks();

    }

    private void initCustomMocks() throws Exception {
        targetFs = mock(DistributedFileSystem.class);
        sourceFs = mock(DistributedFileSystem.class);
        targetBeaconClient = new LocalBeaconClient();
        localBeaconClient = mock(LocalBeaconClient.class);
        FileSystemClientFactory.setFileSystem(targetFs);
        hiveMetadataClient = mock(HiveMetadataClient.class);
        HiveServerClient hiveServerClient = mock(HiveServerClient.class);
        HiveClientFactory.setHiveMetadataClient(hiveMetadataClient);
        HiveClientFactory.setHiveServerClient(hiveServerClient);
        BeaconClient sourceClient = mock(LocalBeaconClient.class);
        BeaconClientFactory.setBeaconClient(sourceClient);
        HdfsAdmin hdfsAdmin = mock(HdfsAdmin.class);
        HdfsAdminFactory.setHdfsAdmin(hdfsAdmin);
        RemoteIterator<EncryptionZone> remoteIterator = mock(EncryptionZoneIterator.class);
        when(remoteIterator.hasNext()).thenReturn(false);
        when(hdfsAdmin.listEncryptionZones()).thenReturn(remoteIterator);
        DistCp distCp = mock(DistCp.class);
        final Job job = mock(Job.class);
        when(distCp.createAndSubmitJob()).thenReturn(job);
        when(job.isComplete()).then(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                return true;
            }
        });
        when(job.getJobState()).thenReturn(JobStatus.State.RUNNING).thenReturn(JobStatus.State.SUCCEEDED);
        final boolean[] killed = {false};
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                killed[0] = true;
                return null;
            }
        }).when(job).killJob();
        when(job.isSuccessful()).then(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                return !killed[0];
            }
        });
        when(job.isSuccessful()).thenReturn(true);
        org.apache.hadoop.mapreduce.JobStatus jobStatus = mock(org.apache.hadoop.mapreduce.JobStatus.class);
        CounterGroup counterGroup = mock(CounterGroup.class);
        Counters counters = mock(Counters.class);
        when(job.getCounters()).thenReturn(counters);
        when(job.getCounters().getGroup(FSReplicationMetrics.COUNTER_GROUP)).thenReturn(counterGroup);
        Iterator iterator = mock(Iterator.class);
        when(counterGroup.iterator()).thenReturn(iterator);
        when(counterGroup.iterator().hasNext()).thenReturn(false);
        when(job.getCounters().getGroup(FSReplicationMetrics.JOB_COUNTER_GROUP)).thenReturn(null);
        when(job.getStatus()).thenReturn(jobStatus);
        when(job.getStatus().getMapProgress()).thenReturn(1f);
        TaskReport taskReport = mock(TaskReport.class);
        TaskReport[] taskReports = new TaskReport[1];
        taskReports[0] = taskReport;
        when(job.getTaskReports(TaskType.MAP)).thenReturn(taskReports);
        DistCpFactory.setDistCp(distCp);
        when(hiveMetadataClient.getDatabaseLocation(Matchers.anyString()))
                .thenReturn(new Path(getRandomString("hive")));
        Statement statement = mock(HiveStatement.class);
        when(hiveServerClient.createStatement()).thenReturn(statement);
        final ResultSet resultSet = mock(ResultSet.class);
        when(statement.executeQuery(Matchers.anyString())).thenAnswer(new Answer<ResultSet>() {
            @Override
            public ResultSet answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(100);
                return resultSet;
            }
        });
        /**
         * Amazon s3 client mocks for cloud replication
         */
        System.setProperty("beacon.hive.username", System.getProperty("user.name"));
        String cwd = System.getProperty("user.dir");
        BeaconConfig.getInstance().getEngine().setCloudCredProviderPath("jceks://file/" + cwd + "/target/credential/");
        AmazonS3Client amazonS3Client = mock(AmazonS3Client.class);
        S3Operation s3Operation = new S3Operation(amazonS3Client);
        S3OperationFactory.setS3Operation(s3Operation);
        when(amazonS3Client.getBucketLocation(Matchers.anyString())).thenReturn("US");
    }

    @Override
    public Cluster getCluster(ResourceBaseTest.ClusterType clusterType, boolean isLocal) {
        Cluster cluster = new Cluster();
        cluster.setLocal(isLocal);
        cluster.setName(clusterType.getClusterName(isLocal));
        cluster.setDescription(getRandomString("description"));
        if (isLocal) {
            cluster.setFsEndpoint("file:///");
        } else {
            cluster.setFsEndpoint("hdfs://local-" + clusterType +"/");
        }
        cluster.setHsEndpoint("jdbc:hive2://local-" + clusterType);
        cluster.setBeaconEndpoint("http://beacon-" + cluster);
        cluster.setTags(Arrays.asList("test", "local", "IT"));
        Properties properties = new Properties();
        properties.put("testKey", "testVal");
        cluster.setCustomProperties(properties);
        return cluster;
    }

    @Override
    public BeaconClient getClient(ResourceBaseTest.ClusterType clusterType) {
        switch (clusterType) {
            case SOURCE:
                return localBeaconClient;
            case TARGET:
                return targetBeaconClient;
            default:
                throw new IllegalStateException("Unhandled cluster type " + clusterType);
        }
    }

    @Override
    public FileSystem getFileSystem(ResourceBaseTest.ClusterType clusterType) {
        switch (clusterType) {
            case SOURCE:
                return sourceFs;
            case TARGET:
                return targetFs;
            default:
                throw new IllegalStateException("Unhandled cluster type " + clusterType);
        }
    }

    @Override
    public void createFSMocks(String path) throws IOException {
        when(targetFs.exists(any(Path.class))).thenReturn(true);
        when(targetFs.create(new Path(path))).thenReturn(mock(FSDataOutputStream.class));
    }
}
