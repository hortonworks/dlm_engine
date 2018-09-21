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
package com.hortonworks.beacon.plugin.atlas;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.plugin.DataSet;
import com.hortonworks.beacon.util.FileSystemClientFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * Integration test for BeaconAtlasPlugin.
 */
public class BeaconAtlasPluginTest extends RequestProviderBase {
    private static final String LOCAL_HOST_URL = "http://localhost:";
    private static final String LOCAL_HOST_FS_URL = "http://localhost:8020";

    private DistributedFileSystem dfs;

    private BeaconAtlasPlugin beaconAtlasPlugin;
    private Path actualExportedPath = null;
    private AtlasMockRESTClient.Builder clientBuilder;

    @BeforeClass
    public void setup() {
        setupResourcesDir();
    }

    private void setupSingle() throws IOException, URISyntaxException {
        dfs = mock(DistributedFileSystem.class);
        FileSystemClientFactory.setFileSystem(dfs);
        when(dfs.create((Path) anyObject())).thenReturn(mock(FSDataOutputStream.class));
        when(dfs.getFileStatus((Path) anyObject())).thenReturn(new FileStatus());
        when(dfs.getUri()).thenReturn(new URI(LOCAL_HOST_FS_URL));

        InputStream inputStream = getSeekableByteArrayInputStream(getZipFilePath());
        when(dfs.open((Path) anyObject())).thenReturn(new FSDataInputStream(inputStream));

        clientBuilder = new AtlasMockRESTClient.Builder();
        clientBuilder.setFilePath(getZipFilePath());
        beaconAtlasPlugin = new BeaconAtlasPlugin(clientBuilder);
        beaconAtlasPlugin.register();
    }

    private String getZipFilePath() {
        return getFileFromResources("stocks.zip");
    }

    private String getZipFilePathForEmpty() {
        return getFileFromResources("stocks-empty.zip");
    }

    @Test
    public void syncData() throws Exception {
        setupSingle();

        DataSet dataSet = getDataSet(false);
        actualExportedPath = beaconAtlasPlugin.exportData(dataSet);

        AtlasPluginStats pluginStats = (AtlasPluginStats) beaconAtlasPlugin.getStats();
        assertNotNull(pluginStats);
        assertNotNull(actualExportedPath);

        beaconAtlasPlugin.importData(dataSet, actualExportedPath);
        pluginStats = (AtlasPluginStats) beaconAtlasPlugin.getStats();
        assertNotNull(pluginStats);
        assertEquals(pluginStats.getExportStats(), pluginStats.getImportStats());
    }

    @Test
    public void syncDataForNonExistentEntity() throws Exception {
        setupSingle();
        clientBuilder.returnEmptyGuid();

        DataSet dataSet = getDataSet(true);
        Path actualPath = beaconAtlasPlugin.exportData(dataSet);
        assertNull(actualPath);

        beaconAtlasPlugin.importData(dataSet, actualExportedPath);
    }

    @Test(expectedExceptions = BeaconException.class)
    public void emptyFileExported() throws IOException, BeaconException, URISyntaxException {
        setupSingle();
        clientBuilder.setFilePath(getZipFilePathForEmpty());

        DataSet dataSet = getDataSet(false);
        actualExportedPath = beaconAtlasPlugin.exportData(dataSet);
        assertNotNull(actualExportedPath);

        beaconAtlasPlugin.importData(dataSet, actualExportedPath);
    }

    private DataSet getDataSet(final boolean nonExistentEntity) {
        return new DataSet() {
            private static final String SOURCE_CLUSTER_NAME = "cl1";
            private static final String TARGET_CLUSTER_NAME = "cl2";
            private static final String DATASET_NAME = "stocks";

            private static final String STATING_PATH = "/staging";
            private static final String TARGET_PORT = "31000";
            private static final String SOURCE_PORT = "21000";

            @Override
            public DataSetType getType() {
                return DataSetType.HIVE;
            }

            @Override
            public String getSourceDataSet() {
                return DATASET_NAME + ((nonExistentEntity) ? "11111" : "");
            }

            @Override
            public String getTargetDataSet() {
                return DATASET_NAME;
            }

            @Override
            public Cluster getSourceCluster() {
                return getCluster(SOURCE_CLUSTER_NAME, SOURCE_PORT);
            }

            @Override
            public Cluster getTargetCluster() {
                return getCluster(TARGET_CLUSTER_NAME, TARGET_PORT);
            }

            @Override
            public String getStagingPath() {
                return STATING_PATH;
            }
        };
    }

    private Cluster getCluster(String name, final String port) {
        final Properties p = new Properties();
        p.setProperty("allowPluginsOnThisCluster", "true");
        p.setProperty("name", name);

        final Cluster.Builder b;
        b = new Cluster.Builder(name, name, "0000").fsEndpoint("file:///");
        b.customProperties(p).atlasEndpoint(LOCAL_HOST_URL + port);
        return new Cluster(b);
    }

    private FSDataInputStream getSeekableByteArrayInputStream(String fileName) throws IOException {
        File file = new File(fileName);
        InputStream in = new FileInputStream(file);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte [] buf = new byte[1024];
        int read;

        while ((read = in.read(buf)) > 0) {
            baos.write(buf, 0, read);
        }

        byte [] data = baos.toByteArray();
        SeekableInputStream bais = new SeekableInputStream(data);
        return new FSDataInputStream(bais);
    }
}
