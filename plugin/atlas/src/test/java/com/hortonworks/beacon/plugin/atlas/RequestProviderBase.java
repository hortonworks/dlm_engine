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
import com.hortonworks.beacon.plugin.DataSet;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.Properties;

/**
 * Base class for all requests used in tests.
 */
public class RequestProviderBase {
    protected static final String ATLAS_RESOURCES_DIR = "/src/test/resources/atlas";
    protected static final String SOURCE_CLUSTER_NAME = "clSrc";
    protected static final String TARGET_CLUSTER_NAME = "clTgt";
    protected static final String SOURCE_FS_URI = "hdfs://serverSrc:8020";
    protected static final String TARGET_FS_URI = "hdfs://serverTgt:8020";

    protected AtlasProcess getMockProcess() {
        return new AtlasProcess(new AtlasMockRESTClient.Builder()) {
            @Override
            public Path run(DataSet dataset, Path stagingDir, AtlasPluginStats pluginStats) {
                return null;
            }
        };
    }

    protected DataSet getDataSet(final DataSet.DataSetType dataSetType,
                                 final String sourceDatasetName,
                                 final String targetDatasetName,
                                 final boolean targetIsNull) {
        return new DataSet() {
            @Override
            public DataSetType getType() {
                return dataSetType;
            }

            @Override
            public String getSourceDataSet() {
                return sourceDatasetName;
            }

            @Override
            public String getTargetDataSet() {
                return targetDatasetName;
            }

            @Override
            public Cluster getSourceCluster() {
                return getCluster(SOURCE_CLUSTER_NAME);
            }

            @Override
            public Cluster getTargetCluster() {
                return (targetIsNull) ? null : getCluster(TARGET_CLUSTER_NAME);
            }

            @Override
            public String getStagingPath() {
                return null;
            }
        };
    }

    protected Cluster getCluster(String name) {
        final Properties p = new Properties();

        final Cluster.Builder b;
        b = new Cluster.Builder(name, "default", "0000").fsEndpoint("dfs://default");
        b.customProperties(p).atlasEndpoint("http://localhost:" + "1010");
        return new Cluster(b);
    }

    protected void setupResourcesDir() {
        String currentDir = System.getProperty("user.dir");
        if (!currentDir.endsWith(ATLAS_RESOURCES_DIR)) {
            System.setProperty("user.dir", currentDir + File.separator + ATLAS_RESOURCES_DIR);
        }
    }

    protected String getFileFromResources(String fileName) {
        return System.getProperty("user.dir") + File.separator + fileName;
    }
}
