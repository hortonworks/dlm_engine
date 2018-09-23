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

import com.hortonworks.beacon.plugin.DataSet;
import org.apache.atlas.entitytransform.BaseEntityHandler;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Test for ImportRequestProvider.
 */
public class ImportRequestProviderTest extends RequestProviderBase {

    private static final String CLASSIFICATION =
            "{\"conditions\":{\"__entity\":\"topLevel: \"},"
                    + "\"action\":{\"__entity\":\"ADD_CLASSIFICATION: clSrc_replicated\"}}";

    private static final String REPLICATED_ATTR_CLEAR =
            "{\"action\":{\"__entity.replicatedTo\":\"CLEAR:\",\"__entity.replicatedFrom\":\"CLEAR:\"}}";

    private static final String HIVE_DB_CLUSTER_NAME_RENAME =
            "{\"conditions\":{\"hive_db.clusterName\":\"EQUALS: clSrc\"},"
                    + "\"action\":{\"hive_db.clusterName\":\"SET: clTgt\"}}";

    private static final String HIVE_DB_NAME_RENAME =
            "{\"conditions\":{\"hive_db.name\":\"EQUALS: stocks\"},"
                    + "\"action\":{\"hive_db.name\":\"SET: stocks_dw\"}}";

    private static final String HDFS_CLUSTER_NAME_RENAME =
            "{\"conditions\":{\"hdfs_path.clusterName\":\"EQUALS: clSrc\"},"
                    + "\"action\":{\"hdfs_path.clusterName\":\"SET: clTgt\"}}";

    private static final String HDFS_NAME_RENAME =
            "{\"conditions\":{\"hdfs_path.name\":\"EQUALS: /tmp/hr/\"},"
                    + "\"action\":{\"hdfs_path.name\":\"SET: /tmp/hr_dw/\"}}";

    private static final String HDFS_NAME_RENAME_URI =
            "{\"conditions\":{\"hdfs_path.name\":\"EQUALS: hdfs://serverSrc:8020/tmp/hr/\"},"
                    + "\"action\":{\"hdfs_path.name\":\"SET: hdfs://serverTgt:8020/tmp/hr_dw/\"}}";

    private static final String HIVE_SOURCE_STOCKS = "stocks";
    private static final String HIVE_SOURCE_STOCKS_DW = "stocks_dw";

    private static final String HDFS_SOURCE_HR = "/tmp/hr";
    private static final String HDFS_SOURCE_HR_DW = "/tmp/hr_dw";

    @Test
    public void hiveDBClusterRename() {
        String[] parts = { CLASSIFICATION, REPLICATED_ATTR_CLEAR, HIVE_DB_CLUSTER_NAME_RENAME };

        assertTransform(4,
                DataSet.DataSetType.HIVE, parts, HIVE_SOURCE_STOCKS, HIVE_SOURCE_STOCKS);
    }

    @Test
    public void hiveDBWithRename() {
        String[] parts = { CLASSIFICATION, REPLICATED_ATTR_CLEAR, HIVE_DB_CLUSTER_NAME_RENAME, HIVE_DB_NAME_RENAME };

        assertTransform(4,
                DataSet.DataSetType.HIVE, parts, HIVE_SOURCE_STOCKS, HIVE_SOURCE_STOCKS_DW);
    }

    @Test
    public void hdfsClusterRename() {
        String[] parts = { CLASSIFICATION, REPLICATED_ATTR_CLEAR, HDFS_CLUSTER_NAME_RENAME};

        assertTransform(1, DataSet.DataSetType.HDFS, parts, HDFS_SOURCE_HR, HDFS_SOURCE_HR);
    }

    @Test
    public void hdfsRename() {
        String[] parts = { CLASSIFICATION, REPLICATED_ATTR_CLEAR, HDFS_CLUSTER_NAME_RENAME, HDFS_NAME_RENAME };

        assertTransform(1,
                DataSet.DataSetType.HDFS, parts, HDFS_SOURCE_HR, HDFS_SOURCE_HR_DW);
    }

    @Test
    public void hdfsRenameWithUri() {
        String[] parts = { CLASSIFICATION, REPLICATED_ATTR_CLEAR, HDFS_CLUSTER_NAME_RENAME, HDFS_NAME_RENAME_URI };

        assertTransform(1,
                DataSet.DataSetType.HDFS, parts,
                SOURCE_FS_URI + HDFS_SOURCE_HR,
                TARGET_FS_URI + HDFS_SOURCE_HR_DW);
    }

    private void assertTransform(int expectedTransformCount, DataSet.DataSetType dataSetType, String[] jsonParts,
                                 String sourceDataSetName, String targetDataSetName) {
        String expectedTransformJSON = composeJson(jsonParts);

        DataSet dataSet = getDataSet(dataSetType, sourceDataSetName, targetDataSetName, false);
        AtlasImportRequest request = ImportRequestProvider.create(dataSet.getType(),
                dataSet.getSourceDataSet(),
                dataSet.getTargetDataSet(),
                SOURCE_CLUSTER_NAME, TARGET_CLUSTER_NAME, "", "", "");

        String actualTransformsJSON = request.getOptions().get(AtlasImportRequest.TRANSFORMERS_KEY);
        assertNotNull(actualTransformsJSON);

        List<BaseEntityHandler> baseEntityHandlerList = BaseEntityHandler.fromJson(
                actualTransformsJSON, null);

        assertNotNull(baseEntityHandlerList);
        assertEquals(baseEntityHandlerList.size(), expectedTransformCount);
        assertEquals(actualTransformsJSON, expectedTransformJSON);
        assertJsonParts(jsonParts, actualTransformsJSON);
    }

    private void assertJsonParts(String[] jsonParts, String actualTransformsJSON) {
        for (String s : jsonParts) {
            assertTrue(actualTransformsJSON.contains(s));
        }
    }

    private String composeJson(String... jsonParts) {
        return "[" + StringUtils.join(jsonParts, ',') + "]";
    }
}
