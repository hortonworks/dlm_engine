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

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.plugin.DataSet;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * Test for ExportRequestProvider.
 */
public class ExportRequestProviderTest extends RequestProviderBase {
    private static final String SOURCE_DB_NAME = "stocks";
    private static final String WAREHOUSE_ACCOUNTS_PATH = "/warehouse/accounts";
    private final long expectedTimestamp = 1534801666522L;

    @Test
    public void hiveRequest() throws BeaconException {
        AtlasExportRequest request = ExportRequestProvider.create(getMockProcess(),
                getDataSet(DataSet.DataSetType.HIVE, SOURCE_DB_NAME, false),
                AtlasMockRESTClient.DEFAULT_GUID);

        String expectedQualifiedName = String.format(
                ExportRequestProvider.QUALIFIED_NAME_FORMAT, SOURCE_DB_NAME, SOURCE_CLUSTER_NAME);

        assertHiveRequest(request, expectedQualifiedName, 4, expectedTimestamp);
    }

    @Test
    public void hiveRequestWithNullTargetCluster() throws BeaconException {
        AtlasExportRequest request = ExportRequestProvider.create(getMockProcess(),
                getDataSet(DataSet.DataSetType.HIVE, SOURCE_DB_NAME, true),
                AtlasMockRESTClient.DEFAULT_GUID);

        String expectedQualifiedName = String.format(
                ExportRequestProvider.QUALIFIED_NAME_FORMAT, SOURCE_DB_NAME, SOURCE_CLUSTER_NAME);

        assertHiveRequest(request, expectedQualifiedName, 3, 0L);
    }

    private void assertHiveRequest(AtlasExportRequest request, String expectedQualifiedName,
                                   int expectedOptionsCount, long timestamp) {
        assertNotNull(request);
        assertEquals(request.getItemsToExport().size(), 1);
        assertEquals(request.getItemsToExport().get(0).getUniqueAttributes().size(), 1);
        assertEquals(request.getItemsToExport().get(0).getTypeName(),
                ExportRequestProvider.ATLAS_TYPE_HIVE_DB);
        assertEquals(request.getItemsToExport().get(0).getUniqueAttributes().get(
                ExportRequestProvider.ATTRIBUTE_QUALIFIED_NAME),
                expectedQualifiedName);

        assertNotNull(request.getOptions());
        assertEquals(request.getOptions().size(), expectedOptionsCount);
        assertEquals(request.getOptions().get(AtlasExportRequest.FETCH_TYPE_INCREMENTAL_FROM_TIME), timestamp);
        assertEquals(request.getOptions().get(AtlasExportRequest.OPTION_SKIP_LINEAGE), true);
        assertNull(request.getOptions().get(AtlasExportRequest.OPTION_ATTR_MATCH_TYPE));

        if (expectedOptionsCount > 3) {
            assertEquals(request.getOptions().get(AtlasExportRequest.OPTION_KEY_REPLICATED_TO), TARGET_CLUSTER_NAME);
        }
    }

    @Test
    public void hdfsRequest() throws BeaconException {
        AtlasExportRequest request = ExportRequestProvider.create(getMockProcess(),
                getDataSet(DataSet.DataSetType.HDFS, WAREHOUSE_ACCOUNTS_PATH, false), AtlasMockRESTClient.DEFAULT_GUID);

        assertNotNull(request);
        assertEquals(request.getItemsToExport().size(), 1);
        assertEquals(request.getItemsToExport().get(0).getUniqueAttributes().size(), 1);
        assertEquals(request.getItemsToExport().get(0).getTypeName(),
                ExportRequestProvider.ATLAS_TYPE_HDFS_PATH);

        assertEquals(request.getItemsToExport().get(0).getUniqueAttributes().get(
                ExportRequestProvider.ATTRIBUTE_PATH_NAME),
                ExportRequestProvider.getPathWithTrailingPathSeparator(WAREHOUSE_ACCOUNTS_PATH));

        assertNotNull(request.getOptions());
        assertEquals(request.getOptions().size(), 5);
        assertEquals(request.getOptions().get(AtlasExportRequest.OPTION_KEY_REPLICATED_TO), TARGET_CLUSTER_NAME);
        assertEquals(request.getOptions().get(AtlasExportRequest.FETCH_TYPE_INCREMENTAL_FROM_TIME), expectedTimestamp);
        assertEquals(request.getOptions().get(AtlasExportRequest.OPTION_SKIP_LINEAGE), true);
        assertEquals(request.getOptions().get(AtlasExportRequest.OPTION_ATTR_MATCH_TYPE),
                AtlasExportRequest.MATCH_TYPE_STARTS_WITH);
    }
}
