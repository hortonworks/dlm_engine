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
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Performs Atlas' Export.
 */
public class ExportProcess extends AtlasProcess {
    private static final String ATLAS_EXPORTED_FILE_NAME_TEMPLATE = "atlas-export-%s-%s.zip";
    private static final String QUALIFIED_NAME_GUID_MAP_KEY_FORMAT = "%s:%s:%s";

    private final Map<String, String> qualifiedNameGuidMap;

    public ExportProcess(RESTClientBuilder builder) {
        super(builder);
        qualifiedNameGuidMap = new HashMap<>();
    }

    public Path run(DataSet dataset, Path stagingDir, AtlasPluginStats pluginStats) throws BeaconException {
        debugDatasetLog(dataset);
        infoLog("==> ExportProcess.run: Starting...");
        Path exportPath = null;
        try {
            Cluster sourceCluster = dataset.getSourceCluster();
            Cluster targetCluster = dataset.getTargetCluster();
            String exportFileName = getExportFileName(targetCluster.getName(), getCurrentTimestamp());

            AtlasExportRequest exportRequest = ExportRequestProvider.create(this, dataset);

            InputStream inputStream = exportData(sourceCluster, exportRequest);
            exportPath = writeDataToFile(targetCluster, stagingDir, exportFileName, inputStream);

            return exportPath;
        } catch (Exception ex) {
            LOG.error("ExportProcess", ex);
            throw new BeaconException(ex);
        } finally {
            infoLog("<== ExportProcess.run: {}: Done!",
                    (exportPath != null) ? exportPath.toString() : "");
        }
    }

    protected InputStream exportData(Cluster cluster, AtlasExportRequest request) throws BeaconException {
        return getClient(cluster).exportData(request);
    }

    private Path writeDataToFile(Cluster clusterToWriteTo, Path stagingDir,
                                 String exportFileName, InputStream data) throws IOException, BeaconException {
        FileSystem fs = FileSystemUtils.getFs(clusterToWriteTo);
        Path exportedFile = new Path(stagingDir, exportFileName);
        long numBytesWritten = FileSystemUtils.writeFile(fs, exportedFile, data);

        updateExportStats(numBytesWritten);
        return new Path(clusterToWriteTo.getFsEndpoint(), exportedFile);
    }

    private String getExportFileName(String clusterName, String suffix) {
        String s = String.format(ATLAS_EXPORTED_FILE_NAME_TEMPLATE, clusterName, suffix);
        debugLog("getExportFileName: {}", s);
        return s;
    }

    private String getCurrentTimestamp() {
        return Long.toString(System.currentTimeMillis());
    }

    private void updateExportStats(long numBytesWritten) {
        updateStats(AtlasPluginStats.EXPORT_KEY, numBytesWritten);
    }

    @Override
    public String getEntityGuid(Cluster cluster, String typeName, String attributeName, String attributeValue) {
        String key = String.format(QUALIFIED_NAME_GUID_MAP_KEY_FORMAT, typeName, attributeName, attributeValue);

        if (qualifiedNameGuidMap.containsKey(key)) {
            return qualifiedNameGuidMap.get(key);
        }

        String guid = super.getEntityGuid(cluster, typeName, attributeName, attributeValue);

        LOG.info("getEntityGuid: {}", guid);

        if (StringUtils.isNotEmpty(guid)) {
            qualifiedNameGuidMap.put(key, guid);
        }

        return guid;
    }
}
