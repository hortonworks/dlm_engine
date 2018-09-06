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
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

/**
 * Performs Atlas' Export.
 */
public class ExportProcess extends AtlasProcess {
    private static final String ATLAS_EXPORTED_FILE_NAME_TEMPLATE = "atlas-export-%s-%s.zip";

    public ExportProcess(RESTClientBuilder builder) {
        super(builder);
    }

    public Path run(DataSet dataset, Path stagingDir, AtlasPluginStats pluginStats) throws BeaconException {
        debugDatasetLog(dataset);
        infoLog("==> ExportProcess.run: Starting: {} ...", stagingDir);
        Path exportPath = null;
        try {
            Cluster sourceCluster = dataset.getSourceCluster();
            AtlasExportRequest exportRequest;
            String entityGuid = checkHiveEntityExists(sourceCluster, dataset);
            if (dataset.getType() == DataSet.DataSetType.HIVE && StringUtils.isEmpty(entityGuid)) {
                return null;
            }

            exportRequest = ExportRequestProvider.create(this, dataset, entityGuid);

            String exportFileName = getExportFileName(sourceCluster, getCurrentTimestamp());

            FileSystem targetFs = FileSystemClientFactory.get().createFileSystem(
                                                    stagingDir.getName(), new Configuration());

            InputStream inputStream = exportData(sourceCluster, exportRequest);
            exportPath = writeDataToFile(targetFs, stagingDir, exportFileName, inputStream);

            return exportPath;
        } catch (Exception ex) {
            infoLog("ExportProcess: failed! - {}", ex.getMessage());
            return null;
        } finally {
            infoLog("<== ExportProcess.run: {} - {}: Done!", stagingDir,
                    (exportPath != null) ? exportPath.toString() : "");
        }
    }

    private String checkHiveEntityExists(Cluster cluster, DataSet dataset) {
        if (dataset.getType() != DataSet.DataSetType.HIVE) {
            return StringUtils.EMPTY;
        }

        String clusterName = getAtlasServerName(cluster);
        AtlasObjectId objectId = null;
        try {
            objectId = ExportRequestProvider.getItemToExport(
                    dataset.getType(),
                    clusterName,
                    dataset.getSourceDataSet());
        } catch (BeaconException e) {
            errorLog("Could not create objectId for: {} - {} - {}", dataset, clusterName);
            return StringUtils.EMPTY;
        }

        Iterator<Map.Entry<String, Object>> iterator = objectId.getUniqueAttributes().entrySet().iterator();
        if (iterator == null || !iterator.hasNext()) {
            errorLog("Could find entries in objectId for: {} - {} - {}", dataset, clusterName);
            return StringUtils.EMPTY;
        }

        Map.Entry<String, Object> item  = iterator.next();
        String guid = getEntityGuid(cluster, objectId.getTypeName(), item.getKey(), (String) item.getValue());

        if (StringUtils.isEmpty(guid)) {
            errorLog("Entity not found: {}. Export skipped!", objectId);
        }

        return guid;
    }

    protected InputStream exportData(Cluster cluster, AtlasExportRequest request) throws BeaconException {
        return getClient(cluster).exportData(request);
    }

    private Path writeDataToFile(FileSystem fileSystem, Path stagingDir,
                                 String exportFileName, InputStream data) throws IOException {
        Path exportedFile = new Path(stagingDir, exportFileName);
        long numBytesWritten = FileSystemUtils.writeFile(fileSystem, exportedFile, data);

        infoLog("ExportProcess: writing {} ({} bytes)", exportFileName, numBytesWritten);
        updateExportStats(numBytesWritten);
        return new Path(stagingDir, exportedFile);
    }

    private String getExportFileName(Cluster cluster, String suffix) {
        String clusterName = getAtlasServerName(cluster);
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
}
