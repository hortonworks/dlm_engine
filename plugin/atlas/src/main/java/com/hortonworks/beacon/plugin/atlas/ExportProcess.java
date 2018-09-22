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
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

/**
 * Performs Atlas' Export.
 */
public class ExportProcess extends AtlasProcess {
    protected static final Logger LOG = LoggerFactory.getLogger(ExportProcess.class);

    private static final String ATLAS_EXPORTED_FILE_NAME_TEMPLATE = "atlas-export-%s-%s.zip";

    public ExportProcess(RESTClientBuilder builder) {
        super(builder);
    }

    public Path run(DataSet dataset, Path stagingDir, AtlasPluginStats pluginStats) throws BeaconException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("BeaconAtlasPlugin: AtlasProcess: dataset: {}, stagingDir: {}", dataset, stagingDir);
        }

        LOG.info("BeaconAtlasPlugin: AtlasProcess: ==> ExportProcess.run: Starting: {} ...", stagingDir);
        Path exportPath = null;
        try {
            FileSystem fs = FileSystemUtils.getFs(stagingDir.toString());
            String sourceFsUri = dataset.getSourceCluster().getFsEndpoint();

            Cluster sourceCluster = dataset.getSourceCluster();
            AtlasExportRequest exportRequest;
            String entityGuid = checkHiveEntityExists(sourceCluster, dataset, sourceFsUri);
            if (dataset.getType() == DataSet.DataSetType.HIVE && StringUtils.isEmpty(entityGuid)) {
                return null;
            }

            exportRequest = ExportRequestProvider.create(this, dataset, entityGuid, sourceFsUri);

            String exportFileName = getExportFileName(sourceCluster, getCurrentTimestamp());

            InputStream inputStream = exportData(sourceCluster, exportRequest);
            exportPath = writeDataToFile(fs, stagingDir, exportFileName, inputStream);

            return exportPath;
        } catch (AtlasRestClientException ex) {
            throw ex;
        } catch (BeaconException ex) {
            LOG.error("BeaconAtlasPlugin: ExportProcess: connection errors", ex);
            throw ex;
        } catch (Exception ex) {
            LOG.info("BeaconAtlasPlugin: AtlasProcess: ExportProcess: failed! - {}", ex.getMessage());
            return null;
        } finally {
            LOG.info("BeaconAtlasPlugin: AtlasProcess: <== ExportProcess.run: {} - {}: Done!", stagingDir,
                    (exportPath != null) ? exportPath.toString() : "");
        }
    }

    private String checkHiveEntityExists(Cluster cluster, DataSet dataset, String fsUri) {
        if (dataset.getType() != DataSet.DataSetType.HIVE) {
            return StringUtils.EMPTY;
        }

        String clusterName = getAtlasServerName(cluster);
        AtlasObjectId objectId = null;
        try {
            objectId = ExportRequestProvider.getItemToExport(
                    dataset.getType(),
                    clusterName,
                    dataset.getSourceDataSet(), fsUri);
        } catch (BeaconException e) {
            LOG.error("BeaconAtlasPlugin: Could not create objectId for: {} - {} - {}", dataset, clusterName);
            return StringUtils.EMPTY;
        }

        Iterator<Map.Entry<String, Object>> iterator = objectId.getUniqueAttributes().entrySet().iterator();
        if (iterator == null || !iterator.hasNext()) {
            LOG.error("BeaconAtlasPlugin: Could find entries in objectId for: {} - {} - {}", dataset, clusterName);
            return StringUtils.EMPTY;
        }

        Map.Entry<String, Object> item  = iterator.next();
        String guid = getEntityGuid(cluster, objectId.getTypeName(), item.getKey(), (String) item.getValue());

        if (StringUtils.isEmpty(guid)) {
            LOG.error("BeaconAtlasPlugin: Entity not found: {}. Export skipped!", objectId);
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

        LOG.info("BeaconAtlasPlugin: AtlasProcess: ExportProcess: writing {} ({} bytes)",
                exportFileName, numBytesWritten);

        updateExportStats(numBytesWritten);
        return new Path(stagingDir, exportedFile);
    }

    private String getExportFileName(Cluster cluster, String suffix) {
        String clusterName = getAtlasServerName(cluster);
        String s = String.format(ATLAS_EXPORTED_FILE_NAME_TEMPLATE, clusterName, suffix);

        if (LOG.isDebugEnabled()) {
            LOG.debug("BeaconAtlasPlugin: AtlasProcess: getExportFileName: {}", s);
        }

        return s;
    }

    private String getCurrentTimestamp() {
        return Long.toString(System.currentTimeMillis());
    }

    private void updateExportStats(long numBytesWritten) {
        updateStats(AtlasPluginStats.EXPORT_KEY, numBytesWritten);
    }
}
