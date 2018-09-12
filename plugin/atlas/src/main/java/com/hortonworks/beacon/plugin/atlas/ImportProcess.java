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
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Performs Atlas' Import.
 */
public class ImportProcess extends AtlasProcess {
    protected static final Logger LOG = LoggerFactory.getLogger(ImportProcess.class);

    public ImportProcess(RESTClientBuilder builder) {
        super(builder);
    }

    @Override
    public Path run(DataSet dataset, Path exportedDataPath, AtlasPluginStats pluginStats) throws BeaconException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("BeaconAtlasPlugin: dataset: {}, path: {}", dataset, exportedDataPath);
        }

        try {
            if (checkEmptyPath(exportedDataPath)) {
                LOG.info("BeaconAtlasPlugin: AtlasProcess: ==> ImportProcess.run: empty path found! Exiting!");
                return exportedDataPath;
            }

            LOG.info("BeaconAtlasPlugin: AtlasProcess: ==> ImportProcess.run: Starting {} ...",
                    exportedDataPath.getName());

            importFile(exportedDataPath, dataset.getSourceCluster(), dataset.getTargetCluster());

            return exportedDataPath;
        } catch (AtlasRestClientException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("BeaconAtlasPlugin: importData", e);
            throw new BeaconException(e);
        } finally {
            LOG.info("BeaconAtlasPlugin: AtlasProcess: <== ImportProcess.run: {}: Done!",
                    (exportedDataPath != null) ? exportedDataPath.getName() : "<empty path>");
        }
    }

    private void importFile(Path filePath, Cluster sourceCluster, Cluster targetCluster) throws Exception {
        if (checkEmptyPath(filePath)) {
            return;
        }

        FileSystem targetFS = FileSystemUtils.getFs(filePath.toString());
        FileStatus fileStatus = locateFile(filePath, targetFS);
        if (fileStatus == null) {
            return;
        }

        LOG.info("BeaconAtlasPlugin: AtlasProcess: importFile: importing {} ...", filePath);
        InputStream inputStream = FileSystemUtils.getInputStream(targetFS, filePath);
        AtlasImportRequest importRequest = ImportRequestProvider.create(
                getAtlasServerName(sourceCluster),
                getAtlasServerName(targetCluster));

        AtlasImportResult result = importData(targetCluster, importRequest, inputStream);
        if (result == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("No entities found!");
            }

            return;
        }

        LOG.info("BeaconAtlasPlugin: AtlasProcess: importFile: imported! {} ({} bytes)...",
                filePath.getName(),
                fileStatus.getLen());

        updateImportStats(fileStatus.getLen());
        inputStream.close();
    }

    private boolean checkEmptyPath(Path filePath) {
        if (filePath == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("BeaconAtlasPlugin: AtlasProcess: Empty path encountered!");
            }

            return true;
        }
        return false;
    }

    private FileStatus locateFile(Path filePath, FileSystem targetFS) throws IOException {
        FileStatus fileStatus = FileSystemUtils.locateFile(targetFS, filePath);
        if (fileStatus == null) {
            LOG.info("BeaconAtlasPlugin: AtlasProcess: importFile: file not found: {}!", filePath);
            return null;
        }

        return fileStatus;
    }

    private void updateImportStats(long numBytesWritten) {
        updateStats(AtlasPluginStats.IMPORT_KEY, numBytesWritten);
    }

    protected AtlasImportResult importData(Cluster cluster,
                                           AtlasImportRequest request,
                                           InputStream inputStream) throws Exception {
        return getClient(cluster).importData(request, inputStream);
    }
}