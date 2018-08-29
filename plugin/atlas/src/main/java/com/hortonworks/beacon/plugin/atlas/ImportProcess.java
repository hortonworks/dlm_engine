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

import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.plugin.DataSet;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Performs Atlas' Import.
 */
public class ImportProcess extends AtlasProcess {
    public ImportProcess(RESTClientBuilder builder) {
        super(builder);
    }

    @Override
    public Path run(DataSet dataset, Path exportedDataPath, AtlasPluginStats pluginStats) throws BeaconException {
        debugDatasetLog(dataset);
        infoLog("==> ImportProcess.run: Starting...");
        try {
            importFile(exportedDataPath, dataset.getSourceCluster(), dataset.getTargetCluster());
            return exportedDataPath;
        } catch (Exception e) {
            LOG.error("importData", e);
            throw new BeaconException(e);
        } finally {
            infoLog("<== ImportProcess.run: Done!");
        }
    }

    private void importFile(Path filePath, Cluster sourceCluster, Cluster targetCluster) throws Exception {
        FileSystem targetFS = FileSystemUtils.getFs(targetCluster);
        LocatedFileStatus locatedFileStatus = locateFile(filePath, targetFS);
        if (locatedFileStatus == null) {
            return;
        }

        infoLog("importFile: importing {} ...", filePath);
        InputStream inputStream = FileSystemUtils.getInputStream(targetFS, filePath);
        AtlasImportRequest importRequest = ImportRequestProvider.create(
                getAtlasClusterName(sourceCluster),
                getAtlasClusterName(targetCluster));

        importData(targetCluster, importRequest, inputStream);
        updateImportStats(locatedFileStatus.getLen());
        inputStream.close();
    }

    private LocatedFileStatus locateFile(Path filePath, FileSystem targetFS) throws IOException {
        LocatedFileStatus locatedFileStatus = FileSystemUtils.locateFile(targetFS, filePath);
        if (locatedFileStatus == null) {
            infoLog("importFile: file not found: {}!", filePath);
            return null;
        }

        return locatedFileStatus;
    }

    private void updateImportStats(long numBytesWritten) {
        updateStats(AtlasPluginStats.IMPORT_KEY, numBytesWritten);
    }

    protected AtlasImportResult importData(Cluster cluster,
                                           AtlasImportRequest request,
                                           InputStream inputStream) throws Exception {
        return getClient(cluster).importData(request, inputStream);
    }

    @VisibleForTesting
    static class ImportRequestProvider {
        private static final String REPLICATED_TAG_NAME = "REPLICATED";

        static final String IMPORT_TRANSFORM_FORMAT =
                "{ \"Asset\": { \"qualifiedName\":[ \"replace:@%s:@%s\"], "
                        + "\"*\":[ \"clearAttrValue:replicatedToCluster,replicatedFromCluster\", "
                        + "\"addClassification:"
                        + REPLICATED_TAG_NAME
                        + "\" ] } }";

        public static AtlasImportRequest create(String sourceClusterName, String targetClusterName) {
            AtlasImportRequest request = new AtlasImportRequest();
            addTransforms(request.getOptions(), sourceClusterName, targetClusterName);
            addMetaInfoUpdate(request.getOptions(), sourceClusterName);
            return request;
        }

        private static void addTransforms(Map<String, String> options,
                                          String sourceClusterName,
                                          String targetClusterName) {

            options.put(AtlasImportRequest.TRANSFORMS_KEY,
                    String.format(IMPORT_TRANSFORM_FORMAT, sourceClusterName, targetClusterName));
        }

        private static void addMetaInfoUpdate(Map<String, String> options, String sourceClusterName) {
            options.put(AtlasImportRequest.OPTION_KEY_REPLICATED_FROM, sourceClusterName);
        }
    }
}
