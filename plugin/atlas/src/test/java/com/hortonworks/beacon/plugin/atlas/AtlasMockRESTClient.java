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
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasExportResult;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.impexp.AtlasServer;
import org.testng.SkipException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

/**
 * Mock implementation of Atlas REST Client. Used by tests.
 */
public class AtlasMockRESTClient extends RetryingClient implements RESTClient {
    private static final String RESOURCES_PATH = "src/test/resources/atlas";
    private static final long EXPECTED_TIMESTAMP = 1534801666522L;
    private String filePath;

    @Override
    public InputStream exportData(AtlasExportRequest request) throws BeaconException {
        try {
            AtlasExportResult result = new AtlasExportResult();
            result.setOperationStatus(AtlasExportResult.OperationStatus.SUCCESS);
            java.nio.file.Path path = Paths.get(filePath);
            return Files.newInputStream(path);
        } catch (IOException ex) {
            throw new BeaconException("exportData: Failed!", ex);
        }
    }

    @Override
    public AtlasImportResult importData(final AtlasImportRequest request, InputStream inputStream) {
        try {
            return invokeWithRetry(new Callable<AtlasImportResult>() {
                @Override
                public AtlasImportResult call() {
                    return getDefaultAtlasImportResult(request);
                }
            }, getDefaultAtlasImportResult(request));
        } catch (BeaconException e) {
            throw new SkipException("BeaconException", e);
        }
    }

    public AtlasImportResult getDefaultAtlasImportResult(AtlasImportRequest request) {
        return new AtlasImportResult(request,
                "",
                "",
                "",
                0L);
    }

    @Override
    public AtlasServer getServer(String serverName) {
        AtlasServer cluster = new AtlasServer(serverName, serverName);
        cluster.setAdditionalInfoRepl(
                getEntityGuid("", "", ""), EXPECTED_TIMESTAMP);
        return cluster;
    }

    private void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public String getEntityGuid(String entityType, String attributeName, String qualifiedName) {
        return "ABCD-DEF";
    }

    private static String getResourceFilePath(String fileName) {
        final String userDir = System.getProperty("user.dir");
        return String.format("%s/%s/%s", userDir, RESOURCES_PATH, fileName);
    }

    /**
     * Builder for AtlasMockRESTClient.
     */
    public static class Builder extends RESTClientBuilder {
        private String filePath;

        public RESTClient create() {
            AtlasMockRESTClient restClient = new AtlasMockRESTClient();
            restClient.setFilePath(filePath);
            return restClient;
        }

        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }
    }
}
