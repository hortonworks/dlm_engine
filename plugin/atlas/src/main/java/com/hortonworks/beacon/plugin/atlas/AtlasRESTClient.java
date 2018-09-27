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
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.impexp.AtlasServer;
import org.apache.atlas.model.instance.AtlasEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Implementation of RESTClient, encapsulates Atlas' REST APIs.
 */
public class AtlasRESTClient extends RetryingClient implements RESTClient {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasRESTClient.class);
    private final AtlasClientV2 clientV2;

    public AtlasRESTClient(AtlasClientV2 clientV2) {
        this.clientV2 = clientV2;
    }

    @Override
    public InputStream exportData(final AtlasExportRequest request) throws BeaconException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("exportData: {}", request);
        }

        return invokeWithRetry(new Callable<InputStream>() {
            @Override
            public InputStream call() throws Exception {
                return clientV2.exportData(request);
            }
        }, null);
    }

    @Override
    public AtlasImportResult importData(final AtlasImportRequest request,
                                        final InputStream inputStream) throws BeaconException {
        AtlasImportResult defaultResult = getDefaultAtlasImportResult(request);
        if (inputStream == null) {
            return defaultResult;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("importData: {}", request);
        }

        return invokeWithRetry(new Callable<AtlasImportResult>() {
            @Override
            public AtlasImportResult call() throws Exception {
                return clientV2.importData(request, inputStream);
            }
        }, defaultResult);
    }


    private AtlasImportResult getDefaultAtlasImportResult(AtlasImportRequest request) {
        return new AtlasImportResult(request, "", "", "", 0L);
    }

    @Override
    public AtlasServer getServer(String serverName) {
        try {
            return clientV2.getServer(serverName);
        } catch (AtlasServiceException e) {
            LOG.warn("getServer of: {} returned: {}", serverName, e.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug("getServer of: {} returned: {}", serverName, e.getMessage(), e);
            }
        }

        return null;
    }

    @Override
    public String getEntityGuid(String entityType,
                                final String attributeName, final String qualifiedName) {
        Map<String, String> attributes = new HashMap<String, String>() {{
                put(attributeName, qualifiedName);
            }};

        try {
            AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo =
                    clientV2.getEntityByAttribute(entityType, attributes);

            if (entityWithExtInfo == null || entityWithExtInfo.getEntity() == null) {
                LOG.warn("Atlas entity cannot be retrieved using: type: {} and {} - {}",
                        entityType, attributeName, qualifiedName);
                return null;
            }

            return entityWithExtInfo.getEntity().getGuid();
        } catch (AtlasServiceException e) {
            LOG.warn("getEntityGuid: Could not retrieve entity guid for: {}-{}-{}",
                    entityType, attributeName, qualifiedName);
            if (LOG.isDebugEnabled()) {
                LOG.warn("getEntityGuid: Could not retrieve entity guid for: {}-{}-{}",
                        entityType, attributeName, qualifiedName, e);
            }

            return null;
        }
    }

    public static RESTClientBuilder build() {
        return new RESTClientBuilder();
    }
}
