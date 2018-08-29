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
import org.apache.atlas.model.impexp.AtlasCluster;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of RESTClient, encapsulates Atlas' REST APIs.
 */
public class AtlasRESTClient implements RESTClient {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasRESTClient.class);
    private final AtlasClientV2 clientV2;

    public AtlasRESTClient(AtlasClientV2 clientV2) {
        this.clientV2 = clientV2;
    }

    @Override
    public InputStream exportData(AtlasExportRequest request) throws BeaconException {
        try {
            debugLog("exportData: {}", request);
            return clientV2.exportData(request);
        } catch (AtlasServiceException e) {
            LOG.error("exportData", e);
            throw new BeaconException(e);
        }
    }

    @Override
    public AtlasImportResult importData(AtlasImportRequest request, InputStream inputStream) throws BeaconException {
        try {
            if (inputStream == null) {
                return new AtlasImportResult(request, "", "", "", 0L);
            }

            debugLog("importData: {}", request);
            return clientV2.importData(request, inputStream);
        } catch (AtlasServiceException e) {
            LOG.error("importData", e);
            throw new BeaconException(e);
        }
    }

    @Override
    public AtlasCluster getCluster(String clusterName) throws BeaconException {
        try {
            debugLog("getCluster: clusterName: {}", clusterName);
            return clientV2.getCluster(clusterName);
        } catch (AtlasServiceException e) {
            LOG.error("getCluster", e);
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
                LOG.error("Atlas entity cannot be retrieved using: type: {} and {} - {}",
                        entityType, attributeName, qualifiedName);
                return null;
            }

            return entityWithExtInfo.getEntity().getGuid();
        } catch (AtlasServiceException e) {
            LOG.error("getEntityGuid", e);
            return null;
        }
    }

    private void debugLog(String s, Object... params) {
        if (!LOG.isDebugEnabled()) {
            return;
        }

        LOG.debug(s, params);
    }

    public static RESTClientBuilder buildCached() {
        return new RESTClientBuilder.CachedBuilder();
    }
}
