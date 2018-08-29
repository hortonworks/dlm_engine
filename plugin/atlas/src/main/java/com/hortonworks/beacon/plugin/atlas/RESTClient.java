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
import org.apache.atlas.model.impexp.AtlasCluster;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;

import java.io.InputStream;

/**
 * Encapsulates AtlasClient.
 */
public interface RESTClient {
    /**
     * Uses Atlas to perform export of data.
     *
     * @param request, request details
     * @throws BeaconException
     * @return, contents of exported data packaged as ZIP file
     */
    InputStream exportData(AtlasExportRequest request) throws BeaconException;

    /**
     * Uses Atlas to perform import of data.
     *
     * @param request,   request details
     * @param inputStream, contents to be imported
     * @return
     * @throws BeaconException
     */
    AtlasImportResult importData(AtlasImportRequest request, InputStream inputStream) throws BeaconException;

    /**
     * Queries Atlas for getting last synchronization information.
     *
     * @param clusterName, name of the cluster
     * @return
     */
    AtlasCluster getCluster(String clusterName) throws BeaconException;

    /**
     * Get entity guid from qualified name.
     *
     * @param entityType,    Atlas entity type
     * @param attributeName, Atlas entity attribute to be used for qualified name
     * @param qualifiedName, qualified name of the entity
     * @return GUID belonging to that entity
     */
    String getEntityGuid(String entityType,
                         final String attributeName, final String qualifiedName) throws BeaconException;
}
