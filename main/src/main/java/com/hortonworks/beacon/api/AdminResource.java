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

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.client.resource.ServerStatusResult;
import com.hortonworks.beacon.client.resource.ServerVersionResult;
import com.hortonworks.beacon.config.PropertiesUtil;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.main.BeaconServer;
import com.hortonworks.beacon.plugin.service.PluginManagerService;
import org.apache.commons.lang3.StringUtils;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.List;

/**
 * Beacon admin resource management operations as REST API. Root resource (exposed at "myresource" path).
 */
@Path("/api/beacon/admin")
public class AdminResource extends AbstractResourceManager {

    @GET
    @Path("version")
    @Produces({MediaType.APPLICATION_JSON})
    public ServerVersionResult getServerVersion() {
        return getServerVersionInternal();
    }

    @GET
    @Path("status")
    @Produces({MediaType.APPLICATION_JSON})
    public ServerStatusResult getServerStatus() {
        return getServerStatusInternal();
    }

    private ServerVersionResult getServerVersionInternal() {
        ServerVersionResult result = new ServerVersionResult();
        result.setStatus("RUNNING");
        String beaconVersion = System.getProperty(BeaconConstants.BEACON_VERSION_CONST,
                BeaconConstants.DEFAULT_BEACON_VERSION);
        result.setVersion(beaconVersion);
        return result;
    }

    private ServerStatusResult getServerStatusInternal() {
        ServerStatusResult result = new ServerStatusResult();
        result.setStatus("RUNNING");
        result.setVersion(getServerVersion().getVersion());

        //Beacon 1.0 features
        result.setWireEncryption(config.getEngine().isTlsEnabled());
        result.setSecurity("None");
        List<String> registeredPlugins = PluginManagerService.getRegisteredPlugins();
        if (registeredPlugins.isEmpty()) {
            result.setPlugins("None");
        } else {
            result.setPlugins(StringUtils.join(registeredPlugins, BeaconConstants.COMMA_SEPARATOR));
        }
        result.setRangerCreateDenyPolicy(PropertiesUtil.getInstance().
                getBooleanProperty("beacon.ranger.plugin.create.denypolicy", true));

        //Beacon 1.1 features
        result.setReplicationTDE(true);
        result.setReplicationCloudFS(true);
        result.setReplicationCloudHiveWithCluster(true);
        result.setEnableSourceSnapshottable(true);
        result.setKnoxProxyingSupported(true);
        result.setKnoxProxyingEnabled(config.getEngine().isKnoxProxyEnabled());

        result.setCloudHosted(BeaconServer.getInstance().isCloudHosted());
        return result;
    }
}
