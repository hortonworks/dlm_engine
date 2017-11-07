/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.client.resource.ServerStatusResult;
import com.hortonworks.beacon.client.resource.ServerVersionResult;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.PropertiesUtil;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.log.BeaconLogUtils;
import com.hortonworks.beacon.plugin.service.PluginManagerService;
import org.apache.commons.lang3.StringUtils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.List;

/**
 * Beacon admin resource management operations as REST API. Root resource (exposed at "myresource" path).
 */
@Path("/api/beacon/admin")
public class AdminResource {

    @GET
    @Path("version")
    @Produces({MediaType.APPLICATION_JSON})
    public ServerVersionResult getServerVersion(@Context HttpServletRequest request) {
        BeaconLogUtils.createPrefix((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName());
        return getServerVersion();
    }

    @GET
    @Path("status")
    @Produces({MediaType.APPLICATION_JSON})
    public ServerStatusResult getServerStatus(@Context HttpServletRequest request) {
        BeaconLogUtils.createPrefix((String) request.getSession().getAttribute(BeaconConstants.USERNAME_ATTRIBUTE),
                BeaconConfig.getInstance().getEngine().getLocalClusterName());
        return getServerStatus();
    }

    private ServerVersionResult getServerVersion() {
        ServerVersionResult result = new ServerVersionResult();
        result.setStatus("RUNNING");
        String beaconVersion = System.getProperty(BeaconConstants.BEACON_VERSION_CONST,
                BeaconConstants.DEFAULT_BEACON_VERSION);
        result.setVersion(beaconVersion);
        return result;
    }

    private ServerStatusResult getServerStatus() {
        ServerStatusResult result = new ServerStatusResult();
        result.setStatus("RUNNING");
        result.setVersion(getServerVersion().getVersion());
        result.setWireEncryption(BeaconConfig.getInstance().getEngine().getTlsEnabled());
        result.setSecurity("None");
        List<String> registeredPlugins = PluginManagerService.getRegisteredPlugins();
        if (registeredPlugins.isEmpty()) {
            result.setPlugins("None");
        } else {
            result.setPlugins(StringUtils.join(registeredPlugins, BeaconConstants.COMMA_SEPARATOR));
        }
        result.setRangerCreateDenyPolicy(PropertiesUtil.getInstance().
                getProperty("beacon.ranger.plugin.create.denypolicy"));
        return result;
    }
}
