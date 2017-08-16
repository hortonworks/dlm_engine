/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.entity.ClusterProperties;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.rb.MessageCode;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

/**
 * Builder class to construct Beacon Cluster resource.
 */
public final class ClusterBuilder {

    private ClusterBuilder() {
    }

    public static Cluster buildCluster(final PropertiesIgnoreCase requestProperties,
                                       final String clusterName) throws BeaconException {
        requestProperties.put(ClusterProperties.NAME.getName(), clusterName);
        for (ClusterProperties property : ClusterProperties.values()) {
            if (requestProperties.getPropertyIgnoreCase(property.getName()) == null && property.isRequired()) {
                throw new BeaconException(MessageCode.COMM_010002.name(), property.getName());
            }
        }

        String name = requestProperties.getPropertyIgnoreCase(ClusterProperties.NAME.getName());
        String description = requestProperties.getPropertyIgnoreCase(ClusterProperties.DESCRIPTION.getName());
        String fsEndpoint = requestProperties.getPropertyIgnoreCase(ClusterProperties.FS_ENDPOINT.getName());
        String beaconEndpoint = requestProperties.getPropertyIgnoreCase(ClusterProperties.BEACON_ENDPOINT.getName());

        String atlasEndpoint = requestProperties.getPropertyIgnoreCase(ClusterProperties.ATLAS_ENDPOINT.getName());
        String rangerEndpoint = requestProperties.getPropertyIgnoreCase(ClusterProperties.RANGER_ENDPOINT.getName());
        String hsEndpoint = requestProperties.getPropertyIgnoreCase(ClusterProperties.HS_ENDPOINT.getName());
        String localCluster = requestProperties.getPropertyIgnoreCase(ClusterProperties.LOCAL.getName());
        boolean isLocal = StringUtils.isNotBlank(localCluster) && Boolean.parseBoolean(localCluster);
        String peers = requestProperties.getPropertyIgnoreCase(ClusterProperties.PEERS.getName());
        String tags = requestProperties.getPropertyIgnoreCase(ClusterProperties.TAGS.getName());
        Properties properties = EntityHelper.getCustomProperties(requestProperties,
                ClusterProperties.getClusterElements());


        String user = requestProperties.getPropertyIgnoreCase(ClusterProperties.USER.getName());

        return new Cluster.Builder(name, description, fsEndpoint, beaconEndpoint)
                .hsEndpoint(hsEndpoint).atlasEndpoint(atlasEndpoint).rangerEndpoint(rangerEndpoint).tags(tags)
                .peers(peers).customProperties(properties).user(user).local(isLocal).build();
    }
}
