package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.client.entity.Acl;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.entity.ClusterProperties;
import com.hortonworks.beacon.exceptions.BeaconException;

import java.util.Properties;

public final class ClusterBuilder {

    private ClusterBuilder() {
    }

    public static Cluster buildCluster(final PropertiesIgnoreCase requestProperties,
                                       final String clusterName) throws BeaconException {
        requestProperties.put(ClusterProperties.NAME.getName(), clusterName);
        for (ClusterProperties property : ClusterProperties.values()) {
            if (requestProperties.getPropertyIgnoreCase(property.getName()) == null && property.isRequired()) {
                throw new BeaconException("Missing parameter: " + property.getName());
            }
        }

        String name = requestProperties.getPropertyIgnoreCase(ClusterProperties.NAME.getName());
        String description = requestProperties.getPropertyIgnoreCase(ClusterProperties.DESCRIPTION.getName());
        String datacenter = requestProperties.getPropertyIgnoreCase(ClusterProperties.DATACENTER.getName());
        String fsEndpoint = requestProperties.getPropertyIgnoreCase(ClusterProperties.FS_URI.getName());
        String beaconEndpoint = requestProperties.getPropertyIgnoreCase(ClusterProperties.BEACON_URI.getName());

        String hsEndpoint = requestProperties.getPropertyIgnoreCase(ClusterProperties.HS_URI.getName());
        String peers = requestProperties.getPropertyIgnoreCase(ClusterProperties.PEERS.getName());
        String tags = requestProperties.getPropertyIgnoreCase(ClusterProperties.TAGS.getName());
        Properties properties = EntityHelper.getCustomProperties(requestProperties, ClusterProperties.getClusterElements());


        String aclOwner = requestProperties.getPropertyIgnoreCase(ClusterProperties.ACL_OWNER.getName());
        String aclGroup = requestProperties.getPropertyIgnoreCase(ClusterProperties.ACL_GROUP.getName());
        String aclPermission = requestProperties.getPropertyIgnoreCase(ClusterProperties.ACL_PERMISSION.getName());
        Acl acl = new Acl(aclOwner, aclGroup, aclPermission);

        return new Cluster.Builder(name, description, fsEndpoint, beaconEndpoint).dataCenter(datacenter)
                .hsEndpoint(hsEndpoint).tags(tags).peers(peers).customProperties(properties).acl(acl).build();
    }
}
