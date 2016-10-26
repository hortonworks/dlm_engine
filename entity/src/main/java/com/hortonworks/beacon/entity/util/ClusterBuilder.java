package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.client.entity.Acl;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.entity.ClusterProperties;
import com.hortonworks.beacon.exceptions.BeaconException;

import java.util.Properties;

public final class ClusterBuilder {

    private ClusterBuilder() {
    }

    public static Cluster buildCluster(final Properties requestProperties) throws BeaconException {
        for (ClusterProperties property : ClusterProperties.values()) {
            if (requestProperties.getProperty(property.getName()) == null && property.isRequired()) {
                throw new BeaconException("Missing parameter: " + property.getName());
            }
        }

        String name = requestProperties.getProperty(ClusterProperties.NAME.getName());
        String description = requestProperties.getProperty(ClusterProperties.DESCRIPTION.getName());
        String datacenter = requestProperties.getProperty(ClusterProperties.DATACENTER.getName());
        String fsEndpoint = requestProperties.getProperty(ClusterProperties.FS_URI.getName());

        String hsEndpoint = requestProperties.getProperty(ClusterProperties.HS_URI.getName());
        String peers = requestProperties.getProperty(ClusterProperties.PEERS.getName());
        String tags = requestProperties.getProperty(ClusterProperties.TAGS.getName());
        Properties properties = EntityHelper.getCustomProperties(requestProperties, ClusterProperties.getClusterElements());


        String aclOwner = requestProperties.getProperty(ClusterProperties.ACL_OWNER.getName());
        String aclGroup = requestProperties.getProperty(ClusterProperties.ACL_GROUP.getName());
        String aclPermission = requestProperties.getProperty(ClusterProperties.ACL_PERMISSION.getName());
        Acl acl = new Acl(aclOwner, aclGroup, aclPermission);

        Cluster cluster = new Cluster.Builder(name, description, fsEndpoint).dataCenter(datacenter).hsEndpoint(hsEndpoint)
                .tags(tags).peers(peers).customProperties(properties).acl(acl).build();

        return cluster;
    }

}
