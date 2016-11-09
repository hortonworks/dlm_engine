package com.hortonworks.beacon.entity.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
        String beaconEndpoint = requestProperties.getProperty(ClusterProperties.BEACON_URI.getName());

        String hsEndpoint = requestProperties.getProperty(ClusterProperties.HS_URI.getName());
        String peers = requestProperties.getProperty(ClusterProperties.PEERS.getName());
        String tags = requestProperties.getProperty(ClusterProperties.TAGS.getName());
        Properties properties = EntityHelper.getCustomProperties(requestProperties, ClusterProperties.getClusterElements());


        String aclOwner = requestProperties.getProperty(ClusterProperties.ACL_OWNER.getName());
        String aclGroup = requestProperties.getProperty(ClusterProperties.ACL_GROUP.getName());
        String aclPermission = requestProperties.getProperty(ClusterProperties.ACL_PERMISSION.getName());
        Acl acl = new Acl(aclOwner, aclGroup, aclPermission);

        return new Cluster.Builder(name, description, fsEndpoint, beaconEndpoint).dataCenter(datacenter).hsEndpoint
                (hsEndpoint)
                .tags(tags).peers(peers).customProperties(properties).acl(acl).build();
    }


    public static Cluster constructCluster(String jsonString) throws BeaconException {
        Gson gson = new GsonBuilder().serializeNulls().create();
        return gson.fromJson(jsonString, Cluster.class);
    }

}
