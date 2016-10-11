package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.entity.Cluster;
import com.hortonworks.beacon.entity.ClusterProperties;

import java.util.Properties;

public final class ClusterHelper {

    private ClusterHelper() {
    }

    public static Cluster buildCluster(final Properties requestProperties) {
        Cluster cluster = new Cluster();
        cluster.setName(requestProperties.getProperty(ClusterProperties.NAME.getName()));
        cluster.setDescription(requestProperties.getProperty(ClusterProperties.DESCRIPTION.getName()));
        cluster.setDataCenter(requestProperties.getProperty(ClusterProperties.DATACENTER.getName()));
        cluster.setFsEndpoint(requestProperties.getProperty(ClusterProperties.FS_URI.getName()));
        cluster.setHsEndpoint(requestProperties.getProperty(ClusterProperties.HS_URI.getName()));
        cluster.setTags(requestProperties.getProperty(ClusterProperties.TAGS.getName()));
        cluster.setCustomProperties(EntityHelper.getCustomProperties(requestProperties, ClusterProperties.getClusterElements()));

        String aclOwner = requestProperties.getProperty(ClusterProperties.ACL_OWNER.getName());
        String aclGroup = requestProperties.getProperty(ClusterProperties.ACL_GROUP.getName());
        String aclPermission = requestProperties.getProperty(ClusterProperties.ACL_PERMISSION.getName());
        cluster.setAcl(EntityHelper.buildACL(aclOwner, aclGroup, aclPermission));

        return cluster;
    }

}
