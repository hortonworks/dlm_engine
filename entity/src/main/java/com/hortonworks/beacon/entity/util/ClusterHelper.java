package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.entity.Cluster;
import com.hortonworks.beacon.entity.ClusterProperties;

import java.util.Properties;

/**
 * Created by sramesh on 10/4/16.
 */
public final class ClusterHelper {

    private ClusterHelper() {
    }

    public static Cluster buildCluster(final Properties requestProperties) {
        Cluster cluster = new Cluster();
        cluster.setName(requestProperties.getProperty(ClusterProperties.NAME.getName()));
        cluster.setDescription(requestProperties.getProperty(ClusterProperties.DESCRIPTION.getName()));
        cluster.setColo(requestProperties.getProperty(ClusterProperties.COLO.getName()));
        cluster.setNameNodeUri(requestProperties.getProperty(ClusterProperties.NAMENODE_URI.getName()));
        cluster.setExecuteUri(requestProperties.getProperty(ClusterProperties.EXECUTE_URI.getName()));
        cluster.setWfEngineUri(requestProperties.getProperty(ClusterProperties.WF_ENGINE_URI.getName()));
        cluster.setMessagingUri(requestProperties.getProperty(ClusterProperties.MESSAGING_URI.getName()));
        cluster.setHs2Uri(requestProperties.getProperty(ClusterProperties.HS2_URI.getName()));
        cluster.setTags(requestProperties.getProperty(ClusterProperties.TAGS.getName()));
        cluster.setCustomProperties(EntityHelper.getCustomProperties(requestProperties, ClusterProperties.getClusterElements()));

        String aclOwner = requestProperties.getProperty(ClusterProperties.ACL_OWNER.getName());
        String aclGroup = requestProperties.getProperty(ClusterProperties.ACL_GROUP.getName());
        String aclPermission = requestProperties.getProperty(ClusterProperties.ACL_PERMISSION.getName());
        cluster.setAcl(EntityHelper.buildACL(aclOwner, aclGroup, aclPermission));

        return cluster;
    }

}
