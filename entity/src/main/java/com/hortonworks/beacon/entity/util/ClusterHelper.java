package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang3.StringUtils;

public final class ClusterHelper {
    private ClusterHelper() {
    }

    public static final String COMMA = ", ";

    public static String[] getPeers(final String clusterName) throws BeaconException {
        Cluster cluster = EntityHelper.getEntity(EntityType.CLUSTER, clusterName);
        String clusterPeers = cluster.getPeers();
        String[] peers = null;
        if (StringUtils.isNotBlank(clusterPeers)) {
            peers = clusterPeers.split(COMMA);
        }
        return peers;
    }

    public static void updatePeers(final Cluster cluster, final String newPeer) {
        String pairedWith = cluster.getPeers();
        if (StringUtils.isBlank(pairedWith)) {
            cluster.setPeers(newPeer);
        } else {
            cluster.setPeers(pairedWith.concat(COMMA).concat(newPeer));
        }
    }

    public static boolean areClustersPaired(final String localCluster,
                                            final String remoteCluster) throws BeaconException {
        String[] peers = getPeers(localCluster);
        if (peers != null) {
            for (String peer : peers) {
                if (peer.equalsIgnoreCase(remoteCluster)) {
                    return true;
                }
            }
        }
        return false;
    }
}
