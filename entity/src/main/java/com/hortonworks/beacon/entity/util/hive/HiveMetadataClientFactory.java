/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */
package com.hortonworks.beacon.entity.util.hive;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.exceptions.BeaconException;

/**
 * Hive Metadata Client.
 */
public final class HiveMetadataClientFactory {

    private HiveMetadataClientFactory() {
    }

    public static HiveMetadataClient getClient(Cluster cluster) throws BeaconException {
        if (cluster.getHmsEndpoint() != null) {
            return new HMSMetadataClient(cluster);
        }

        if (cluster.getHsEndpoint() != null) {
            return new HS2MetadataClient(cluster);
        }

        throw new BeaconException("Failed to get HMSMetadataClient for cluster {}. Specify HMS/HS2 endpoint",
                cluster.getName());
    }

    public static void close(HiveMetadataClient client) throws BeaconException {
        if (client != null) {
            client.close();
        }
    }
}
