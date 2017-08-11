/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.test;

import com.hortonworks.beacon.main.Main;

import java.util.Properties;

/**
 * Embedded beacon server.
 */
public class EmbeddedBeaconServer {

    private void startBeaconServer(int port, String localCluster) throws Exception {
        BeaconTestUtil.createDBSchema();
        Main.main(new String[] {"-port", String.valueOf(port), "-localcluster", localCluster, });
    }


    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("properties file missing for embedded beacon server.");
        }

        Properties prop = BeaconTestUtil.getProperties(args[0]);
        EmbeddedBeaconServer embeddedBeaconServer = new EmbeddedBeaconServer();
        embeddedBeaconServer.startBeaconServer(
                Integer.parseInt(prop.getProperty("beacon.port")),
                prop.getProperty("beacon.local.cluster"));
    }
}
