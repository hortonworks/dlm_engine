/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.client.cli;

import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.BeaconWebClient;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.Engine;

/**
 * Main class for CLI. Parses and executes beacon command using beacon web client.
 */
public class BeaconCLI {

    private final BeaconClient client;

    public static final void main(String[] args) throws BeaconClientException {
        new BeaconCLI().processCommand(args);
    }

    public void processCommand(String[] args) throws BeaconClientException {
        MainCommand mainCommand = new MainCommand(client);
        mainCommand.processCommand(args);
    }

    public BeaconCLI() throws BeaconClientException {
        BeaconConfig beaconConfig = BeaconConfig.getInstance();
        Engine engine = beaconConfig.getEngine();
        client = new BeaconWebClient(getBeaconEndpoint(engine));
    }

    @VisibleForTesting
    public BeaconCLI(String endpoint) throws BeaconClientException {
        client = new BeaconWebClient(endpoint);
    }

    @VisibleForTesting
    public BeaconCLI(BeaconClient client) {
        this.client = client;
    }

    //Get beacon endpoint from different properties
    private static String getBeaconEndpoint(Engine engine) {
        int port = engine.getTlsEnabled() ? engine.getTlsPort() : engine.getPort();
        String protocol = engine.getTlsEnabled() ? "https" : "http";
        return String.format("%s://%s:%s", protocol, engine.getHostName(), port);
    }
}
