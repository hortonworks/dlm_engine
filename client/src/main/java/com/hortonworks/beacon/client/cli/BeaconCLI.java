/**
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *    OR LOSS OR CORRUPTION OF DATA.
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
        int port = engine.isTlsEnabled() ? engine.getTlsPort() : engine.getPort();
        String protocol = engine.isTlsEnabled() ? "https" : "http";
        return String.format("%s://%s:%s", protocol, engine.getHostName(), port);
    }
}
