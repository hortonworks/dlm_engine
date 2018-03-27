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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.BeaconWebClient;

/**
 * Main command that handles higher level commands like status, version etc.
 */
public class MainCommand extends CommandBase {
    public static final String MAIN_COMMAND = "beacon";

    private static final String STATUS = "status";
    private static final String VERSION = "version";

    MainCommand(BeaconClient client) {
        super(MAIN_COMMAND, client);
    }

    protected Options createOptions() {
        Options options = new Options();
        options.addOption(new Option(HELP, "Prints command usage"));
        options.addOption(OptionBuilder.withArgName("cluster name").hasOptionalArg()
                .withDescription("Cluster operations").create(CLUSTER));
        options.addOption(OptionBuilder.withArgName("policy name").hasOptionalArg()
                .withDescription("Policy operations").create(POLICY));
        options.addOption(OptionBuilder.withArgName("cloudcred id").hasOptionalArg()
                .withDescription("Cloudcred operations").create(CLOUDCRED));
        options.addOption(OptionBuilder.withArgName("beacon endpoint").hasArg()
                .withDescription("Beacon endpoint").create(URL));
        options.addOption(new Option(STATUS, "Prints beacon server status"));
        options.addOption(new Option(VERSION, "Prints beacon server version"));
        return options;
    }

    protected void processCommand(CommandLine cmd, String[] originalArgs) throws BeaconClientException {
        if (cmd.hasOption(URL)) {
            client = new BeaconWebClient(cmd.getOptionValue(URL));
        }

        if (cmd.hasOption(CLUSTER)) {
            handleClusterCommand(cmd.getOptionValue(CLUSTER), originalArgs);
        } else if (cmd.hasOption(POLICY)) {
            handlePolicyCommand(cmd.getOptionValue(POLICY), originalArgs);
        }else if (cmd.hasOption(CLOUDCRED)) {
            handleCloudCredCommand(cmd.getOptionValue(CLOUDCRED), originalArgs);
        } else if (cmd.hasOption(STATUS)) {
            printStatus();
        } else if (cmd.hasOption(VERSION)) {
            printVersion();
        } else {
            handleResourceCommand(originalArgs);
        }
    }

    private void handlePolicyCommand(String policyName, String[] originalArgs) throws BeaconClientException {
        String cmdString = MAIN_COMMAND + " -" + POLICY;
        PolicyCommand policyCmd = new PolicyCommand(cmdString, client, policyName);
        policyCmd.processCommand(originalArgs);
    }

    private void handleCloudCredCommand(String cloudCredID, String[] originalArgs) throws BeaconClientException {
        String cmdString = MAIN_COMMAND + " -" + CLOUDCRED;
        CloudCredCommand cloudCredCmd = new CloudCredCommand(cmdString, client, cloudCredID);
        cloudCredCmd.processCommand(originalArgs);
    }

    private void handleClusterCommand(String clusterName, String[] originalArgs) throws BeaconClientException {
        String cmdString = MAIN_COMMAND + " -" + CLUSTER;
        ClusterCommand clusterCommand = new ClusterCommand(cmdString, client, clusterName);
        clusterCommand.processCommand(originalArgs);
    }

    private void handleResourceCommand(String[] originalArgs) throws BeaconClientException {
        MiscCommand datasetCommand = new MiscCommand(MAIN_COMMAND, client);
        datasetCommand.processCommand(originalArgs);
    }

    public void printStatus() throws BeaconClientException {
        System.out.println(
                ReflectionToStringBuilder.toString(client.getServiceStatus(), ToStringStyle.MULTI_LINE_STYLE));
    }

    private void printVersion() throws BeaconClientException {
        System.out.println(
                ReflectionToStringBuilder.toString(client.getServiceVersion(), ToStringStyle.MULTI_LINE_STYLE));
    }
}
