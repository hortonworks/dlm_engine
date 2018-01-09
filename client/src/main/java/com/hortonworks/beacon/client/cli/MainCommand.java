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
        } else if (cmd.hasOption(STATUS)) {
            printStatus();
        } else if (cmd.hasOption(VERSION)) {
            printVersion();
        } else {
            printUsage();
        }
    }

    private void handlePolicyCommand(String policyName, String[] originalArgs) throws BeaconClientException {
        String cmdString = MAIN_COMMAND + " -" + POLICY;
        PolicyCommand policyCmd = new PolicyCommand(cmdString, client, policyName);
        policyCmd.processCommand(originalArgs);
    }

    private void handleClusterCommand(String clusterName, String[] originalArgs) throws BeaconClientException {
        String cmdString = MAIN_COMMAND + " -" + CLUSTER;
        ClusterCommand clusterCommand = new ClusterCommand(cmdString, client, clusterName);
        clusterCommand.processCommand(originalArgs);
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
