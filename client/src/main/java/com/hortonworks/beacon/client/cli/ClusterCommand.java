/**
 * Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 * <p>
 * Except as expressly permitted in a written agreement between you or your
 * company and Hortonworks, Inc. or an authorized affiliate or partner
 * thereof, any use, reproduction, modification, redistribution, sharing,
 * lending or other exploitation of all or any part of the contents of this
 * software is strictly prohibited.
 */


package com.hortonworks.beacon.client.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.ClusterList;
import com.hortonworks.beacon.client.resource.StatusResult;

public class ClusterCommand extends CommandBase {
    public static final String PAIR = "pair";
    private final String clusterName;

    ClusterCommand(String cmd, BeaconClient client, String clusterName) {
        super(cmd, client);
        this.clusterName = clusterName;
    }

    @Override
    protected void processCommand(CommandLine cmd, String[] originalArgs) {
        if (cmd.hasOption(SUBMIT)) {
            submitCluster(clusterName, cmd.getOptionValue(CONFIG));
        } else if (cmd.hasOption(LIST)) {
            listClusters();
        } else if (cmd.hasOption(HELP)) {
            printUsage();
        } else if (cmd.hasOption(STATUS)) {
            printStatus(clusterName);
        } else if(cmd.hasOption(PAIR)) {
            pair(clusterName);
        } else if(cmd.hasOption(DELETE)) {
            delete(clusterName);
        } else {
            printUsage();
        }
    }

    @Override
    protected void printUsage() {
        System.out.println("Cluster submit: beacon -cluster <cluster name> -submit -config <config file path>");
        System.out.println("Cluster status: beacon -cluster <cluster name> -status");
        System.out.println("Cluster delete: beacon -cluster <cluster name> -delete");
        System.out.println("Cluster pairing: beacon -cluster <remote cluster name> -pair");
        System.out.println("Cluster list: beacon -cluster -list");
        super.printUsage();
    }

    private void delete(String clusterName) {
        APIResult result = client.deleteCluster(clusterName);
        printResult("Delete of cluster " + clusterName, result);
    }

    private void pair(String remoteCluster) {
        APIResult result = client.pairClusters(remoteCluster, false);
        printResult("Cluster pairing with " + remoteCluster + " cluster", result);
    }

    private void printStatus(String clusterName) {
        StatusResult result = client.getClusterStatus(clusterName);
        printResult("Cluster " + clusterName + "' status", result);
    }

    private void listClusters() {
        //TODO add sane defaults to the API and client
        //TODO result doesn't have API success/failure?
        //TODO handle pagination?
        ClusterList result = client.getClusterList("name", "name", "ASC", 0, 10);
        printResult("Cluster list", result);
    }

    private void printResult(String operation, ClusterList result) {
        System.out.println(operation + " " + APIResult.Status.SUCCEEDED);
        if (result != null) {
            for (ClusterList.ClusterElement element : result.getElements()) {
                System.out.println(element.name);
            }
        }
    }

    private void submitCluster(String clusterName, String configFile) {
        APIResult result = client.submitCluster(clusterName, configFile);
        printResult("Cluster submit of " + clusterName, result);
    }

    @Override
    protected Options createOptions() {
        Options options = new Options();
        options.addOption(new Option(SUBMIT, "Submit this cluster"));
        options.addOption(OptionBuilder.withArgName("file path").hasArg()
                .withDescription("File containing cluster configuration").create(CONFIG));
        options.addOption(new Option(HELP, "Prints command usage"));
        options.addOption(new Option(LIST, "Lists the clusters submitted"));
        options.addOption(new Option(STATUS, "Prints cluster's status"));
        options.addOption(new Option(PAIR, "Pairs local cluster with remote cluster"));
        options.addOption(new Option(DELETE, "Deletes cluster"));
        return options;
    }
}
