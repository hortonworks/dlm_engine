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

import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.ClusterList;

/**
 * Handles cluster commands like submit, pair etc.
 */
public class ClusterCommand extends CommandBase {
    public static final String PAIR = "pair";
    public static final String UNPAIR = "unpair";
    private final String clusterName;

    ClusterCommand(String cmd, BeaconClient client, String clusterName) {
        super(cmd, client);
        this.clusterName = clusterName;
    }

    @Override
    protected void processCommand(CommandLine cmd, String[] originalArgs)
            throws InvalidCommandException, BeaconClientException {
        if (cmd.hasOption(SUBMIT)) {
            checkOptionValue(clusterName);
            checkOptionValue(cmd, CONFIG);
            submitCluster(cmd.getOptionValue(CONFIG));
        } else if (cmd.hasOption(LIST)) {
            listClusters();
        } else if (cmd.hasOption(HELP)) {
            printUsage();
        } else if (cmd.hasOption(STATUS)) {
            checkOptionValue(clusterName);
            printStatus();
        } else if (cmd.hasOption(PAIR)) {
            checkOptionValue(clusterName);
            pair();
        } else if (cmd.hasOption(UNPAIR)) {
            checkOptionValue(clusterName);
            unpair();
        } else if (cmd.hasOption(DELETE)) {
            checkOptionValue(clusterName);
            delete();
        } else {
            System.out.println("Operation is not recognised");
            printUsage();
        }
    }

    private void unpair() throws BeaconClientException {
        client.unpairClusters(clusterName, false);
        printResult("Cluster unpairing with " + clusterName + " cluster");
    }

    private void checkOptionValue(String localClusterName) throws InvalidCommandException {
        if (localClusterName == null) {
            throw new InvalidCommandException("Missing option value for -cluster");
        }
    }

    @Override
    protected void printUsage() {
        super.printUsage();
        System.out.println("Available operations are:");
        System.out.println("Cluster submit: beacon -cluster <cluster name> -submit -config <config file path>");
        System.out.println("Cluster status: beacon -cluster <cluster name> -status");
        System.out.println("Cluster delete: beacon -cluster <cluster name> -delete");
        System.out.println("Cluster pairing: beacon -cluster <remote cluster name> -pair");
        System.out.println("Cluster unpairing: beacon -cluster <remote cluster name> -unpair");
        System.out.println("Cluster list: beacon -cluster -list");
    }

    private void delete() throws BeaconClientException {
        client.deleteCluster(clusterName);
        printResult("Delete of cluster " + clusterName);
    }

    private void pair() throws BeaconClientException {
        client.pairClusters(clusterName, false);
        printResult("Cluster pairing with " + clusterName + " cluster");
    }

    private void printStatus() throws BeaconClientException {
        Entity.EntityStatus entityStatus = client.getClusterStatus(clusterName);
        printResult("Cluster " + clusterName + "'", entityStatus);
    }

    private void listClusters() throws BeaconClientException {
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

    private void submitCluster(String configFile) throws BeaconClientException {
        client.submitCluster(clusterName, configFile);
        printResult("Cluster submit of " + clusterName);
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
        options.addOption(new Option(UNPAIR, "Removes pairing of local cluster with remote cluster"));
        options.addOption(new Option(DELETE, "Deletes cluster"));
        return options;
    }
}
