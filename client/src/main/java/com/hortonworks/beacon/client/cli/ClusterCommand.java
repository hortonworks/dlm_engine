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

import com.hortonworks.beacon.client.entity.Cluster;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.ClusterList;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Handles cluster commands like submit, pair etc.
 */
public class ClusterCommand extends CommandBase {
    public static final String PAIR = "pair";
    public static final String UNPAIR = "unpair";
    public static final String UPDATE = "update";
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
        } else if (cmd.hasOption(GET)) {
            checkOptionValue(clusterName);
            printClusterDefinition();
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
        } else if (cmd.hasOption(UPDATE)) {
            checkOptionValue(clusterName);
            checkOptionValue(cmd, CONFIG);
            updateCluster(cmd.getOptionValue(CONFIG));
        } else {
            System.out.println("Operation is not recognised");
            printUsage();
        }
    }

    private void printClusterDefinition() throws BeaconClientException {
        Cluster cluster = client.getCluster(clusterName);
        System.out.println(ReflectionToStringBuilder.toString(cluster, ToStringStyle.MULTI_LINE_STYLE));
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
        System.out.println("Cluster get: beacon -cluster <cluster name> -get");
        System.out.println("Cluster status: beacon -cluster <cluster name> -status");
        System.out.println("Cluster delete: beacon -cluster <cluster name> -delete");
        System.out.println("Cluster update: beacon -cluster <cluster name> -update -config <config file path>");
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

    private void updateCluster(String configFile) throws BeaconClientException {
        client.updateCluster(clusterName, configFile);
        printResult("Cluster update of " + clusterName);
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
        options.addOption(new Option(UPDATE, "Updates cluster"));
        options.addOption(new Option(GET, "Gets cluster entity definition"));
        return options;
    }
}
