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
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.resource.APIResult;

/**
 * Base class for all commands - MainCommand, PolicyCommand and CLusterCommand.
 */
public abstract class CommandBase {
    protected static final String CLUSTER = "cluster";
    protected static final String POLICY = "policy";
    protected static final String CLOUDCRED="cloudcred";

    protected static final String HELP = "help";
    protected static final String URL = "url";
    protected static final String SUBMIT = "submit";
    protected static final String SUBMIT_SCHEDULE = "submitSchedule";
    protected static final String CONFIG = "config";
    protected static final String LIST = "list";
    protected static final String STATUS = "status";
    protected static final String DELETE = "delete";

    private final Options options;
    protected BeaconClient client;
    private String baseCommand;

    CommandBase(String baseCommand, BeaconClient client) {
        this.baseCommand = baseCommand;
        this.client = client;
        this.options = createOptions();
    }

    protected CommandLineParser parser = new IgnoreUnrecognisedOptionParser();

    protected void printUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(getBaseCommand(), options);
    }

    public void processCommand(String[] args) throws BeaconClientException {
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args, false);
            processCommand(cmd, args);
        } catch (ParseException e) {
            System.out.println("Invalid command: " + e.getMessage());
            printUsage();
        } catch (InvalidCommandException e) {
            System.out.println(e.getMessage());
            printUsage();
        }
    }

    protected abstract void processCommand(CommandLine cmd, String[] originalArgs)
            throws InvalidCommandException, BeaconClientException;

    protected abstract Options createOptions();

    protected String getBaseCommand() {
        return baseCommand;
    }

    protected BeaconClient getClient() {
        return client;
    }

    protected void printResult(String operation) {
        System.out.println(operation + " " + APIResult.Status.SUCCEEDED.name());
    }

    protected void printResult(String operation, Entity.EntityStatus status) {
        //TODO result doesn't have API status?
        System.out.println(operation + " status: " + status);
    }

    protected void checkOptionValue(CommandLine cmd, String option) throws InvalidCommandException {
        if (!cmd.hasOption(option) || cmd.getOptionValue(option) == null) {
            throw new InvalidCommandException("Missing option/option value for  -" + option);
        }
    }
}
