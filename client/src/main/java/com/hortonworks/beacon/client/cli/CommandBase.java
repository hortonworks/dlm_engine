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
    protected static final String GET = "get";

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
