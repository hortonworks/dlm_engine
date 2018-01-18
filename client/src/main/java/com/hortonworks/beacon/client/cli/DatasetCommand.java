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

import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.result.DBListResult;
import com.hortonworks.beacon.client.result.FileListResult;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

/**
 * Handles beacon resource commands like list files, list dbs etc.
 */
public class DatasetCommand extends CommandBase {

    private static final String LISTFS = "listfs";
    private static final String LISTDB = "listdb";

    DatasetCommand(String cmd, BeaconClient client) {
        super(cmd, client);
    }

    @Override
    protected void processCommand(CommandLine cmd, String[] originalArgs)
            throws InvalidCommandException, BeaconClientException {
        if (cmd.hasOption(LISTFS)) {
            String path = cmd.getOptionValue(LISTFS);
            checkOptionValue(LISTFS, path);
            listFiles(path);
        } else if (cmd.hasOption(LISTDB)) {
            listDBs();
        } else {
            printUsage();
        }
    }

    private void checkOptionValue(String option, String value) throws InvalidCommandException {
        if (value == null) {
            throw new InvalidCommandException("Missing option value for -" + option);
        }
    }

    protected void printUsage() {
        System.out.println("Dataset file list: beacon -dataset -listfs <path>");
        System.out.println("Dataset db list: beacon -dataset -listdb");
        super.printUsage();
    }

    private void listFiles(String path) throws BeaconClientException {
        FileListResult result = client.listFiles(path);
        printResult("Path:" +  path +" listing", result);
    }

    private void listDBs() throws BeaconClientException {
        DBListResult result = client.listDBs();
        printResult("DB Listing", result);
    }

    private void printResult(String operation, DBListResult result) {
        System.out.println(operation + " " + APIResult.Status.SUCCEEDED);
        if (result != null) {
            for (DBListResult.DBList element : (DBListResult.DBList[]) result.getCollection()) {
                System.out.println(element.toString());
            }
        }
    }

    private void printResult(String operation, FileListResult result) {
        System.out.println(operation + " " + APIResult.Status.SUCCEEDED);
        if (result != null) {
            for (FileListResult.FileList element : (FileListResult.FileList[]) result.getCollection()) {
                System.out.println(element.toString());
            }
        }
    }

    @Override
    protected Options createOptions() {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path").hasOptionalArg()
                .withDescription("file list").create(LISTFS));
        options.addOption(new Option(LISTDB, "List the DBs in Hive"));
        return options;
    }

}
