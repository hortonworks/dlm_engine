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

import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.BeaconClientException;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.UserPrivilegesResult;
import com.hortonworks.beacon.client.result.DBListResult;
import com.hortonworks.beacon.client.result.FileListResult;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Handles beacon resource commands like list files, list dbs etc.
 */
public class MiscCommand extends CommandBase {

    private static final String LISTFS = "listfs";
    private static final String LISTDB = "listdb";
    private static final String USER = "user";

    MiscCommand(String cmd, BeaconClient client) {
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
            String dbName = cmd.getOptionValue(LISTDB);
            checkOptionValue(LISTDB, dbName);
            listDBs();
        } else if (cmd.hasOption(USER)) {
            fetchUserPrivileges();
        } else {
            printUsage();
        }
    }

    private void fetchUserPrivileges() throws BeaconClientException {
        UserPrivilegesResult result = client.getUserPrivileges();
        System.out.println(ReflectionToStringBuilder.toString(result, ToStringStyle.MULTI_LINE_STYLE));
    }

    private void checkOptionValue(String option, String value) throws InvalidCommandException {
        if (value == null) {
            throw new InvalidCommandException("Missing option value for -" + option);
        }
    }

    protected void printUsage() {
        System.out.println("Dataset file list: beacon -listfs <path>");
        System.out.println("Dataset db list: beacon -listdb");
        System.out.println("Get user privileges: beacon -user");
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
        options.addOption(OptionBuilder.withArgName("dbname").hasOptionalArg()
                .withDescription("db name").create(LISTDB));
        options.addOption(new Option(USER, "Fetches user privileges"));
        return options;
    }

}
