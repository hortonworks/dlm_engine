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
import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.client.resource.PolicyList;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Policy command that handles policy operations like submit, schedule, instance list.
 */
public class PolicyCommand extends CommandBase {
    private static final String ABORT = "abort";
    private static final String DRYRUN = "dryrun";
    private static final String RERUN = "rerun";
    private static final String INSTANCE_LIST = "instancelist";
    private static final String LOGS = "logs";
    private static final String ID = "id";
    private final String policyName;

    PolicyCommand(String cmd, BeaconClient client, String policyName) {
        super(cmd, client);
        this.policyName = policyName;
    }

    @Override
    protected void processCommand(CommandLine cmd, String[] originalArgs)
            throws InvalidCommandException, BeaconClientException {
        if (cmd.hasOption(SUBMIT_SCHEDULE)) {
            checkOptionValue(policyName);
            checkOptionValue(cmd, CONFIG);
            submitAndSchedule(cmd.getOptionValue(CONFIG));
        } else if (cmd.hasOption(DRYRUN)) {
            checkOptionValue(policyName);
            checkOptionValue(cmd, CONFIG);
            dryrun(cmd.getOptionValue(CONFIG));
        } else if (cmd.hasOption(LIST)) {
            listPolicies();
        } else if (cmd.hasOption(HELP)) {
            printUsage();
        } else if (cmd.hasOption(STATUS)) {
            checkOptionValue(policyName);
            printStatus();
        } else if (cmd.hasOption(GET)) {
            checkOptionValue(policyName);
            printPolicy();
        } else if (cmd.hasOption(DELETE)) {
            checkOptionValue(policyName);
            delete();
        } else if (cmd.hasOption(INSTANCE_LIST)) {
            checkOptionValue(policyName);
            listInstances();
        } else if (cmd.hasOption(ABORT)) {
            checkOptionValue(policyName);
            abortInstance();
        } else if (cmd.hasOption(LOGS)) {
            if (cmd.hasOption(ID)) {
                checkOptionValue(ID, cmd.getOptionValue(ID));
                fetchLogsById(cmd.getOptionValue(ID));
            } else {
                checkOptionValue(policyName);
                fetchLogsByName();
            }
        } else {
            System.out.println("Operation is not recognised");
            printUsage();
        }
    }

    private void printPolicy() throws BeaconClientException {
        ReplicationPolicy policy = client.getPolicy(policyName);
        System.out.println(ReflectionToStringBuilder.toString(policy, ToStringStyle.MULTI_LINE_STYLE));
    }

    private void fetchLogsById(String policyId) throws BeaconClientException {
        String logs = client.getPolicyLogsForId(policyId);
        System.out.println("Logs of policy id " + policyId + ":");
        System.out.println(logs);
    }

    private void fetchLogsByName() throws BeaconClientException {
        String logs = client.getPolicyLogs(policyName);
        System.out.println("Logs of policy " + policyName + ":");
        System.out.println(logs);
    }

    private void checkOptionValue(String localPolicyName) throws InvalidCommandException {
        checkOptionValue(POLICY, localPolicyName);
    }

    private void checkOptionValue(String option, String optionValue) throws InvalidCommandException {
        if (optionValue == null) {
            throw new InvalidCommandException("Missing option value for -" + option);
        }
    }

    private void listInstances() throws BeaconClientException {
        PolicyInstanceList instances = client.listPolicyInstances(policyName);
        System.out.println("Start time \t Status \t End time \t Tracking Info");
        for (PolicyInstanceList.InstanceElement element : instances.getElements()) {
            System.out.println(element.startTime + "\t" + element.status + "\t" + element.endTime + "\t"
                    + element.trackingInfo);
        }
    }

    private void abortInstance() throws BeaconClientException {
        client.abortPolicyInstance(policyName);
        printResult("Abort of instance for policy" + policyName);
    }

    @Override
    protected void printUsage() {
        super.printUsage();
        System.out.println("Available operations are:");
        System.out.println("Policy submit and schedule: beacon -policy <policy name> -submitSchedule "
                + "-config <config file path>");
        System.out.println("Policy list: beacon -policy -list");
        System.out.println("Policy get: beacon -policy <policy name> -get");
        System.out.println("Policy status: beacon -policy <policy name> -status");
        System.out.println("Policy delete: beacon -policy <policy name> -delete");
        System.out.println("Policy instance list: beacon -policy <policy name> -instancelist");
        System.out.println("Policy abort instance: beacon -policy <policy name> -abort");
        System.out.println("Policy logs: beacon -policy [<policy name>] -logs [-id <policy id>]");
    }

    private void delete() throws BeaconClientException {
        client.deletePolicy(policyName, false);
        printResult("Delete of policy " + policyName);
    }

    private void printStatus() throws BeaconClientException {
        Entity.EntityStatus entityStatus = client.getPolicyStatus(policyName);
        printResult("Status of policy " + policyName, entityStatus);
    }

    private void listPolicies() throws BeaconClientException {
        //TODO add sane defaults
        //TODO handle pagination?
        //TODO result doesn't have API status?
        PolicyList result = client.getPolicyList("name", "name", null, "ASC", 0, 10);
        printResult("Policy list", result);
    }

    private void printResult(String operation, PolicyList result) {
        System.out.println(operation + " " + APIResult.Status.SUCCEEDED);
        if (result != null) {
            for (PolicyList.PolicyElement element : result.getElements()) {
                //TODO display more fields
                System.out.println(element.name);
            }
        }
    }

    private void submitAndSchedule(String configFile) throws BeaconClientException {
        client.submitAndScheduleReplicationPolicy(policyName, configFile);
        printResult("Submit and schedule of policy " + policyName);
    }

    private void dryrun(String configFile) throws BeaconClientException {
        client.dryrunPolicy(policyName, configFile);
        printResult("Dry-run of policy " + policyName);
    }

    @Override
    protected Options createOptions() {
        Options options = new Options();
        options.addOption(new Option(SUBMIT_SCHEDULE, "Submit and schedule policy"));
        options.addOption(new Option(DRYRUN, "Performs a dry run on a new policy"));
        options.addOption(OptionBuilder.withArgName("file path").hasArg()
                .withDescription("File containing policy configuration").create(CONFIG));
        options.addOption(new Option(LIST, "Lists the policies submitted"));
        options.addOption(new Option(STATUS, "Prints policy's status"));
        options.addOption(new Option(GET, "Prints policy definition"));
        options.addOption(new Option(DELETE, "Deletes policy"));
        options.addOption(new Option(HELP, "Prints command usage"));
        options.addOption(new Option(INSTANCE_LIST, "Lists the instances for the policy"));
        options.addOption(new Option(ABORT, "Aborts the instances of a policy"));
        options.addOption(new Option(LOGS, "Fetches logs for policy"));
        options.addOption(OptionBuilder.withArgName("policy id").hasArg()
                .withDescription("Policy id").create(ID));
//        options.addOption(new Option(ABORT, "Aborts the last instance"));
//        options.addOption(new Option(RERUN, "Reruns the last instance"));
        return options;
    }
}
