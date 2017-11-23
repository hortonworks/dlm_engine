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
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.resource.PolicyInstanceList;
import com.hortonworks.beacon.client.resource.PolicyList;
import com.hortonworks.beacon.client.resource.StatusResult;

/**
 * Policy command that handles policy operations like submit, schedule, instance list.
 */
public class PolicyCommand extends CommandBase {
    private static final String ABORT = "abort";
    private static final String RERUN = "rerun";
    private static final String INSTANCE_LIST = "instancelist";
    private final String policyName;

    PolicyCommand(String cmd, BeaconClient client, String policyName) {
        super(cmd, client);
        this.policyName = policyName;
    }

    @Override
    protected void processCommand(CommandLine cmd, String[] originalArgs) {
        if (cmd.hasOption(SUBMIT_SCHEDULE)) {
            submitAndSchedule(cmd.getOptionValue(CONFIG));
        } else if (cmd.hasOption(LIST)) {
            listPolicies();
        } else if (cmd.hasOption(HELP)) {
            printUsage();
        } else if (cmd.hasOption(STATUS)) {
            printStatus();
        } else if (cmd.hasOption(DELETE)) {
            delete();
        } else if (cmd.hasOption(INSTANCE_LIST)) {
            listInstances();
        } else {
            printUsage();
        }
    }

    private void listInstances() {
        PolicyInstanceList instances = client.listPolicyInstances(policyName);
        System.out.println("Start time \t Status \t End time \t Tracking Info");
        for (PolicyInstanceList.InstanceElement element : instances.getElements()) {
            System.out.println(element.startTime + "\t" + element.status + "\t" + element.endTime + "\t"
                    + element.trackingInfo);
        }
    }

    @Override
    protected void printUsage() {
        System.out.println("Policy submit: beacon -policy <policy name> -submit -config <config file path>");
        System.out.println("Policy schedule: beacon -policy <policy name> -schedule");
        System.out.println("Policy submit and schedule: beacon -policy <policy name> -submitSchedule "
                + "-config <config file path>");
        System.out.println("Policy list: beacon -policy -list");
        System.out.println("Policy status: beacon -policy <policy name> -status");
        System.out.println("Policy delete: beacon -policy <policy name> -delete");
        System.out.println("Policy instance list: beacon -policy <policy name> -instancelist");
        super.printUsage();
    }

    private void delete() {
        APIResult result = client.deletePolicy(policyName, false);
        printResult("Delete of policy " + policyName, result);
    }

    private void printStatus() {
        StatusResult result = client.getPolicyStatus(policyName);
        printResult("Status of policy " + policyName, result);
    }

    private void listPolicies() {
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

    private void submitAndSchedule(String configFile) {
        APIResult result = client.submitAndScheduleReplicationPolicy(policyName, configFile);
        printResult("Submit and schedule of policy " + policyName, result);
    }

    @Override
    protected Options createOptions() {
        Options options = new Options();
        options.addOption(new Option(SUBMIT_SCHEDULE, "Submit and schedule policy"));
        options.addOption(OptionBuilder.withArgName("file path").hasArg()
                .withDescription("File containing policy configuration").create(CONFIG));
        options.addOption(new Option(LIST, "Lists the policies submitted"));
        options.addOption(new Option(STATUS, "Prints policy's status"));
        options.addOption(new Option(DELETE, "Deletes policy"));
        options.addOption(new Option(HELP, "Prints command usage"));
        options.addOption(new Option(INSTANCE_LIST, "Lists the instances for the policy"));
//        options.addOption(new Option(ABORT, "Aborts the last instance"));
//        options.addOption(new Option(RERUN, "Reruns the last instance"));
        return options;
    }
}
