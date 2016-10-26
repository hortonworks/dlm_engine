package com.hortonworks.beacon.cli;

import com.hortonworks.beacon.client.BeaconClient;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Entity extension to Beacon Command Line Interface - wraps the RESTful API for entities.
 */
public class BeaconEntityCLI extends BeaconCLI {

    public BeaconEntityCLI() throws Exception {
        super();
    }

    public Options createClusterOptions() {

        Options clusterOptions = new Options();

        Option submit = new Option(BeaconCLIConstants.SUBMIT_OPT, false, BeaconCLIConstants.SUBMIT_OPT_DESCRIPTION);
        /* TODO implement */

        return clusterOptions;
    }

    public void clusterCommand(CommandLine commandLine, BeaconClient client) throws IOException {
        Set<String> optionsList = new HashSet<String>();
        for (Option option : commandLine.getOptions()) {
            optionsList.add(option.getOpt());
        }
        String result = null;
        /* TODO implement */

        OUT.get().println(result);
    }

    public Options createPolicyOptions() {

        Options clusterOptions = new Options();

        Option submit = new Option(BeaconCLIConstants.SUBMIT_OPT, false, BeaconCLIConstants.SUBMIT_OPT_DESCRIPTION);
        /* TODO implement */

        return clusterOptions;
    }

    public void policyCommand(CommandLine commandLine, BeaconClient client) throws IOException {
        Set<String> optionsList = new HashSet<String>();
        for (Option option : commandLine.getOptions()) {
            optionsList.add(option.getOpt());
        }
        String result = null;
        /* TODO implement */

        OUT.get().println(result);
    }



}