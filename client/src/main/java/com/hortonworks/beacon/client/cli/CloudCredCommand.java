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
import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.resource.CloudCredList;
import com.hortonworks.beacon.client.util.CloudCredBuilder;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.PropertiesIgnoreCase;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Cloudcred command that handles cloudcred operations like submit, delete, update, validate, list.
 */
public class CloudCredCommand extends CommandBase {
    private static final String GET = "get";
    private static final String UPDATE = "update";
    private static final String VALIDATE = "validate";
    private static final String PATH = "path";
    private final String cloudCredID;

    CloudCredCommand(String cmd, BeaconClient client, String cloudCredID) {
        super(cmd, client);
        this.cloudCredID = cloudCredID;
    }

    @Override
    protected void processCommand(CommandLine cmd, String[] originalArgs)
            throws InvalidCommandException, BeaconClientException {
        if (cmd.hasOption(SUBMIT)) {
            checkOptionValue(cmd, CONFIG);
            submit(cmd.getOptionValue(CONFIG));
        } else if (cmd.hasOption(UPDATE)) {
            checkOptionValue(cloudCredID);
            checkOptionValue(cmd, CONFIG);
            update(cmd.getOptionValue(CONFIG));
        } else if (cmd.hasOption(LIST)) {
            listCloudCredEntities();
        } else if (cmd.hasOption(DELETE)) {
            checkOptionValue(cloudCredID);
            delete();
        } else if (cmd.hasOption(GET)) {
            checkOptionValue(cloudCredID);
            get();
        } else if (cmd.hasOption(VALIDATE)) {
            checkOptionValue(cloudCredID);
            checkOptionValue(cmd, PATH);
            validate(cmd.getOptionValue(PATH));
        } else {
            printUsage();
        }
    }

    private void checkOptionValue(String localPolicyName) throws InvalidCommandException {
        if (localPolicyName == null) {
            throw new InvalidCommandException("Missing option value for -policy");
        }
    }

    @Override
    protected void printUsage() {
        System.out.println("CloudCred submit: beacon -cloudcred -submit -config <config file path>");
        System.out.println("CloudCred update: beacon -cloudcred <cloudcred id>  -update -config <config file path>");
        System.out.println("CloudCred list: beacon -cloudcred -list");
        System.out.println("CloudCred delete: beacon -cloudcred <cloudcred id> -delete");
        System.out.println("CloudCred get: beacon -cloudcred <cloudcred id> get");
        System.out.println("CloudCred validate: beacon -cloudcred <cloudcred id> -validate -path <cloud path>");
        super.printUsage();
    }

    private void delete() throws BeaconClientException {
        client.deleteCloudCred(cloudCredID);
        printResult("Delete of cloudCred " + cloudCredID);
    }

    private void listCloudCredEntities() throws BeaconClientException {
        CloudCredList result = client.listCloudCred("name", "name", "ASC", 0,  10);
        printResult("CloudCred list", result);
    }

    private void printResult(String operation, CloudCredList result) {
        if (result == null) {
            return;
        }
        System.out.println("Name \tProvider \tAuth Type \tID");
        for (CloudCred cloudCred : result.getCloudCreds()) {
            System.out.println(cloudCred.getName() + "\t" + cloudCred.getProvider() + "\t" + cloudCred.getAuthType()
                    + "\t" + cloudCred.getId());
        }
    }

    private void submit(String configFile) throws BeaconClientException {
        PropertiesIgnoreCase configProps = createConfigProperties(configFile);
        CloudCred cloudCred = null;
        try {
            cloudCred = CloudCredBuilder.buildCloudCred(configProps);
        } catch (BeaconException e) {
            System.out.println("Unable to build cloudCred with given config file:"+configFile);
            throw  new BeaconClientException(e);
        }
        String id = client.submitCloudCred(cloudCred);
        printResult("Submit a CloudCred entity");
        System.out.println("CloudCred ID: " + id);
    }

    private void update(String configFile) throws BeaconClientException {
        PropertiesIgnoreCase configProps = createConfigProperties(configFile);
        CloudCred cloudCred = null;
        try {
            cloudCred = CloudCredBuilder.buildCloudCred(configProps);
        } catch (BeaconException e) {
            System.out.println("Unable to build cloudCred with given config file:"+configFile);
            throw  new BeaconClientException(e);
        }
        cloudCred.setId(cloudCredID);
        client.updateCloudCred(cloudCredID, cloudCred);
        printResult("Update a CloudCred entity");
    }

    private void get() throws BeaconClientException {
        CloudCred cloudCred = client.getCloudCred(cloudCredID);
        System.out.println(ReflectionToStringBuilder.toString(cloudCred, ToStringStyle.MULTI_LINE_STYLE));
    }

    private void validate(String path) throws BeaconClientException {
        client.validateCloudPath(cloudCredID, path);
        printResult("Validate a CloudCred entity");
    }

    private PropertiesIgnoreCase createConfigProperties(String configFile) throws BeaconClientException {
        FileInputStream configFileIS = null;
        PropertiesIgnoreCase configProps = null;
        try {
            configFileIS = new FileInputStream(new File(configFile));
            configProps = new PropertiesIgnoreCase();
            configProps.load(configFileIS);
        } catch (FileNotFoundException e) {
            System.out.println("Config file is not present:"+configFile);
            throw  new BeaconClientException(new BeaconException(e));
        } catch (IOException e) {
            System.out.println("Unable to load config file:"+configFile);
            throw  new BeaconClientException(new BeaconException(e));

        } finally {
            try {
                if (configFileIS != null) {
                    configFileIS.close();
                }
            } catch (IOException e) {
                System.out.println("Could not close the config file:"+configFile);
            }
        }
        return configProps;
    }

    @Override
    protected Options createOptions() {
        Options options = new Options();
        options.addOption(new Option(SUBMIT, "Submits CloudCred entity"));
        options.addOption(OptionBuilder.withArgName("file path").hasArg()
                .withDescription("File containing CloudCred config properties").create(CONFIG));
        options.addOption(new Option(UPDATE, "Updates CloudCred entity"));
        options.addOption(new Option(LIST, "Lists the cloudCreds submitted"));
        options.addOption(new Option(DELETE, "Deletes cloudCred"));
        options.addOption(new Option(GET, "Gets cloudCred"));
        options.addOption(new Option(VALIDATE, "Validates cloudCred"));
        options.addOption(OptionBuilder.withArgName("cloud path").hasArg()
                .withDescription("Cloud path to validate the CloudCred entity against").create(PATH));
        options.addOption(new Option(HELP, "Prints command usage"));
        return options;
    }
}
