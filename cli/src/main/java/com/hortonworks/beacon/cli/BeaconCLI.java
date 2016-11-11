package com.hortonworks.beacon.cli;

import com.hortonworks.beacon.cli.parser.CLIParser;
import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.client.BeaconClientException;
import com.sun.jersey.api.client.ClientHandlerException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static com.hortonworks.beacon.cli.BeaconCLIConstants.BEACON_URL;

public class BeaconCLI {
    public static final AtomicReference<PrintStream> ERR = new AtomicReference<PrintStream>(System.err);
    public static final AtomicReference<PrintStream> OUT = new AtomicReference<PrintStream>(System.out);

    private final Properties clientProperties;

    public BeaconCLI() throws Exception {
        clientProperties = getClientProperties();
    }

    /**
     * Entry point for the Beacon CLI when invoked from the command line. Upon
     * completion this method exits the JVM with '0' (success) or '-1'
     * (failure).
     *
     * @param args options and arguments for the Beacon CLI.
     */
    public static void main(final String[] args) throws Exception {
        System.exit(new BeaconCLI().run(args));
    }


    // TODO help and headers
    private static final String[] BEACON_HELP = { "the env variable '" + BEACON_URL
            + "' is used as default value for the '-"
            + BeaconCLIConstants.URL_OPTION + "' option",
            "custom headers for Beacon web services can be specified using '-D"
                    + BeaconCLIConstants.WS_HEADER_PREFIX + "NAME=VALUE'", };

    /**
     * Run a CLI programmatically.
     * <p/>
     * It does not exit the JVM.
     * <p/>
     * A CLI instance can be used only once.
     *
     * @param args options and arguments for the Oozie CLI.
     * @return '0' (success), '-1' (failure).
     */
    public synchronized int run(final String[] args) throws Exception {

        CLIParser parser = new CLIParser("beacon", BEACON_HELP);

        BeaconEntityCLI entityCLI = new BeaconEntityCLI();

        parser.addCommand(BeaconCLIConstants.HELP_CMD, "", "display usage", new Options(), false);
        parser.addCommand(BeaconCLIConstants.CLUSTER_CMD, "",
                "Cluster operations like submit, delete, status, get, list",
                entityCLI.createClusterOptions(), false);
        parser.addCommand(BeaconCLIConstants.POLICY_CMD, "",
                "Entity operations like submit, suspend, resume, delete, status, get, list, submitAndSchedule",
                entityCLI.createPolicyOptions(), false);

        try {
            CLIParser.Command command = parser.parse(args);
            int exitValue = 0;
            if (command.getName().equals(BeaconCLIConstants.HELP_CMD)) {
                parser.showHelp();
            } else {
                CommandLine commandLine = command.getCommandLine();
                String beaconUrl = getBeaconEndpoint(commandLine);
                BeaconClient client = new BeaconClient(beaconUrl, clientProperties);

                setDebugMode(client, commandLine.hasOption(BeaconCLIConstants.DEBUG_OPTION));
                if (command.getName().equals(BeaconCLIConstants.CLUSTER_CMD)) {
                    entityCLI.clusterCommand(commandLine, client);
                } else if (command.getName().equals(BeaconCLIConstants.POLICY_CMD)) {
                    entityCLI.policyCommand(commandLine, client);
                }
            }
            return exitValue;
        } catch (ParseException ex) {
            ERR.get().println("Invalid sub-command: " + ex.getMessage());
            ERR.get().println();
            ERR.get().println(parser.shortHelp());
            ERR.get().println("Stacktrace:");
            ex.printStackTrace();
            return -1;
        } catch (ClientHandlerException ex) {
            ERR.get().print("Unable to connect to Beacon server, "
                    + "please check if the URL is correct and Beacon server is up and running\n");
            ERR.get().println("Stacktrace:");
            ex.printStackTrace();
            return -1;
        } catch (BeaconClientException e) {
            ERR.get().println("ERROR: " + e.getMessage());
            return -1;
        } catch (Exception ex) {
            ERR.get().println("Stacktrace:");
            ex.printStackTrace();
            return -1;
        }
    }

    protected Integer parseIntegerInput(String optionValue, Integer defaultVal, String optionName) {
        Integer integer = defaultVal;
        if (optionValue != null) {
            try {
                return Integer.parseInt(optionValue);
            } catch (NumberFormatException e) {
                throw new BeaconClientException("Input value provided for queryParam \""+ optionName
                        +"\" is not a valid Integer");
            }
        }
        return integer;
    }

    protected String getColo(String colo) throws IOException {
        if (colo == null) {
            Properties prop = getClientProperties();
            colo = prop.getProperty(BeaconCLIConstants.CURRENT_COLO, "*");
        }
        return colo;
    }

    protected String getBeaconEndpoint(CommandLine commandLine) throws IOException {
        String url = commandLine.getOptionValue(BeaconCLIConstants.URL_OPTION);
        if (url == null) {
            url = System.getenv(BEACON_URL);
        }
        if (url == null) {
            if (clientProperties.containsKey("beacon.url")) {
                url = clientProperties.getProperty("beacon.url");
            }
        }
        if (url == null) {
            throw new BeaconClientException("Failed to get beacon url from cmdline, or environment or client properties");
        }

        return url;
    }

    private void setDebugMode(BeaconClient client, boolean debugOpt) {
        String debug = System.getenv(BeaconCLIConstants.ENV_BEACON_DEBUG);
        if (debugOpt) {  // CLI argument "-debug" used
            client.setDebugMode(true);
        } else if (StringUtils.isNotBlank(debug)) {
            System.out.println(BeaconCLIConstants.ENV_BEACON_DEBUG + ": " + debug);
            if (debug.trim().toLowerCase().equals("true")) {
                client.setDebugMode(true);
            }
        }
    }

    private Properties getClientProperties() throws IOException {
        InputStream inputStream = null;
        try {
            inputStream = BeaconCLI.class.getResourceAsStream(BeaconCLIConstants.CLIENT_PROPERTIES);
            Properties prop = new Properties();
            if (inputStream != null) {
                prop.load(inputStream);
            }
            return prop;
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }
}
