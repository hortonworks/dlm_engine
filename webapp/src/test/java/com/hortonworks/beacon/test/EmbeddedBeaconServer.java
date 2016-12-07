package com.hortonworks.beacon.test;

import com.google.common.io.Resources;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.Engine;
import com.hortonworks.beacon.entity.store.ConfigurationStore;
import com.hortonworks.beacon.scheduler.BeaconQuartzScheduler;
import com.hortonworks.beacon.store.BeaconStore;
import org.apache.commons.io.FileUtils;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.webapp.WebAppContext;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Properties;

public class EmbeddedBeaconServer {

    private static Server server;

    private void startBeaconServer(String configStore, short port, String hostname, String localCluster) throws Exception {
        FileUtils.deleteDirectory(new File(configStore));
        BeaconConfig config = BeaconConfig.getInstance();
        Engine engine = config.getEngine();
        engine.setConfigStoreUri(configStore);
        engine.setPort(port);
        engine.setHostName(hostname);
        engine.setLocalClusterName(localCluster);

        Connector connector = new SocketConnector();
        connector.setPort(engine.getPort());
        connector.setHost(engine.getHostName());
        connector.setHeaderBufferSize(engine.getSocketBufferSize());
        connector.setRequestBufferSize(engine.getSocketBufferSize());

        server = new Server();
        server.addConnector(connector);
        WebAppContext application = new WebAppContext("../"+engine.getAppPath(), "/");
        application.setParentLoaderPriority(true);
        server.setHandler(application);
        server.start();
        ConfigurationStore.getInstance().init();
        BeaconQuartzScheduler.get().startScheduler();
        BeaconStore.getInstance().init();
    }


    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("properties file missing for embedded beacon server.");
        }

        Properties prop = getProperties(args[0]);
        EmbeddedBeaconServer server = new EmbeddedBeaconServer();
        server.startBeaconServer(prop.getProperty("beacon.config.store"),
                Short.parseShort(prop.getProperty("beacon.port")),
                prop.getProperty("beacon.host"),
                prop.getProperty("beacon.local.cluster"));
    }

    private static Properties getProperties(String propFile) throws IOException {
        URL resource = Resources.getResource(propFile);
        Properties prop = new Properties();
        BufferedReader reader = new BufferedReader(new InputStreamReader(resource.openStream()));
        prop.load(reader);
        return prop;
    }
}