/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.servlet;

import com.hortonworks.beacon.config.BeaconConfig;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * ServletContextListener thats called at server start, before filters/servlets are initialized.
 */
public class BeaconServletContextListener implements ServletContextListener {
    private static final Logger LOG = LoggerFactory.getLogger(BeaconServletContextListener.class);

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        LOG.debug("Initializing Beacon");
        //Force loading hadoop conf
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("yarn-default.xml");
        Configuration.addDefaultResource("yarn-site.xml");

        //Force loading of beacon config so that beacon conf directory is set for others to load
        BeaconConfig.getInstance();
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {

    }
}
