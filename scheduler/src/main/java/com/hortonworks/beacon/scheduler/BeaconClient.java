/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.scheduler;

import com.hortonworks.beacon.scheduler.hive.HiveDRProperties;
import org.apache.commons.lang3.StringUtils;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class BeaconClient {
    private static final Logger LOG = LoggerFactory.getLogger(BeaconClient.class);

    private File getDRPropertyFilePath(String drPropertiesFile) throws IOException {
        File drPropertiesFilePath = null;
        if (StringUtils.isNotBlank(drPropertiesFile)) {
            File file = new File(drPropertiesFile);
            if (file.exists()) {
                drPropertiesFilePath = new File(file.getAbsoluteFile().getParentFile().getAbsolutePath()
                        + File.separator + drPropertiesFile);
            }
        }

        return drPropertiesFilePath;
    }

    private InputStream getResourceAsStream(File fileToLoad) throws FileNotFoundException {
        InputStream resourceAsStream = null;
        if (fileToLoad.exists() && fileToLoad.isFile() && fileToLoad.canRead()) {
            resourceAsStream = new FileInputStream(fileToLoad);
        }
        return resourceAsStream;
    }

    private Properties doLoadProperties(InputStream resourceAsStream, String drPropertyFile) throws IOException {
        Properties properties = null;
        try {
            properties = new Properties();
            properties.load(resourceAsStream);
        } catch (IOException e) {
            throw new IOException("Error loading properties file: " + drPropertyFile, e);
        }

        //Validation of Properties file
        //validateDRProperties(properties);

        return properties;
    }

    private void submitAndScheduleReplicationJob(String drPropertiesFile) throws IOException {
        File drPropFile = getDRPropertyFilePath(drPropertiesFile);
        Properties drProperties = null;

        if (!drPropFile.exists()) {
            LOG.error("Error in loading dr property file : {} as it don't exists", drPropFile.getAbsolutePath());
            System.exit(1);
        } else {
            drProperties = doLoadProperties(getResourceAsStream(drPropFile), drPropertiesFile);
        }

        try {
            scheduleReplicationJob(drProperties);
        } catch (SchedulerException se) {
            LOG.error("Scheduler Exception occurred while scheduling Replication job :"+se);
        }
    }

    public void scheduleReplicationJob(Properties drProperties) throws SchedulerException, IOException {

        ReplicationJobDetails details = ReplicationJobFactory.getReplicationType(drProperties.getProperty("type"));

        details.validateReplicationProperties(drProperties);
        details.setReplicationJobDetails(drProperties);
        QuartzReplication quartzReplication = new QuartzReplication();

        quartzReplication.createScheduler();

        quartzReplication.createReplicationJob(drProperties.getProperty(HiveDRProperties.JOB_NAME.getName()), details);
        quartzReplication.scheduleJob(details);
        quartzReplication.startScheduler();
        try {
            Thread.sleep(1000*1000);
        } catch (InterruptedException e) {
            LOG.error("Exception raised while sleeping: " +e);
        }
        quartzReplication.stopScheduler();
    }

    public static void main(String args[]) throws Exception {
        if (args.length < 1) {
            LOG.error("DR Property File is not specified");
            System.exit(1);
        }

        String drPropertiesFile = args[0];
        BeaconClient bc = new BeaconClient();

        bc.submitAndScheduleReplicationJob(drPropertiesFile);

    }
}
