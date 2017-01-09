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

package com.hortonworks.beacon.scheduler.quartz;

import com.google.common.io.Resources;
import com.hortonworks.beacon.config.BeaconConfig;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class QuartzConfig {

    private static final String QUARTZ_PROP_FILE = "quartz.properties";
    private static final String QUARTZ_PROP_NAME = "org.quartz.properties";
    private static final Logger LOG = LoggerFactory.getLogger(QuartzConfig.class);

    static void init() {
        String beaconConfDir = BeaconConfig.getBeaconConfDir(BeaconConfig.getBeaconHome());
        String quartzPropPath = System.getProperty(QUARTZ_PROP_NAME);
        if (StringUtils.isBlank(quartzPropPath)) {
            File quartzPropFile = new File(beaconConfDir, QUARTZ_PROP_FILE);
            quartzPropPath = quartzPropFile.exists()
                    ? quartzPropFile.getAbsolutePath()
                    : Resources.getResource(QUARTZ_PROP_FILE).getPath();
            LOG.info("Quartz properties file to be used [{}]", quartzPropPath);
            System.setProperty(QUARTZ_PROP_NAME, quartzPropPath);
        }
    }
}