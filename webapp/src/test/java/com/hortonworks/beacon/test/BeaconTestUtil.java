/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.test;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.tools.BeaconDBSetup;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Properties;

/**
 * Utility class for tests.
 */
public final class BeaconTestUtil {

    private BeaconTestUtil() {
    }

    static void createDBSchema() throws Exception {
        String currentDir = System.getProperty("user.dir");
        File hsqldbFile = new File(currentDir, "../src/sql/tables_hsqldb.sql");
        BeaconConfig.getInstance().getDbStore().setSchemaDirectory(hsqldbFile.getParent());
        BeaconDBSetup.setupDB();
    }

    static Properties getProperties(String propFile) throws IOException {
        URL resource = BeaconTestUtil.class.getResource("/" + propFile);
        Properties prop = new Properties();
        BufferedReader reader = new BufferedReader(new InputStreamReader(resource.openStream()));
        prop.load(reader);
        return prop;
    }
}
