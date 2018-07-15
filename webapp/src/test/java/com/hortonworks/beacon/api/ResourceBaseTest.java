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

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.TestDataGenerator;
import com.hortonworks.beacon.client.BeaconClient;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.testng.annotations.BeforeClass;

/**
 * Base class for tests.
 */
public abstract class ResourceBaseTest {
    /**
     * Enum for source/target.
     */
    public enum ClusterType {
        SOURCE {
            @Override
            public String getClusterName(boolean isLocal) {
                if (isLocal) {
                    return "cluster-local";
                } else {
                    return randomString("cluster");
                }
            }
        },
        TARGET {
            @Override
            public String getClusterName(boolean isLocal) {
                if (isLocal) {
                    return "cluster-target";
                } else {
                    return randomString("cluster");
                }
            }
        };

        public abstract String getClusterName(boolean isLocal);

        public String randomString(String prefix) {
            return prefix + RandomStringUtils.randomAlphanumeric(10);
        }
    }

    protected TestDataGenerator testDataGenerator;

    protected BeaconClient sourceClient;
    protected FileSystem sourceFs;

    @BeforeClass
    public void setup() throws BeaconException {
        System.setProperty("beacon.test.local", "true");
        testDataGenerator = TestDataGenerator.getTestDataGenerator();
        sourceClient = testDataGenerator.getClient(ClusterType.SOURCE);
        sourceFs = testDataGenerator.getFileSystem(ClusterType.SOURCE);
        testDataGenerator.init();
    }
}
