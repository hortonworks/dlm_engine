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
package com.hortonworks.beacon.replication.hive;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.entity.util.ReplicationDistCpOption;
import org.junit.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Properties;

/**
 * Test for Hive Replication Policy helper.
 */
public class HivePolicyHelperTest {

    @Test(dataProvider = "getTestProperties")
    public void testDistcpOptions(Properties properties, String[] availablePropList) {
        Map<String, String> distcpOptions =   HivePolicyHelper.getDistcpOptions(properties);
        Assert.assertEquals(distcpOptions.entrySet().size(), availablePropList.length);
        for (String availableProp : availablePropList) {
            Assert.assertNotNull(distcpOptions.get(availableProp));
        }
    }

    @DataProvider
    private Object[][] getTestProperties() {
        return new Object[][] {
            {new Properties(), new String[]{"distcp.options.pugpb"}},
            {getFileSystemWithACLAndXAttrEnabledProperties(), new String[]{"distcp.options.pugpbax"}},
            {getFileSystemWithACLEnabledProperties(), new String[]{"distcp.options.pugpba"}},
            {getFileSystemWithXAttrEnabledProperties(), new String[]{"distcp.options.pugpbx"}},
            {getCloudDataLakeProperties(), new String[]{"distcp.options.pugpb"}},
            {getTDEEnabledWithSameKeyProperties(), new String[]{"distcp.options.pugpbax"}},
            {getTDEEnabledWithDifferentKeyProperties(), new String[]{"distcp.options.pugpbax",
                "distcp.options.skipcrccheck",
                "distcp.options.update",
            }, },
        };
    }

    private Object getFileSystemWithACLAndXAttrEnabledProperties() {
        Properties defaultProp = new Properties();
        defaultProp.putAll((Properties) getFileSystemWithACLEnabledProperties());
        defaultProp.putAll((Properties) getFileSystemWithXAttrEnabledProperties());
        return defaultProp;
    }

    private Object getFileSystemWithACLEnabledProperties() {
        Properties defaultProp = new Properties();
        defaultProp.setProperty(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_ACL.getName(), "true");
        return defaultProp;
    }

    private Object getFileSystemWithXAttrEnabledProperties() {
        Properties defaultProp = new Properties();
        defaultProp.setProperty(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_XATTR.getName(), "true");
        return defaultProp;
    }

    private Object getCloudDataLakeProperties() {
        Properties defaultProp = new Properties();
        defaultProp.setProperty(Cluster.ClusterFields.CLOUDDATALAKE.getName(), "true");
        return defaultProp;
    }

    private Object getTDEEnabledWithSameKeyProperties() {
        Properties defaultProp = new Properties((Properties) getFileSystemWithACLAndXAttrEnabledProperties());
        defaultProp.setProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "true");
        defaultProp.setProperty(FSDRProperties.TDE_SAMEKEY.getName(), "true");
        return defaultProp;
    }

    private Object getTDEEnabledWithDifferentKeyProperties() {
        Properties defaultProp = new Properties((Properties) getFileSystemWithACLAndXAttrEnabledProperties());
        defaultProp.setProperty(FSDRProperties.TDE_ENCRYPTION_ENABLED.getName(), "true");
        return defaultProp;
    }
}
