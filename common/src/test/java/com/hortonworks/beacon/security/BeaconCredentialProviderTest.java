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

package com.hortonworks.beacon.security;

import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;

/**
 * Test for BeaconCredentialProvider test.
 */
public class BeaconCredentialProviderTest {
    private static final String JKS_FILE_NAME = "credentials.jks";

    private static final File CRED_DIR = new File(".");
    private BeaconCredentialProvider credProvider;

    @BeforeClass
    public void setup() throws BeaconException {
        String providerPath = "jceks://file/" + CRED_DIR.getAbsolutePath() + "/" + JKS_FILE_NAME;
        credProvider = new BeaconCredentialProvider(providerPath);
    }

    @AfterClass
    public void tearDown() throws Exception {
        // delete temporary jks files
        File file = new File(CRED_DIR, JKS_FILE_NAME);
        file.delete();
        file = new File(CRED_DIR, "." + JKS_FILE_NAME + ".crc");
        file.delete();
    }

    @Test
    public void testResolveAlias() throws Exception {
        String alias1 = getRandomString();
        String alias2 = getRandomString();
        String passwd1 = getRandomString();
        String passwd2 = getRandomString();
        credProvider.createCredentialEntry(alias1, passwd1);
        credProvider.createCredentialEntry(alias2, passwd2);
        credProvider.flush();
        Assert.assertEquals(passwd1, credProvider.resolveAlias(alias1));
        Assert.assertEquals(passwd2, credProvider.resolveAlias(alias2));
    }

    @Test(expectedExceptions = BeaconException.class,
            expectedExceptionsMessageRegExp = "Can't find the alias .*")
    public void testDeleteAlias() throws Exception {
        String alias = getRandomString();
        String passwd = getRandomString();
        credProvider.createCredentialEntry(alias, passwd);
        credProvider.flush();
        Assert.assertEquals(passwd, credProvider.resolveAlias(alias));

        credProvider.deleteCredentialEntry(alias);
        credProvider.flush();

        credProvider.resolveAlias(alias);
    }

    @Test
    public void testUpdateAlias() throws Exception {
        String alias = getRandomString();
        String passwd = getRandomString();
        credProvider.createCredentialEntry(alias, passwd);
        credProvider.flush();
        Assert.assertEquals(passwd, credProvider.resolveAlias(alias));

        //Update the credential
        passwd = getRandomString();
        credProvider.updateCredentialEntry(alias, passwd);
        credProvider.flush();
        Assert.assertEquals(passwd, credProvider.resolveAlias(alias));
    }

    private String getRandomString() {
        return RandomStringUtils.randomAlphanumeric(10);
    }
}
