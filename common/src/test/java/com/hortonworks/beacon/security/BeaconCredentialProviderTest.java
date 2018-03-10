/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
