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

import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;

/**
 * Test for CredentialProviderHelper test.
 */
public class CredentialProviderHelperTest {
    private static final String ALIAS_1 = "alias-key-1";
    private static final String ALIAS_2 = "alias-key-2";
    private static final String PASSWORD_1 = "password1";
    private static final String PASSWORD_2 = "password2";
    private static final String JKS_FILE_NAME = "credentials.jks";

    private static final File CRED_DIR = new File(".");

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
        // clean credential provider store
        File file = new File(CRED_DIR, JKS_FILE_NAME);
        file.delete();

        // add alias to hadoop credential provider
        Configuration conf = new Configuration();
        String providerPath = "jceks://file/" + CRED_DIR.getAbsolutePath() + "/" + JKS_FILE_NAME;
        conf.set(CredentialProviderHelper.CREDENTIAL_PROVIDER_PATH, providerPath);
        CredentialProviderHelper.createCredentialEntry(conf, ALIAS_1, PASSWORD_1);
        CredentialProviderHelper.createCredentialEntry(conf, ALIAS_2, PASSWORD_2);

        Assert.assertEquals(PASSWORD_1, CredentialProviderHelper.resolveAlias(conf, ALIAS_1));
        Assert.assertEquals(PASSWORD_2, CredentialProviderHelper.resolveAlias(conf, ALIAS_2));
    }
}
