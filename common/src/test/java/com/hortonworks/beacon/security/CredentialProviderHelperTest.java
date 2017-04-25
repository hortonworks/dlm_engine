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
