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

package org.apache.hadoop.security;

import com.hortonworks.beacon.security.KrbPasswordSaverLoginModule;
import org.apache.hadoop.security.authentication.util.KerberosUtil;

import javax.security.auth.login.AppConfigurationEntry;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for SecureClientLogin.
 */
public class SecureClientLoginConfiguration extends javax.security.auth.login.Configuration {

    private Map<String, String> kerberosOptions = new HashMap<String, String>();
    private boolean usePassword = false;

    SecureClientLoginConfiguration(boolean useKeyTab, String principal, String credential) {
        kerberosOptions.put("principal", principal);
        kerberosOptions.put("debug", "false");
        if (useKeyTab) {
            kerberosOptions.put("useKeyTab", "true");
            kerberosOptions.put("keyTab", credential);
            kerberosOptions.put("doNotPrompt", "true");
        } else {
            usePassword = true;
            kerberosOptions.put("useKeyTab", "false");
            kerberosOptions.put(KrbPasswordSaverLoginModule.USERNAME_PARAM, principal);
            kerberosOptions.put(KrbPasswordSaverLoginModule.PASSWORD_PARAM, credential);
            kerberosOptions.put("doNotPrompt", "false");
            kerberosOptions.put("useFirstPass", "true");
            kerberosOptions.put("tryFirstPass", "false");
        }
        kerberosOptions.put("storeKey", "true");
        kerberosOptions.put("refreshKrb5Config", "true");
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
        AppConfigurationEntry keytabKerberosLogin = new AppConfigurationEntry(KerberosUtil.getKrb5LoginModuleName(),
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, kerberosOptions);
        if (usePassword) {
            AppConfigurationEntry kerberosPwdSaver = new AppConfigurationEntry(
                    KrbPasswordSaverLoginModule.class.getName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                    kerberosOptions);
            return new AppConfigurationEntry[] { kerberosPwdSaver, keytabKerberosLogin };
        } else {
            return new AppConfigurationEntry[] { keytabKerberosLogin };
        }
    }
}

