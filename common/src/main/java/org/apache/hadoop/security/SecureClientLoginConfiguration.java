/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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

