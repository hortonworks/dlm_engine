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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.util.StringUtils;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;

/**
 *  SecureClientLogin class is client login implementation class to connect to kerberos server.
 */

public final class SecureClientLogin {
    private SecureClientLogin() { }

    private static final Log LOG = LogFactory.getLog(SecureClientLogin.class);
    public static final String HOSTNAME_PATTERN = "_HOST";

    public static synchronized Subject loginUserFromKeytab(String user, String path, String nameRules)
            throws IOException {
        try {
            Subject subject = new Subject();
            SecureClientLoginConfiguration loginConf = new SecureClientLoginConfiguration(true, user, path);
            LoginContext login = new LoginContext("hadoop-keytab-kerberos", subject, null, loginConf);
            KerberosName.setRules(nameRules);
            subject.getPrincipals().add(new User(user, AuthenticationMethod.KERBEROS, login));
            login.login();
            return login.getSubject();
        } catch (LoginException le) {
            throw new IOException("Login failure for " + user + " from keytab " + path, le);
        }
    }

    public static boolean isKerberosCredentialExists(String principal, String keytabPath){
        boolean isValid = false;
        if (keytabPath != null && !keytabPath.isEmpty()) {
            File keytabFile = new File(keytabPath);
            if (!keytabFile.exists()) {
                LOG.warn(keytabPath + " doesn't exist.");
            } else if (!keytabFile.canRead()) {
                LOG.warn("Unable to read " + keytabPath + " Please check the file access permissions for user");
            }else{
                isValid = true;
            }
        } else {
            LOG.warn("Can't find keyTab Path : "+keytabPath);
        }
        if (!(principal != null && !principal.isEmpty() && isValid)) {
            isValid = false;
            LOG.warn("Can't find principal : "+principal);
        }
        return isValid;
    }

    public static String getPrincipal(String principalConfig, String hostName) throws IOException {
        String[] components = getComponents(principalConfig);
        if (components == null || components.length != 3 || !components[1].equals(HOSTNAME_PATTERN)) {
            return principalConfig;
        } else {
            if (hostName == null) {
                throw new IOException(
                        "Can't replace " + HOSTNAME_PATTERN + " pattern since client beacon.service.host is null");
            }
            return replacePattern(components, hostName);
        }
    }

    private static String[] getComponents(String principalConfig) {
        if (principalConfig == null) {
            return null;
        }
        return principalConfig.split("[/@]");
    }

    private static String replacePattern(String[] components, String hostname)
            throws IOException {
        String fqdn = hostname;
        if (fqdn == null || fqdn.isEmpty() || fqdn.equals("0.0.0.0")) {
            fqdn = java.net.InetAddress.getLocalHost().getCanonicalHostName();
        }
        return components[0] + "/" + StringUtils.toLowerCase(fqdn) + "@" + components[2];
    }
}
