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

import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

/**
 * KrbPasswordSaverLoginModule class stores kerberized user and principal.
 */

public class KrbPasswordSaverLoginModule implements LoginModule {

    public static final String USERNAME_PARAM = "javax.security.auth.login.name";
    public static final String PASSWORD_PARAM = "javax.security.auth.login.password";

    @SuppressWarnings("rawtypes")
    private Map sharedState = null;

    public KrbPasswordSaverLoginModule() {
    }

    @Override
    public boolean abort() throws LoginException {
        return true;
    }

    @Override
    public boolean commit() throws LoginException {
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void initialize(Subject subject, CallbackHandler callbackhandler, Map<String, ?> sharedMap,
            Map<String, ?> options) {

        this.sharedState = sharedMap;

        String userName = (options != null) ? (String)options.get(USERNAME_PARAM) : null;
        if (userName != null) {
            this.sharedState.put(USERNAME_PARAM, userName);
        }
        String password = (options != null) ? (String)options.get(PASSWORD_PARAM) : null;

        if (password != null) {
            this.sharedState.put(PASSWORD_PARAM, password.toCharArray());
        }
    }

    @Override
    public boolean login() throws LoginException {
        return true;
    }

    @Override
    public boolean logout() throws LoginException {
        return true;
    }

}
