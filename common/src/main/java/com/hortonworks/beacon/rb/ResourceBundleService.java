/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */
package com.hortonworks.beacon.rb;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.EnumUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.service.BeaconService;
import com.hortonworks.beacon.service.Services;

/**
 * Service class to handle ResourceBundle.
 */
public class ResourceBundleService implements BeaconService {

    private static final ResourceBundleService INSTANCE = new ResourceBundleService();
    private static final Logger LOG = LoggerFactory.getLogger(ResourceBundleService.class);
    public static final String SERVICE_NAME = ResourceBundleService.class.getName();
    private ResourceBundle bundle;

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    public static ResourceBundleService get() {
        return INSTANCE;
    }

    public static ResourceBundleService getService() {
        return (ResourceBundleService) Services.get().getService(ResourceBundleService.SERVICE_NAME);
    }

    @Override
    public void init() throws BeaconException{
        Locale locale = null;
        try {
            if (BeaconConfig.getInstance().getEngine().getLocale() != null) {
                locale = new Locale(BeaconConfig.getInstance().getEngine().getLocale());
            }
            if (locale == null || !isValid(locale)) {
                locale = Locale.getDefault();
            }
            final File confFile = new File(BeaconConfig.getBeaconConfDir(BeaconConfig.getBeaconHome()) + "/messages/");
            if (confFile.exists()) {
                //Checks for the bundle in the CONF directory
                final Locale localeForBundle = (Locale) locale.clone();
                bundle = AccessController.doPrivileged(new PrivilegedAction<ResourceBundle>() {
                    public ResourceBundle run() {
                        try {
                            return ResourceBundle.getBundle("message", localeForBundle,
                                    new URLClassLoader(new URL[] { confFile.toURI().toURL() }));
                        } catch (MalformedURLException e) {
                            LOG.error("MalformedURLException occurred while reading from Resource Bundle {} ",
                                    e.getMessage());
                        } catch (MissingResourceException e) {
                            LOG.error("MissingResourceException occurred while reading from Resource Bundle {} ",
                                    e.getMessage());
                        }
                        return ResourceBundle.getBundle("messages/message", localeForBundle);
                    }
                });
            } else {
                //Checks for the bundle in the classpath
                bundle = ResourceBundle.getBundle("messages/message", locale);
            }
        } catch (MissingResourceException e) {
            LOG.error("Exception occurred while reading from Resource Bundle {} ", e.getMessage());
            throw e;
        }
    }

    @Override
    public void destroy() throws BeaconException {
        ResourceBundle.clearCache();
        bundle = null;
    }

    private boolean isValid(Locale locale) {
        try {
            return StringUtils.isNotBlank(locale.getISO3Language());
        } catch (MissingResourceException e) {
            return false;
        }
    }

    public String getString(String key, Object... arrayOfParameters) {
        String value = getString(key, bundle);
        if (StringUtils.isBlank(value)) {
            if (EnumUtils.isValidEnum(MessageCode.class, key)) {
                value = MessageCode.valueOf(key).getMsg();
            } else {
                value = key;
            }
        }
        if (ArrayUtils.isNotEmpty(arrayOfParameters)) {
            return MessageFormat.format(value, arrayOfParameters);
        }
        return value;
    }

    private String getString(String key, ResourceBundle resourceBundle) {
        String value = null;
        try {
            value = resourceBundle.getString(key);
            if (StringUtils.isNotBlank(value)) {
                value = new String(value.getBytes(), Charset.forName("UTF-8"));
            }
        } catch (MissingResourceException exception) {
            LOG.error("MissingResourceException occurred while reading from Resource Bundle {}",
                    exception.getMessage());
        }
        return value;
    }
}
