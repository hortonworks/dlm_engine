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

package com.hortonworks.beacon.util.config;


import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * The config file is a YAML file with the following struct
 *
 * - server : {
 *      hostname: val,
 *      port : val
 *      kerberos_principal : val
 *      tlsenabled : val
 *   }
 * - quartz : {
 *     quartz jdbc store prefix
 *     quartz configs
 *  }
 *
 */
public final class BeaconConfig {
    private String hostName;
    private Short tlsPort;
    private Short port;
    private String principal;
    private Boolean tlsEnabled;
    private String quartzPrefix;
    private String configStoreUri;
    private String appPath;
    private static final String BUILD_PROPS = "beacon-buildinfo.properties";
    private static final String DEF_VERSION = "1.0-SNAPSHOT";
    Properties buildInfo = new Properties();

    public BeaconConfig() {
        setHostName("0.0.0.0");
        setPort((short) 25000);
        setTlsPort((short) 25493);
        setPrincipal("");
        setTlsEnabled(false);
        setConfigStoreUri("/tmp/config-store/");
        Class cl = BeaconConfig.class;
        InputStream resourceAsStream = null;
        URL resource = cl.getResource("/" + BUILD_PROPS);
        if (resource != null) {
            resourceAsStream = cl.getResourceAsStream("/" + BUILD_PROPS);
        } else {
            resource = cl.getResource(BUILD_PROPS);
            if (resource != null) {
                resourceAsStream = cl.getResourceAsStream(BUILD_PROPS);
            }
        }
        if (resourceAsStream != null)  {

            try {
                buildInfo.load(resourceAsStream);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        String version = (String) buildInfo.get("build.version");
        if (version == null) {
            version = "1.0-SNAPSHOT";
        }
        setAppPath("webapp/target/beacon-webapp-" + version);

    }

    public static Properties get() {
        /* TODO : Add logic to read from config file */
        return new Properties();
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }


    public Short getPort() {
        return port;
    }

    public Short getTlsPort() {
        return tlsPort;
    }


    public void setPort(Short port) {
        this.port = port;
    }

    public void setTlsPort(Short port) {
        this.tlsPort = port;
    }


    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    public Boolean getTlsEnabled() {
        return tlsEnabled;
    }

    public void setTlsEnabled(Boolean tlsEnabled) {
        this.tlsEnabled = tlsEnabled;
    }

    public String getQuartzPrefix() {
        return quartzPrefix;
    }

    public void setQuartzPrefix(String quartzPrefix) {
        this.quartzPrefix = quartzPrefix;
    }


    public String getConfigStoreUri() {
        return configStoreUri;
    }

    public void setConfigStoreUri(String configStoreUri) {
        this.configStoreUri = configStoreUri;
    }

    public String getAppPath() {
        return appPath;
    }

    public void setAppPath(String appPath) {
        this.appPath = appPath;
    }


}
