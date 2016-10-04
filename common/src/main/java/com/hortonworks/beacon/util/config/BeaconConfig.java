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


import java.util.Properties;
import com.esotericsoftware.yamlbeans.YamlReader;

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
    private Short port;
    private String principal;
    private Boolean tlsEnabled;
    private String quartzPrefix;
    private String configStoreUri;

    public BeaconConfig() {
        setHostName("localhost");
        setPort((short)25000);
        setPrincipal("");
        setTlsEnabled(false);
        setConfigStoreUri("/tmp/config-store/");
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

    public void setPort(Short port) {
        this.port = port;
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

}
