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

package com.hortonworks.beacon.client.entity;

import com.hortonworks.beacon.util.StringFormat;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonValue;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Definition of the cloud cred entity.
 */

@XmlRootElement(name = "cloudCred")
@XmlAccessorType(XmlAccessType.FIELD)
public class CloudCred extends Entity {

    @XmlElement
    private String id;

    @XmlElement
    private String name;

    @XmlElement
    private Provider provider;

    @XmlElement
    private AuthType authType;

    @XmlElement
    protected Map<Config, String> configs;

    @XmlElement
    private String creationTime;

    @XmlElement
    private String lastModifiedTime;

    public CloudCred() {
    }

    public CloudCred(CloudCred cloudCred) {
        this.id = cloudCred.id;
        this.name = cloudCred.name;
        this.provider = cloudCred.provider;
        this.authType = cloudCred.authType;
        this.configs = cloudCred.configs;
        this.creationTime = cloudCred.creationTime;
        this.lastModifiedTime = cloudCred.lastModifiedTime;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    @Override
    public String getTags() {
        throw new RuntimeException("Operation is not supported on cloudCred entity");
    }

    public void setName(String name) {
        this.name = name;
    }

    public Provider getProvider() {
        return provider;
    }

    public void setProvider(Provider provider) {
        this.provider = provider;
    }

    public AuthType getAuthType() {
        return authType;
    }

    public void setAuthType(AuthType authType) {
        this.authType = authType;
    }

    public Map<Config, String> getConfigs() {
        return configs;
    }

    public void setConfigs(Map<Config, String> configs) {
        this.configs = configs;
    }

    public String getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(String creationTime) {
        this.creationTime = creationTime;
    }

    public String getLastModifiedTime() {
        return lastModifiedTime;
    }

    public void setLastModifiedTime(String lastModifiedTime) {
        this.lastModifiedTime = lastModifiedTime;
    }



    /**
     * S3 cloud cred configuration keys.
     */
    public enum Config {
        VERSION("version", null, null, false),

        //AWS related configs
        AWS_ACCESS_KEY("aws.access.key", "fs.s3a.access.key", Provider.AWS, true),
        AWS_SECRET_KEY("aws.secret.key", "fs.s3a.secret.key", Provider.AWS, true);

        private static final Map<String, Config> CONFIG_MAP = new HashMap<>();

        static {
            for(Config config : Config.values()) {
                CONFIG_MAP.put(config.getName(), config);
            }
        }

        private final String name;
        private final String hadoopConfigName;
        private final Provider provider;
        private final boolean password;

        Config(String name, String hadoopConfigName, Provider provider, boolean password) {
            this.name = name;
            this.hadoopConfigName = hadoopConfigName;
            this.provider = provider;
            this.password = password;
        }

        @JsonCreator
        public static Config forValue(String valueOf) {
            if (CONFIG_MAP.containsKey(valueOf)) {
                return CONFIG_MAP.get(valueOf);
            }
            throw new IllegalArgumentException(
                    StringFormat.format("Invalid configuration parameter passed: {}", valueOf));
        }

        public String getName() {
            return name;
        }

        public Provider getProvider() {
            return provider;
        }

        public String getHadoopConfigName() {
            return hadoopConfigName;
        }

        public boolean isPassword() {
            return password;
        }

        @JsonValue
        @Override
        public String toString() {
            return this.getName();
        }
    }

    /**
     * Cloud cred provider types.
     */
    public enum Provider {
        AWS("s3", "s3a");

        private final String scheme;

        private final String hcfsScheme;

        Provider(String scheme, String hcfsScheme) {
            this.scheme = scheme;
            this.hcfsScheme = hcfsScheme;
        }

        public String getScheme() {
            return scheme;
        }

        public String getHcfsScheme() {
            return hcfsScheme;
        }
    }

    /**
     * Cloud cred auth types.
     */
    public enum AuthType {
        AWS_ACCESSKEY(Config.AWS_ACCESS_KEY, Config.AWS_SECRET_KEY), AWS_SESSIONKEY, AWS_INSTANCEPROFILE;

        private final List<Config> requiredConfigs;

        AuthType(Config... requiredConfigs) {
            this.requiredConfigs = Arrays.asList(requiredConfigs);
        }

        public String getAuthType() {
            return name().toLowerCase(Locale.ENGLISH);
        }

        public List<Config> getRequiredConfigs() {
            return requiredConfigs;
        }
    }
}
