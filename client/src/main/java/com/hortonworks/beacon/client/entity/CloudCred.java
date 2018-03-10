/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
        AWS("s3a");

        private final String scheme;

        Provider(String scheme) {
            this.scheme = scheme;
        }

        public String getScheme() {
            return scheme;
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
            return name().toLowerCase();
        }

        public List<Config> getRequiredConfigs() {
            return requiredConfigs;
        }
    }
}
