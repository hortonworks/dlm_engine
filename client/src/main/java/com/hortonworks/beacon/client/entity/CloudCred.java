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
import java.util.HashMap;
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
    private Map<Config, String> configs;

    @XmlElement
    private String creationTime;

    @XmlElement
    private String lastModifiedTime;

    public CloudCred() {
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
        S3_ACCESS_KEY("s3.access.key", "fs.s3a.access.key", Provider.S3, true),
        S3_SECRET_KEY("s3.secret.key", "fs.s3a.secret.key", Provider.S3, true),
        S3_ENCRYPTION_KEY("s3.encryption.key", "", Provider.S3);

        private static final Map<String, Config> configMap = new HashMap<>();
        static {
            for(Config config : Config.values()) {
                configMap.put(config.getName(), config);
            }
        }

        private final String name;
        private final String configName;
        private final Provider provider;
        private final boolean required;

        Config(String name, String s3aConfig, Provider provider) {
            this.name = name;
            this.configName = s3aConfig;
            this.provider = provider;
            this.required = false;
        }

        Config(String name, String s3aConfig, Provider provider, boolean required) {
            this.name = name;
            this.configName = s3aConfig;
            this.provider = provider;
            this.required = required;
        }

        @JsonCreator
        public static Config forValue(String valueOf) {
            if(configMap.containsKey(valueOf)) {
                return configMap.get(valueOf);
            }
            throw new IllegalArgumentException(
                    StringFormat.format("Invalid configuration parameter passed: {}", valueOf));
        }

        public String getName() {
            return name;
        }

        public boolean isRequired() {
            return required;
        }

        public Provider getProvider() {
            return provider;
        }

        public String getConfigName() {
            return configName;
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
        S3, ADLS, WASB
    }
}
