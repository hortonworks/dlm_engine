/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.store.bean;

import com.hortonworks.beacon.client.entity.CloudCred.AuthType;
import com.hortonworks.beacon.client.entity.CloudCred.Provider;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.openjpa.persistence.jdbc.Strategy;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Enumerated;
import javax.persistence.EnumType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import java.util.Date;
import java.util.Map;

/**
 * Cloud cred entity bean.
 */
@SuppressFBWarnings(value = {"NP_BOOLEAN_RETURN_NULL", "UWF_UNWRITTEN_FIELD"})
@Entity
@Table(name = "BEACON_CLOUD_CRED")
@NamedQueries({
        @NamedQuery(name = "GET_CLOUD_CRED", query = "select OBJECT(b) from CloudCredBean b where b.id = :id"),
        @NamedQuery(name = "UPDATE_CLOUD_CRED", query = "update CloudCredBean b "
                + "set b.configuration = :configuration, b.lastModifiedTime = :lastModifiedTime, "
                + "b.authType = :authType where b.id = :id"),
        @NamedQuery(name = "DELETE_CLOUD_CRED", query = "delete from CloudCredBean b where b.id = :id")
    })
public class CloudCredBean {

    @Id
    @Column(name = "id")
    private String id;

    @Column(name = "name")
    private String name;

    @Column(name = "provider")
    @Enumerated(EnumType.STRING)
    private Provider provider;

    @Column(name = "auth_type")
    @Enumerated(EnumType.STRING)
    private AuthType authType;

    @Column(name = "creation_time")
    private java.sql.Timestamp creationTime;

    @Column(name = "last_modified_time")
    private java.sql.Timestamp lastModifiedTime;

    @Basic
    @Column(name = "configuration")
    @Lob
    @Strategy("com.hortonworks.beacon.store.executors.StringMapValueHandler")
    private Map<String, String> configuration;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
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
        return this.authType;
    }

    public void setAuthType(AuthType authType) {
        this.authType = authType;
    }

    public Date getCreationTime() {
        if (creationTime != null) {
            return new Date(creationTime.getTime());
        }
        return null;
    }

    public void setCreationTime(Date creationTime) {
        if (creationTime != null) {
            this.creationTime = new java.sql.Timestamp(creationTime.getTime());
        }
    }

    public Date getLastModifiedTime() {
        if (lastModifiedTime != null) {
            return new Date(lastModifiedTime.getTime());
        }
        return null;
    }

    public void setLastModifiedTime(Date lastModifiedTime) {
        this.lastModifiedTime = new java.sql.Timestamp(lastModifiedTime.getTime());
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Map<String, String> configuration) {
        this.configuration = configuration;
    }
}
