/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.plugin.ranger;

import java.util.Date;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/** RangerBaseModelObject class to contain common attributes of Ranger Base object.
*
*/
@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RangerBaseModelObject implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private Long    id;
    private String  guid;
    private Boolean isEnabled;
    private String  createdBy;
    private String  updatedBy;
    private Date    createTime;
    private Date    updateTime;
    private Long    version;

    public RangerBaseModelObject() {
        setIsEnabled(null);
    }

    public void updateFrom(RangerBaseModelObject other) {
        setIsEnabled(other.getIsEnabled());
    }

    /**
     * @return the id
     */
    public Long getId() {
        return id;
    }
    /**
     * @param id the id to set
     */
    public void setId(Long id) {
        this.id = id;
    }
    /**
     * @return the guid
     */
    public String getGuid() {
        return guid;
    }
    /**
     * @param guid the guid to set
     */
    public void setGuid(String guid) {
        this.guid = guid;
    }
    /**
     * @return the isEnabled
     */
    public Boolean getIsEnabled() {
        return isEnabled;
    }
    /**
     * @param isEnabled the isEnabled to set
     */
    public void setIsEnabled(Boolean isEnabled) {
        this.isEnabled = isEnabled == null ? Boolean.TRUE : isEnabled;
    }
    /**
     * @return the createdBy
     */
    public String getCreatedBy() {
        return createdBy;
    }
    /**
     * @param createdBy the createdBy to set
     */
    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }
    /**
     * @return the updatedBy
     */
    public String getUpdatedBy() {
        return updatedBy;
    }
    /**
     * @param updatedBy the updatedBy to set
     */
    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }
    /**
     * @return the createTime
     */
    public Date getCreateTime() {
        return new Date(createTime.getTime());
    }
    /**
     * @param createTime the createTime to set
     */
    public void setCreateTime(Date createTime) {
        this.createTime = new Date(createTime.getTime());
    }
    /**
     * @return the updateTime
     */
    public Date getUpdateTime() {
        return new Date(updateTime.getTime());
    }
    /**
     * @param updateTime the updateTime to set
     */
    public void setUpdateTime(Date updateTime) {
        this.updateTime = new Date(updateTime.getTime());
    }
    /**
     * @return the version
     */
    public Long getVersion() {
        return version;
    }
    /**
     * @param version the version to set
     */
    public void setVersion(Long version) {
        this.version = version;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        toString(sb);
        return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        sb.append("id={").append(id).append("} ");
        sb.append("guid={").append(guid).append("} ");
        sb.append("isEnabled={").append(isEnabled).append("} ");
        sb.append("createdBy={").append(createdBy).append("} ");
        sb.append("updatedBy={").append(updatedBy).append("} ");
        sb.append("createTime={").append(createTime).append("} ");
        sb.append("updateTime={").append(updateTime).append("} ");
        sb.append("version={").append(version).append("} ");

        return sb;
    }
}
