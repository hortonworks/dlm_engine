package com.hortonworks.beacon.entity;

import java.util.Properties;

/**
 * Created by sramesh on 9/29/16.
 */
public class Cluster extends Entity {

    private String name;
    private String description;
    private String colo;
    private String nameNodeUri;
    private String executeUri;
    private String wfEngineUri;
    private String messagingUri;
    private String hs2Uri;
    private String tags;
    private Properties customProperties;
    private Acl acl;
    //    private int version;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getColo() {
        return colo;
    }

    public void setColo(String colo) {
        this.colo = colo;
    }

    public String getNameNodeUri() {
        return nameNodeUri;
    }

    public void setNameNodeUri(String nameNodeUri) {
        this.nameNodeUri = nameNodeUri;
    }

    public String getWfEngineUri() {
        return wfEngineUri;
    }

    public void setWfEngineUri(String wfEngineUri) {
        this.wfEngineUri = wfEngineUri;
    }

    public String getMessagingUri() {
        return messagingUri;
    }

    public void setMessagingUri(String messagingUri) {
        this.messagingUri = messagingUri;
    }

    public String getExecuteUri() {
        return executeUri;
    }

    public void setExecuteUri(String executeUri) {
        this.executeUri = executeUri;
    }

    public String getHs2Uri() {
        return hs2Uri;
    }

    public void setHs2Uri(String hs2Uri) {
        this.hs2Uri = hs2Uri;
    }

    @Override
    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public Properties getCustomProperties() {
        return customProperties;
    }

    public void setCustomProperties(Properties customProperties) {
        this.customProperties = customProperties;
    }

    @Override
    public Acl getAcl() {
        return acl;
    }

    public void setAcl(Acl acl) {
        this.acl = acl;
    }

    @Override
    public String toString() {
        return "Cluster{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", colo='" + colo + '\'' +
                ", nameNodeUri='" + nameNodeUri + '\'' +
                ", executeUri='" + executeUri + '\'' +
                ", wfEngineUri='" + wfEngineUri + '\'' +
                ", messagingUri='" + messagingUri + '\'' +
                ", hs2Uri='" + hs2Uri + '\'' +
                ", tags='" + tags + '\'' +
                ", customProperties=" + customProperties +
                ", acl=" + acl +
                '}';
    }
}
