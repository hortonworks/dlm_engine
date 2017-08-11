/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.replication;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * ReplicationJobDetails class with Replication Policy Details.
 */
public class ReplicationJobDetails implements Serializable {

    private static final long serialVersionUID = 9999L;

    private String identifier;
    private String name;
    private String type;
    private Properties properties;

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public ReplicationJobDetails(String identifier, String name, String type, Properties properties) {
        this.identifier = identifier;
        this.name = name;
        this.type = type;
        this.properties = properties;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(type);
        out.writeUTF(identifier);
        out.writeObject(properties);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        name = in.readUTF();
        type = in.readUTF();
        identifier = in.readUTF();
        properties = (Properties) in.readObject();
    }

    @Override
    public String toString() {
        return "ReplicationJobDetails{"
                + "identifier='" + identifier + '\''
                + "name='" + name + '\''
                + ", type='" + type + '\''
                + ", properties=" + properties
                + '}';
    }
}
