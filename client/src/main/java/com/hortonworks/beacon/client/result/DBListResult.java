/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.client.result;

import com.hortonworks.beacon.client.resource.APIResult;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * DBListResult is output returned after listing Database.
 */

//SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck
@XmlRootElement
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class DBListResult extends APIResult {

    @XmlElement
    public long totalResults;

    @XmlElement
    public DBList[] dbList;

    public DBListResult() {
    }

    public DBListResult(Status status, String message) {
        super(status, message);
    }

    private DBList[] getDatabaseDetails() {
        return dbList;
    }

    private void setDBListCollection(DBList[] dbList, int size) {
        this.dbList = dbList;
        this.totalResults = size;
    }

    public long getTotalResults() {
        return totalResults;
    }

    @Override
    public Object[] getCollection() {
        return getDatabaseDetails();
    }

    @Override
    public void setCollection(Object[] items) {
        if (items == null) {
            setDBListCollection(new DBList[0], 0);
        } else {
            DBList[] dbList = new DBList[items.length];
            for (int index = 0; index < items.length; index++) {
                dbList[index] = (DBList) items[index];
            }
            setDBListCollection(dbList, dbList.length);
        }
    }


    /**
     * DB List.
     */
    @XmlRootElement(name = "dblist")
    public static class DBList {
        @XmlElement
        public String database;

        @XmlElement
        public boolean isEncrypted;

        @XmlElement
        public String encryptionKeyName;

        @XmlElement
        public List<String> table;

        @Override
        public String toString() {
            return "DBList{"
                    + "database='" + database + '\''
                    + ", table=" + table
                    + ", isEncrypted=" + isEncrypted
                    + ", encryptionKeyName=" + encryptionKeyName
                    + '}';
        }
    }
}
//RESUME CHECKSTYLE CHECK VisibilityModifierCheck
