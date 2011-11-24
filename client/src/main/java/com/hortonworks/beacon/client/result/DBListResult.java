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
