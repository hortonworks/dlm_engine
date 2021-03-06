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

package com.hortonworks.beacon.entity.entityNeo;

import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Abstract class for different types of datasets supported in DLM replication.
 *
 */
public abstract class DataSet {

    public abstract boolean exists() throws IOException, BeaconException;

    public abstract void create() throws IOException;

    public abstract boolean isEmpty() throws IOException, BeaconException;

    public abstract void isWriteAllowed() throws ValidationException;

    public abstract Configuration getHadoopConf();

    public abstract boolean isSnapshottable() throws IOException;

    public abstract void deleteAllSnapshots(String snapshotNamePrefix) throws IOException;

    public abstract void allowSnapshot() throws IOException;

    public abstract void disallowSnapshot() throws IOException;

    public abstract boolean isEncrypted() throws BeaconException;

    public abstract void close();

    public static DataSet create(String dataSet, String clusterName, ReplicationPolicy policy) throws BeaconException {
        String policyType = policy.getType().toLowerCase();
        switch (policyType) {
            case "hive" :
                return new HiveDataSet(dataSet, clusterName, policy);
            case "fs" :
                return FSDataSet.create(dataSet, clusterName, policy);
            default:
                throw new IllegalStateException("Unhandeled policyType " + policyType);
        }
    }
}
