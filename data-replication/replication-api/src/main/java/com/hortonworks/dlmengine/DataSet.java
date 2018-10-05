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

package com.hortonworks.dlmengine;

import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class for different types of datasets supported in DLM replication.
 *
 */
public abstract class DataSet implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DataSet.class);

    protected String name;

    public DataSet(String name) {
        this.name = name;
    }

    public abstract void create(FileStatus fileStatus) throws BeaconException;

    public abstract boolean exists() throws BeaconException;

    public abstract boolean isEmpty() throws BeaconException;

    public abstract void validateWriteAllowed() throws ValidationException;

    public abstract Configuration getHadoopConf() throws BeaconException;

    public abstract boolean isEncrypted() throws BeaconException;

    public abstract boolean isSnapshottable() throws BeaconException;

    public abstract void deleteAllSnapshots(String snapshotNamePrefix) throws BeaconException;

    public abstract void allowSnapshot() throws BeaconException;

    public abstract void disallowSnapshot() throws BeaconException;

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[" + name + "]";
    }

    public abstract boolean conflicts(String otherDataset);

    public abstract FileStatus getFileStatus() throws BeaconException;

    public boolean failIfTargetNotExists() {
        return false;
    }

    public void validateEncryptionParameters() throws BeaconException {
        //do nothing
    }

    public static void close(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                LOG.warn("Failed to close {}, exception message - {}", closeable.getClass().getName(), e.getMessage());
            }
        }
    }
}
