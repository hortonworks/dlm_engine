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

package com.hortonworks.dlmengine.fs;

import com.hortonworks.beacon.entity.BeaconCloudCred;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Dataset that represents file on Hadoop Compatible Filesystem (HCFS) like S3, WASB etc.
 */
public abstract class HCFSDataset extends FSDataSet {
    private static final Logger LOG = LoggerFactory.getLogger(HCFSDataset.class);
    protected BeaconCloudCred cloudCred;

    public HCFSDataset(String path, BeaconCloudCred cloudCred) throws BeaconException {
        super(path);
        this.cloudCred = cloudCred;
    }

    @Override
    public Configuration getHadoopConf() throws BeaconException {
        //TODO set fs caching = false
        return cloudCred.getHadoopConf(false);
    }

    @Override
    public void validateWriteAllowed() throws ValidationException {
        String tmpFileName = ".Beacon_" + System.currentTimeMillis() + ".tmp";
        try {
            Path tmpFilePath = new Path(path, tmpFileName);
            FSDataOutputStream os = fileSystem.create(tmpFilePath);
            os.close();
            boolean tmpDeleted = fileSystem.delete(tmpFilePath, false);
            if (tmpDeleted) {
                LOG.debug("Deleted the temp file {} created during policy validation process", tmpFileName);
            } else {
                LOG.warn("Could not delete the temp file {} created during policy validation process", tmpFileName);
            }
        } catch (IOException ioEx) {
            throw new ValidationException(ioEx, ioEx.getCause().getMessage());
        }
        LOG.info("Validation for write access to cloud path {} succeeded", path);
    }


    @Override
    public boolean isSnapshottable() {
        return false;
    }

    @Override
    public void deleteAllSnapshots(String snapshotNamePrefix) {
        //do nothing
    }

    @Override
    public void allowSnapshot() {
        //do nothing
    }

    @Override
    public void disallowSnapshot() {
        //do nothing
    }

    @Override
    public void close() {
        close(fileSystem);
    }
}
