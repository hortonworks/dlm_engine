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
import com.hortonworks.beacon.entity.BeaconCloudCred;
import com.hortonworks.beacon.entity.EncryptionAlgorithmType;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

import static com.hortonworks.beacon.util.FSUtils.merge;

/**
 * Dataset that represents file on Hadoop Compatible Filesystem (HCFS) like S3, WASB etc.
 */
public class HCFSDataset extends FSDataSet {
    public HCFSDataset(String path, ReplicationPolicy policy) throws BeaconException {
        super(path, policy);
    }

    protected Configuration getHadoopConf(String path, ReplicationPolicy policy) throws BeaconException {
        BeaconCloudCred cloudCred = new BeaconCloudCred(policy.getCloudCred());

        Configuration conf = cloudCred.getHadoopConf(false);
        Configuration encryptionConf = EncryptionAlgorithmType.getHadoopConf(policy, path);
        merge(conf, encryptionConf);
        return conf;
    }

    @Override
    public boolean isSnapshottable() throws IOException {
        return false;
    }

    @Override
    public void deleteAllSnapshots(String snapshotNamePrefix) throws IOException {
        throw new IllegalStateException("deleteAllSnapshots doesn't apply for HCFS");
    }

    @Override
    public void allowSnapshot() throws IOException {
        throw new IllegalStateException("allowSnapshot doesn't apply for HCFS");
    }

    @Override
    public void disallowSnapshot() throws IOException {
        throw new IllegalStateException("disallowSnapshot doesn't apply for HCFS");
    }
}
