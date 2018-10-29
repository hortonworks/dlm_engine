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

package com.hortonworks.dlmengine.fs.wasb;

import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.ReplicationPolicy;
import com.hortonworks.beacon.entity.BeaconCloudCred;
import com.hortonworks.beacon.entity.EncryptionAlgorithmType;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.StringFormat;
import com.hortonworks.dlmengine.fs.HCFSDataset;
import org.apache.hadoop.fs.Path;

/**
 * Azure wasb based implementation of data set.
 */
public class WASBFSDataSet extends HCFSDataset {
    //TODO remove
    public static final String WASB_ENDPOINT = ".blob.core.windows.net";
    private ReplicationPolicy policy;

    public WASBFSDataSet(String path, BeaconCloudCred cloudCred, ReplicationPolicy policy) throws BeaconException {
        super(path, cloudCred, policy);
        this.policy = policy;
    }

    @Override
    public String resolvePath(String path, ReplicationPolicy policyInp) {
        cloudCred = new BeaconCloudCred(policyInp.getCloudCred());
        String wasbAccount = cloudCred.getConfigs().get(CloudCred.Config.WASB_ACCOUNT_NAME);
        String authority = new Path(path).toUri().getAuthority();
        String myPath = new Path(path).toUri().getPath();
        if (path.contains(wasbAccount) && path.contains(WASB_ENDPOINT)) {
            //path is a hive warehouse directory
            return path;
        } else {
            return StringFormat.format("{}://{}@{}.blob.core.windows.net/{}",
                    CloudCred.Provider.WASB.getHcfsScheme(), authority, wasbAccount, myPath);
        }
    }

    @Override
    public boolean isEncrypted(Path path) throws BeaconException {
        /**
         * Encryption is by default enabled in WASB store and can't be disabled also.
         */
        return true;
    }

    @Override
    public void validateEncryptionParameters() throws BeaconException {
        EncryptionAlgorithmType.validate(policy);
    }
}

