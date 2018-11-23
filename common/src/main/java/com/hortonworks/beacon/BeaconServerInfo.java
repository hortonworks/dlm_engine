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
package com.hortonworks.beacon;

import com.amazonaws.util.EC2MetadataUtils;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.hortonworks.beacon.constants.BeaconConstants.HDP_VERSION;

/**
 * BeaconServerInfo class maintains beacon server status, hdp version, cloud hosted etc.
 */
public final class BeaconServerInfo {

    private static final BeaconServerInfo INSTANCE = new BeaconServerInfo();
    private static final Logger LOG = LoggerFactory.getLogger(BeaconServerInfo.class);
    private boolean cloudHosted = false;
    private String hdpVersion;

    static {
        try {
            //https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html
            INSTANCE.cloudHosted = EC2MetadataUtils.getAmiId() != null;
        } catch (Throwable t) {
            LOG.warn("Failed to get cloudHosted status", t);
        }
    }

    private BeaconServerInfo() {
    }

    public static BeaconServerInfo getInstance() {
        return INSTANCE;
    }

    public boolean isCloudHosted() {
        return cloudHosted;
    }


    public String getHdpVersion() {
        if (hdpVersion != null) {
            return hdpVersion;
        }
        return System.getenv(HDP_VERSION);
    }

    public boolean isHDP3() {
        return getHdpVersion().startsWith("3");
    }

    public boolean isCloudReplicationEnabled() {
        return !isHDP3();
    }

    @VisibleForTesting
    public void setHdpVersion(String hdpVersion) {
        BeaconServerInfo.getInstance().hdpVersion = hdpVersion;
    }
}
