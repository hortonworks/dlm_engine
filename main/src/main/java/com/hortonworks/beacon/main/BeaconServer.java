/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.main;

import com.amazonaws.util.EC2MetadataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server class that maintains beacon server status for now.
 */
public class BeaconServer {

    private static final BeaconServer INSTANCE = new BeaconServer();
    private static final Logger LOG = LoggerFactory.getLogger(BeaconServer.class);
    private boolean cloudHosted = false;

    static {
        try {
            //https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html
            INSTANCE.cloudHosted = EC2MetadataUtils.getAmiId() != null;
        } catch (Throwable t) {
            LOG.warn("Failed to get cloudHosted status", t);
        }
    }

    public static final BeaconServer getInstance() {
        return INSTANCE;
    }

    public boolean isCloudHosted() {
        return cloudHosted;
    }
}
