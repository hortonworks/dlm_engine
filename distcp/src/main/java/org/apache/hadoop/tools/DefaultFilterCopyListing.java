/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package org.apache.hadoop.tools;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;

/**
 * DefaultFilterCopyListing extends the SimpleCopyListing class, to filter out
 * temporary file paths.
 */
public class DefaultFilterCopyListing extends SimpleCopyListing {
    private static final Log LOG = LogFactory.getLog(DefaultFilterCopyListing.class);

    /**
     * Constructor, to initialize the configuration.
     * @param configuration The input Configuration object.
     * @param credentials Credentials object on which the FS delegation tokens are cached. If null
     * delegation token caching is skipped
     */
    public DefaultFilterCopyListing(Configuration configuration, Credentials credentials) {
        super(configuration, credentials);
    }

    @Override
    protected boolean shouldCopy(Path path) {
        String filePath = path.toString();
        if (filePath.matches(".*_COPYING[^\\/]*")
                || filePath.matches(".*\\/_temporary$")
                || filePath.matches(".*\\/_temporary\\/.*")
                || filePath.matches(".*\\/_WIP_$")
                || filePath.matches(".*\\/_WIP_\\/.*")) {
            LOG.info("Ignoring temporary file from being copied : " + filePath);
            return false;
        }
        return true;
    }

}
