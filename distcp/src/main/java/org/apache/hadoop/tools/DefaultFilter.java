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

import com.hortonworks.beacon.constants.BeaconConstants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.regex.Pattern;

/**
 * DefaultFilter extends the CopyFilter class, to filter out
 * temporary file paths.
 */
public class DefaultFilter extends CopyFilter {
    private static final Log LOG = LogFactory.getLog(DefaultFilter.class);
    private Pattern excludeFilePattern;

    protected DefaultFilter(Configuration conf) {
        String excludeFileRegex = conf.get(BeaconConstants.DISTCP_EXCLUDE_FILE_REGEX);
        excludeFilePattern = Pattern.compile(excludeFileRegex);
    }

    @Override
    public void initialize() {
        super.initialize();
    }

    @Override
    public boolean shouldCopy(Path path) {
        String filePath = path.toString();
        if (excludeFilePattern.matcher(filePath).find()) {
            LOG.debug("Ignoring temporary file from being copied : " + path.toString());
            return false;
        }
        return true;
    }

}
