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

/**
 * DefaultFilter extends the CopyFilter class, to filter out
 * temporary file paths.
 */
public class DefaultFilter extends CopyFilter {
    private static final Log LOG = LogFactory.getLog(DefaultFilter.class);

    protected DefaultFilter(Configuration conf) {
    }

    @Override
    public void initialize() {
        super.initialize();
    }

    @Override
    public boolean shouldCopy(Path path) {
        String fileName = path.getName();
        String fileParentPath = path.getParent().toString();
        if(fileParentPath.matches(".*\\/_temporary")
                || fileParentPath.matches(".*\\/_temporary\\/.*")
                || fileParentPath.matches(".*\\/\\._WIP_.*")
                || fileParentPath.matches(".*\\/\\._WIP_.*\\/.*")
                || fileName.endsWith("_COPYING")
                || fileName.equals("_temporary")) {
            LOG.debug("Ignoring temporary file from being copied : " + path.toString());
            return false;
        }
        return true;
    }

}
