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

package com.hortonworks.beacon.replication.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Utility class for reading and writing hadoop sequence files.
 */
public class PermissionMetaFileOperator {

    public PermissionMetaFileOperator() {
    }

    private static final Logger LOG = LoggerFactory.getLogger(PermissionMetaFileOperator.class);

    /**
     * creates a sequence file reader.
     * @param path path of the hadoop sequence file.
     * @param configuration hadoop configuration object
     * @return
     * @throws IOException
     */
    public SequenceFile.Reader reader(Path path, Configuration configuration) throws IOException {
        return new SequenceFile.Reader(configuration,
                SequenceFile.Reader.file(path));
    }

    /**
     * creates a sequence file writer.
     * @param path path of the hadoop sequence file.
     * @param conf hadoop configuration object
     * @return
     * @throws IOException
     */
    public SequenceFile.Writer writer(Path path, Configuration conf) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        if (fs.exists(path)) {
            fs.delete(path, false);
        }
        return SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(path),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(CopyListingFileStatus.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
    }

    /**
     * logs the {@link CopyListingFileStatus } for a hadoop seq file.
     * @param path
     * @param configuration
     *
     */
    public String infoLogger(Path path, Configuration configuration) throws IOException {
        SequenceFile.Reader reader = null;
        StringBuilder stringBuilder = new StringBuilder();
        try {
            reader = reader(path, configuration);
            CopyListingFileStatus fileStatus = new CopyListingFileStatus();
            Text relPath = new Text();
            while (reader.next(relPath, fileStatus)) {
                stringBuilder.append("Path: " + relPath + " File Status: " + fileStatus);
            }
        } finally {
            IOUtils.closeStream(reader);
        }
        return stringBuilder.toString();
    }
}
