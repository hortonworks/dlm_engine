/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.replication.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.File;

/**
 * Create a local MiniDFS cluster for testing snapshots et al.
 */
public final class MiniHDFSClusterUtil {

    private MiniHDFSClusterUtil() {
    }

    public static final int SNAPSHOT_REPL_TEST_PORT1 = 54136;
    public static final int SNAPSHOT_REPL_TEST_PORT2 = 54137;


    public static MiniDFSCluster initMiniDfs(int port, File baseDir) throws Exception {
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        builder.nameNodePort(port);
        return builder.build();
    }

    public static void cleanupDfs(MiniDFSCluster miniDFSCluster, File baseDir) throws Exception {
        miniDFSCluster.shutdown();
        FileUtil.fullyDelete(baseDir);
    }
}
