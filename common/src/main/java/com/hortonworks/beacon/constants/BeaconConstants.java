/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.constants;

/**
 * Beacon constants.
 */
public final class BeaconConstants {

    private BeaconConstants() {
        // Disable construction.
    }
    /**
     * Constant for the configuration property that indicates the Name node principal.
     */
    public static final String FS_DEFAULT_NAME_KEY = "fs.defaultFS";
    public static final String NN_PRINCIPAL = "dfs.namenode.kerberos.principal";
    public static final String JCEKS_HDFS_FILE_REGEX = "jceks://(hdfs|file)/";
    public static final String CREDENTIAL_PROVIDER_PATH = "hadoop.security.credential.provider.path";
    public static final String JCEKS_EXT = ".jceks";

    public static final int MAX_YEAR = 9999;
    public static final long DAY_IN_MS = 24 * 60 * 60 * 1000;
    public static final int MAX_DAY = 31;
    public static final long SERVER_START_TIME = System.currentTimeMillis();
    public static final String COLON_SEPARATOR = ":";
    public static final String SEMICOLON_SEPARATOR = ";";
    public static final String COMMA_SEPARATOR = ",";
    public static final String DOT_SEPARATOR = ".";
    public static final String EQUAL_SEPARATOR = "=";
    public static final String NEW_LINE = "\n";
    public static final String CLUSTER_NAME_SEPARATOR_REGEX = "\\$";
    public static final String VALIDATION_QUERY = "select count(*) from beacon_sys";
    public static final String BEACON_VERSION_CONST = "beacon.version";
    public static final String DEFAULT_BEACON_VERSION = "1.0";
    public static final String DFS_HA_NAMENODES = "dfs.ha.namenodes";
    public static final String DFS_NN_RPC_PREFIX = "dfs.namenode.rpc-address";
    public static final String DFS_NAMESERVICES = "dfs.nameservices";
    public static final String DFS_INTERNAL_NAMESERVICES = "dfs.internal.nameservices";
    public static final String DFS_CLIENT_FAILOVER_PROXY_PROVIDER = "dfs.client.failover.proxy.provider";
    public static final String DFS_CLIENT_DEFAULT_FAILOVER_STRATEGY = "org.apache.hadoop.hdfs.server."
            + "namenode.ha.ConfiguredFailoverProxyProvider";
    public static final String HA_CONFIG_KEYS = "ha.config.keys";
    public static final String MAPRED_QUEUE_NAME = "mapreduce.job.queuename";
    public static final String SET = "SET ";
    public static final String HIVE_EXEC_PARALLEL = "hive.exec.parallel";
    public static final String HIVE_TDE_SAMEKEY = "hive.repl.add.raw.reserved.namespace";
    public static final String HIVE_DISTCP_DOAS = "hive.distcp.privileged.doAs";
    public static final String HIVE_PRINCIPAL = "hive.server2.authentication.kerberos.principal";
    public static final String MAPREDUCE_JOB_HDFS_SERVERS = "mapreduce.job.hdfs-servers";
    public static final String MAPREDUCE_JOB_SEND_TOKEN_CONF = "mapreduce.job.send-token-conf";
    public static final String DISTCP_OPTIONS = "distcp.options.";
    public static final String DISTCP_EXCLUDE_FILE_REGEX = "distcp.exclude-file-regex";
    public static final String MASK = "********";
}
