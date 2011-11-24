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
    public static final String HMS_PRINCIPAL = "hive.metastore.kerberos.principal";
    public static final String JCEKS_HDFS_FILE_REGEX = "jceks://(hdfs|file)/";
    public static final String CREDENTIAL_PROVIDER_PATH = "hadoop.security.credential.provider.path";
    public static final String FS_S3A_IMPL_DISABLE_CACHE = "fs.s3a.impl.disable.cache";
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
    public static final String HIVE_HTTP_PROXY_PATH="httpPath";
    public static final String HIVE_SSO_COOKIE="http.cookie.hadoop-jwt";
    public static final String HIVE_SSL_MODE="ssl=true";
    public static final String HIVE_SSL_TRUST_STORE = "sslTrustStore";
    public static final String HIVE_SSL_TRUST_STORE_PASSWORD = "trustStorePassword";
    public static final String HIVE_TRANSPORT_MODE = "transportMode";
    public static final String HIVE_TRANSPORT_MODE_HTTP = "http";
    public static final String HIVE_JDBC_PROVIDER = "hive2://";
    public static final String MAPREDUCE_JOB_HDFS_SERVERS = "mapreduce.job.hdfs-servers";
    public static final String MAPREDUCE_JOB_SEND_TOKEN_CONF = "mapreduce.job.send-token-conf";
    public static final String DISTCP_OPTIONS = "distcp.options.";
    public static final String MASK = "********";
    public static final String AWS_BUCKET_ENDPOINT = "fs.s3a.bucket.%s.endpoint";
    public static final String AWS_SSEKMSKEY = "fs.s3a.bucket.%s.server-side-encryption.key";
    public static final String START_TIME = "startTime";
    public static final String END_TIME = "endTime";
    public static final String META_LOCATION = "dlm-engine.metaLocation";
    public static final String PLUGIN_STAGING_DIR = "dlm-engine.pluginStagingDir";
    public static final String SNAPSHOT_DIR_PREFIX = ".snapshot";
    public static final String SNAPSHOT_PREFIX = "beacon-snapshot-";
}
