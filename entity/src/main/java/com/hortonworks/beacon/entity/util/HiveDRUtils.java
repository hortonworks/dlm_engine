/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.client.entity.Cluster.ClusterFields;
import com.hortonworks.beacon.client.entity.ReplicationPolicy.ReplicationPolicyFields;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.BeaconCloudCred;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.entity.HiveDRProperties;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.HiveActionType;
import com.hortonworks.beacon.util.StringFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Utility Class for Hive Repl Status.
 */
public final class HiveDRUtils {
    private static final Logger LOG = LoggerFactory.getLogger(HiveDRUtils.class);

    private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static final int TIMEOUT_IN_SECS = 300;
    public static final String JDBC_PREFIX = "jdbc:";
    public static final String BOOTSTRAP = "bootstrap";
    public static final String DEFAULT = "default";

    private HiveDRUtils() {}

    private static String getSourceHS2ConnectionUrl(Properties properties, HiveActionType actionType) {
        String connString;
        switch (actionType) {
            case EXPORT:
                connString = getHS2ConnectionUrl(properties.getProperty(HiveDRProperties.SOURCE_HS2_URI.getName()));
                break;
            case IMPORT:
                connString = getHS2ConnectionUrl(properties.getProperty(HiveDRProperties.TARGET_HS2_URI.getName()));
                break;
            default:
                throw new IllegalArgumentException(
                    StringFormat.format("Hive action type: {} is not supported.", actionType));
        }

        return connString;
    }

    public static String getHS2ConnectionUrl(final String hs2Uri) {
        StringBuilder connString = new StringBuilder();

        if (hs2Uri.contains("serviceDiscoveryMode=zooKeeper")) {
            connString.append(hs2Uri);
        } else {
            connString.append(JDBC_PREFIX).append(StringUtils.removeEnd(hs2Uri, "/"));
        }

        LOG.debug("getHS2ConnectionUrl connection uri: {}", connString);
        return connString.toString();
    }

    public static Connection getDriverManagerConnection(Properties properties,
                                                        HiveActionType actionType) throws BeaconException {
        String connString = getSourceHS2ConnectionUrl(properties, actionType);
        return getConnection(connString);
    }

    public static Connection getTargetConnection(Properties properties) throws BeaconException {
        boolean isDataLake = Boolean.valueOf(properties.getProperty(ClusterFields.CLOUDDATALAKE.getName()));
        LOG.info("Destination cluster is data lake: [{}]", isDataLake);
        Connection targetConnection;
        if (isDataLake) {
            targetConnection = HiveDRUtils.getDriverManagerConnection(properties, HiveActionType.EXPORT);
        } else {
            targetConnection = HiveDRUtils.getDriverManagerConnection(properties, HiveActionType.IMPORT);
        }
        return targetConnection;
    }

    public static String setConfigParameters(Properties properties) throws BeaconException {
        StringBuilder builder = new StringBuilder();
        String queueName = properties.getProperty(HiveDRProperties.QUEUE_NAME.getName());
        if (StringUtils.isNotBlank(queueName)) {
            appendConfig(builder, BeaconConstants.MAPRED_QUEUE_NAME, queueName);
        }
        appendConfig(builder, BeaconConstants.HIVE_EXEC_PARALLEL, "true");

        if (properties.containsKey(BeaconConstants.HA_CONFIG_KEYS)) {
            String haConfigKeys = properties.getProperty(BeaconConstants.HA_CONFIG_KEYS);
            for(String haConfigKey: haConfigKeys.split(BeaconConstants.COMMA_SEPARATOR)) {
                appendConfig(properties, builder, haConfigKey);
            }
        }

        if (UserGroupInformation.isSecurityEnabled()) {
            builder.append("'").append(BeaconConstants.MAPREDUCE_JOB_HDFS_SERVERS).append("'").
                    append(BeaconConstants.EQUAL_SEPARATOR).
                    append("'").append(properties.getProperty(HiveDRProperties.SOURCE_NN.getName())).
                    append(",").append(properties.getProperty(HiveDRProperties.TARGET_NN.getName())).append("'").
                    append(BeaconConstants.COMMA_SEPARATOR);

            appendConfig(builder, BeaconConstants.MAPREDUCE_JOB_SEND_TOKEN_CONF, PolicyHelper.getRMTokenConf());
        }
        boolean isDataLake = Boolean.valueOf(properties.getProperty(ClusterFields.CLOUDDATALAKE.getName()));
        if (!isDataLake && Boolean.valueOf(properties.getProperty(FSDRProperties.TDE_SAMEKEY.getName()))) {
            appendConfig(builder, BeaconConstants.HIVE_TDE_SAMEKEY, "true");
        }

        String user;
        try {
            user = UserGroupInformation.getLoginUser().getShortUserName();
        } catch (IOException e) {
            throw new BeaconException("Error while trying to get login user name:", e);
        }

        appendConfig(builder, BeaconConstants.HIVE_DISTCP_DOAS, user);

        if (isDataLake) {
            appendConfig(properties, builder, ClusterFields.HMSENDPOINT.getName());
            appendConfig(properties, builder, ClusterFields.HIVE_WAREHOUSE.getName());
            appendConfig(properties, builder, ClusterFields.HIVE_INHERIT_PERMS.getName());
            appendConfig(properties, builder, ClusterFields.HIVE_FUNCTIONS_DIR.getName());

            String cloudCredId = properties.getProperty(ReplicationPolicyFields.CLOUDCRED.getName());

            if (StringUtils.isNotBlank(cloudCredId)) {
                BeaconCloudCred cloudCred = new BeaconCloudCred(new CloudCredDao().getCloudCred(cloudCredId));
                Configuration cloudConf = cloudCred.getHadoopConf(false);
                appendConfig(builder, cloudConf);

                String warehouseDir = properties.getProperty(ClusterFields.HIVE_WAREHOUSE.getName());
                try {
                    appendConfig(builder, cloudCred.getBucketEndpointConf(warehouseDir));
                } catch (URISyntaxException e) {
                    throw new BeaconException("Hive metastore warehouse location not correct: {}", warehouseDir, e);
                }

            }
            String cloudEncryptionAlgorithm = properties.getProperty(
                    FSDRProperties.CLOUD_ENCRYPTIONALGORITHM.getName());

            if (StringUtils.isNotBlank(cloudEncryptionAlgorithm)) {
                appendConfig(builder, new BeaconCloudCred(new CloudCredDao().getCloudCred(cloudCredId))
                        .getCloudEncryptionTypeConf(properties));
            }

            if (properties.containsKey(HiveDRProperties.TARGET_HMS_KERBEROS_PRINCIPAL.getName())) {
                appendConfig(builder, HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.toString(),
                        properties.getProperty(HiveDRProperties.TARGET_HMS_KERBEROS_PRINCIPAL.getName()));
                appendConfig(builder, HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.toString(), "true");
            }
        }
        return  setDistcpOptions(builder, properties);
    }

    private static void appendConfig(StringBuilder builder, Configuration conf) {
        Iterator<Map.Entry<String, String>> iterator = conf.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            appendConfig(builder, entry.getKey(), entry.getValue());
        }
    }

    public static void appendConfig(Properties properties, StringBuilder builder, String nameKey, String
            valueKey) {
        appendConfig(builder, properties.getProperty(nameKey), properties.getProperty(valueKey));
    }

    public static void appendConfig(Properties properties, StringBuilder builder, String name) {
        appendConfig(builder, name, properties.getProperty(name));
    }

    public static void appendConfig(StringBuilder builder, String key, String value) {
        builder.append("'").append(key).append("'")
                .append(BeaconConstants.EQUAL_SEPARATOR)
                .append("'").append(value)
                .append("'").append(BeaconConstants.COMMA_SEPARATOR);
    }


    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings("DMI_EMPTY_DB_PASSWORD")
    public static Connection getConnection(String connString) throws BeaconException {
        Connection connection;
        String user = "";
        try {
            UserGroupInformation currentUser = UserGroupInformation.getLoginUser();
            if (currentUser != null) {
                user = currentUser.getShortUserName();
            }
            connection = DriverManager.getConnection(connString, user, "");
        } catch (IOException | SQLException ex) {
            LOG.error("Exception occurred initializing Hive server: {}", ex);
            throw new BeaconException("Exception occurred initializing Hive server: ", ex);
        }
        return connection;
    }

    public static void initializeDriveClass() throws BeaconException {
        try {
            Class.forName(DRIVER_NAME);
            DriverManager.setLoginTimeout(TIMEOUT_IN_SECS);
        } catch (ClassNotFoundException e) {
            LOG.error("{} not found: {}", DRIVER_NAME, e);
            throw new BeaconException(e, "{} not found: ", DRIVER_NAME);
        }
    }

    private static String setDistcpOptions(StringBuilder builder, Properties properties) {
        for (Map.Entry<Object, Object> prop : properties.entrySet()) {
            if (prop.getKey().toString().startsWith(BeaconConstants.DISTCP_OPTIONS)) {
                appendConfig(builder, prop.getKey().toString(), prop.getValue().toString());
            }
        }

        return builder.substring(0, builder.toString().length() - 1);
    }

    public static void cleanup(Statement statement, Connection connection) throws BeaconException {
        try {
            if (statement != null) {
                statement.close();
            }

            if (connection != null) {
                connection.close();
            }
        } catch (SQLException sqe) {
            throw new BeaconException("Exception occurred while closing connection: ", sqe);
        }
    }
}
