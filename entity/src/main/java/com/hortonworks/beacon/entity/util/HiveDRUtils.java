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

package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.HiveServerAuthenticationType;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.Cluster.ClusterFields;
import com.hortonworks.beacon.client.entity.ReplicationPolicy.ReplicationPolicyFields;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.config.Engine;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.BeaconCloudCred;
import com.hortonworks.beacon.entity.FSDRProperties;
import com.hortonworks.beacon.entity.HiveDRProperties;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.HiveActionType;
import com.hortonworks.beacon.util.KnoxTokenUtils;
import com.hortonworks.beacon.util.StringFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Utility Class for Hive Repl Status.
 */
public final class HiveDRUtils {
    private static final Logger LOG = LoggerFactory.getLogger(HiveDRUtils.class);

    public static final String JDBC_PREFIX = "jdbc:";
    public static final String BOOTSTRAP = "bootstrap";
    public static final String DEFAULT = "default";
    private static final String PATTERN_ZOOKEEPER_DISCOVERY = "^(.*?)serviceDiscoveryMode=zooKeeper[;]?(.*)$";
    private static final String PATTERN_ZOOKEEPER_NAMESPACE = "^(.*?)zooKeeperNamespace=[0-9a-zA-z]+[;]?(.*)$";

    private HiveDRUtils() {}

    private static String getSourceHS2ConnectionUrl(Properties properties, HiveActionType actionType)
            throws BeaconException{
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
        if (connString.endsWith(BeaconConstants.HIVE_SSO_COOKIE)) {
            String token =
                    KnoxTokenUtils.getKnoxSSOToken(properties.getProperty(ClusterFields.KNOX_GATEWAY_URL.getName()),
                            true);
            connString += "=" + token;
            LOG.debug("Connection URL with token " + connString);

        }
        return connString;
    }

    public static String getHS2ConnectionUrl(final String hs2Uri) {
        StringBuilder connString = new StringBuilder();

        if (hs2Uri.startsWith(JDBC_PREFIX)) {
            connString.append(hs2Uri);
        } else {
            connString.append(JDBC_PREFIX).append(StringUtils.removeEnd(hs2Uri, "/"));
        }

        LOG.debug("getHS2ConnectionUrl connection uri: {}", connString);
        return connString.toString();
    }

    public static String getConnectionString(Properties properties,
                                             HiveActionType actionType) throws BeaconException {
        return getSourceHS2ConnectionUrl(properties, actionType);
    }

    public static String getTargetConnectionString(Properties properties) throws BeaconException {
        boolean isDataLake = Boolean.valueOf(properties.getProperty(ClusterFields.CLOUDDATALAKE.getName()));
        LOG.info("Destination cluster is data lake: [{}]", isDataLake);
        if (isDataLake) {
            return HiveDRUtils.getSourceHS2ConnectionUrl(properties, HiveActionType.EXPORT);
        } else {
            return HiveDRUtils.getSourceHS2ConnectionUrl(properties, HiveActionType.IMPORT);
        }
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
            String warehouseDir = properties.getProperty(ClusterFields.HIVE_WAREHOUSE.getName());
            if (StringUtils.isNotBlank(cloudCredId)) {
                BeaconCloudCred cloudCred = new BeaconCloudCred(new CloudCredDao().getCloudCred(cloudCredId));
                Configuration cloudConf = cloudCred.getHadoopConf(false);
                appendConfig(builder, cloudConf);

                appendConfig(builder, cloudCred.getBucketEndpointConf(warehouseDir));
            }
            String cloudEncryptionAlgorithm = properties.getProperty(
                    FSDRProperties.CLOUD_ENCRYPTIONALGORITHM.getName());

            if (StringUtils.isNotBlank(cloudEncryptionAlgorithm)) {
                appendConfig(builder, new BeaconCloudCred(new CloudCredDao().getCloudCred(cloudCredId))
                        .getCloudEncryptionTypeConf(properties, warehouseDir));
            }
            setHMSKerberosProperties(builder, properties);
        }
        return  setDistcpOptions(builder, properties);
    }

    public static void setHMSKerberosProperties(StringBuilder builder, Properties properties) {
        String hs2Authentication = properties.getProperty(HiveDRProperties.TARGET_HIVE_SERVER_AUTHENTICATION.getName());
        if (StringUtils.isNotEmpty(hs2Authentication)
                && HiveServerAuthenticationType.getHiveServerAuthenticationType(hs2Authentication)
                == HiveServerAuthenticationType.KERBEROS) {
            appendConfig(builder, HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.
                    toString(), "true");
            appendConfig(builder,
                    HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.toString(),
                    properties.getProperty(HiveDRProperties.TARGET_HMS_KERBEROS_PRINCIPAL.getName()));
        }
    }

    private static void appendConfig(StringBuilder builder, Configuration conf) {
        Iterator<Map.Entry<String, String>> iterator = conf.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            appendConfig(builder, entry.getKey(), entry.getValue());
        }
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

    private static String setDistcpOptions(StringBuilder builder, Properties properties) {
        for (Map.Entry<Object, Object> prop : properties.entrySet()) {
            if (prop.getKey().toString().startsWith(BeaconConstants.DISTCP_OPTIONS)) {
                appendConfig(builder, prop.getKey().toString(), prop.getValue().toString());
            }
        }

        return builder.substring(0, builder.toString().length() - 1);
    }

    public static  String getKnoxProxiedURL(Cluster cluster) throws BeaconException {
        Engine engine = BeaconConfig.getInstance().getEngine();
        String srcKnoxURL = cluster.getKnoxGatewayURL();
        URI knoxUri = null;
        try {
            knoxUri = new URI(srcKnoxURL);
        } catch (URISyntaxException use) {
            throw new BeaconException("Invalid knox url provided", use);
        }
        String gatewayPath = knoxUri.getPath();
        if (gatewayPath.endsWith("/")) {
            gatewayPath = gatewayPath.substring(0, gatewayPath.length() - 1);
        }
        String httpPath = gatewayPath
                + KnoxTokenUtils.getKnoxProxiedURL("", "hive");
        String srcHiveURL = cluster.getHsEndpoint();
        int idx = srcHiveURL.indexOf(';');
        String fragment = null;
        if (idx >= 0) {
            fragment = srcHiveURL.substring(idx);
        }

        StringBuilder jdbcURL = new StringBuilder(JDBC_PREFIX)
                .append(BeaconConstants.HIVE_JDBC_PROVIDER)
                .append(knoxUri.getHost())
                .append(':').append(knoxUri.getPort())
                .append('/');
        if (fragment != null) {
            Pattern p1 = Pattern.compile(PATTERN_ZOOKEEPER_DISCOVERY);
            Pattern p2 = Pattern.compile(PATTERN_ZOOKEEPER_NAMESPACE);

            Matcher m1 = p1.matcher(fragment);
            if (m1.matches()) {
                String part1 = m1.group(1);
                String part2 = m1.group(2);
                Matcher m2 = p2.matcher(part1);
                if (m2.matches()) {
                    part1 = m2.group(1) + m2.group(2);
                }
                Matcher m3 = p2.matcher(part2);
                if (m3.matches()) {
                    part2 = m3.group(1) + m3.group(2);
                }
                fragment = part1 + part2;
            }
            jdbcURL.append(fragment);
        }
        if (!jdbcURL.toString().endsWith(";")) {
            jdbcURL.append(';');
        }
        jdbcURL.append(BeaconConstants.HIVE_TRANSPORT_MODE)
                .append('=').append(BeaconConstants.HIVE_TRANSPORT_MODE_HTTP).append(';')
                .append(BeaconConstants.HIVE_HTTP_PROXY_PATH).append('=').append(httpPath);
        if (!jdbcURL.toString().contains(BeaconConstants.HIVE_SSL_MODE)) {
            jdbcURL.append(';').append(BeaconConstants.HIVE_SSL_MODE);
        }
        if (engine.isTlsEnabled()) {
            try {
                if (!jdbcURL.toString().contains(BeaconConstants.HIVE_SSL_TRUST_STORE)) {
                    jdbcURL
                            .append(';').append(BeaconConstants.HIVE_SSL_TRUST_STORE).append('=')
                            .append(URLEncoder.encode(engine.getTrustStore(), "UTF-8"))
                            .append(';').append(BeaconConstants.HIVE_SSL_TRUST_STORE_PASSWORD).append('=')
                            .append(URLEncoder.encode(engine.resolveTrustStorePassword(), "UTF-8"));
                }
            } catch(IOException ioe) {
                throw new BeaconException("Unable to encode jdbcURL : " + jdbcURL, ioe);
            }
        }
        // Value will be added when connection is created using this URL
        jdbcURL.append(';').append(BeaconConstants.HIVE_SSO_COOKIE);
        return jdbcURL.toString();
    }

}
