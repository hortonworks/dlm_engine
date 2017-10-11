/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.entity;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.HiveDRUtils;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.log.BeaconLog;
import com.hortonworks.beacon.notification.BeaconNotification;
import com.hortonworks.beacon.rb.MessageCode;
import com.hortonworks.beacon.util.FileSystemClientFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.NoSuchElementException;
import java.util.Properties;

/**
 * Validation helper function to validate Beacon Cluster definition.
 */
public class ClusterValidator extends EntityValidator<Cluster> {
    private static final BeaconLog LOG = BeaconLog.getLog(ClusterValidator.class);
    public static final String FS_DEFAULT_NAME_KEY = "fs.defaultFS";
    private static final String IPC_MAX_TRIES = "ipc.client.connect.max.retries";
    private static final String SHOW_DATABASES = "SHOW DATABASES";

    ClusterValidator() {
        super(EntityType.CLUSTER);
    }

    @Override
    public void validate(Cluster entity) throws BeaconException {
        if (entity.isLocal()) {
            validateClusterName(entity);
            validateClusterExists();
            validateCustomSetup(entity);
        }
        if (ClusterHelper.isHighlyAvailableHDFS(entity.getCustomProperties())) {
            validateHAConfig(entity.getCustomProperties());
        }
        validateFSInterface(entity);
        validateHiveInterface(entity);
    }

    private void validateCustomSetup(Cluster entity) throws ValidationException {
        Configuration defaultConf = new Configuration();
        boolean isHA = StringUtils.isNotBlank(defaultConf.get(BeaconConstants.DFS_NAMESERVICES));
        BeaconNotification notification = new BeaconNotification();
        if (isHA != ClusterHelper.isHighlyAvailableHDFS(entity.getCustomProperties())) {
            notification.addError(MessageCode.ENTI_000023.getMsg());
        }
        if (ClusterHelper.isHiveEnabled(entity.getHsEndpoint())
                && (ClusterHelper.isHighlyAvailableHive(entity.getHsEndpoint())) != isHA) {
            LOG.warn(MessageCode.ENTI_000024.name());
        }
        if (UserGroupInformation.isSecurityEnabled() && !ClusterHelper.isKerberized(entity)) {
            notification.addError(MessageCode.ENTI_000026.getMsg());
        }
        if (notification.hasErrors()) {
            throw new ValidationException(MessageCode.ENTI_000028.name(), notification.errorMessage());
        }
    }

    private void validateFSInterface(Cluster entity) throws ValidationException {
        String fsEndPoint = entity.getFsEndpoint();
        Configuration conf = ClusterHelper.getHAConfigurationOrDefault(entity);
        if (entity.isLocal()) {
            String defaultStorageUrl = conf.get(FS_DEFAULT_NAME_KEY).trim();
            if (!defaultStorageUrl.equals(fsEndPoint)) {
                throw new ValidationException(MessageCode.ENTI_000027.name(), fsEndPoint, defaultStorageUrl);
            }
        }
        validateFileSystem(fsEndPoint, conf);
    }

    private void validateFileSystem(String storageUrl, Configuration conf) throws ValidationException {
        try {
            LOG.debug(MessageCode.ENTI_000010.name(), storageUrl);
            conf.set(FS_DEFAULT_NAME_KEY, storageUrl);
            conf.setInt(IPC_MAX_TRIES, 10);
            FileSystem fs = FileSystemClientFactory.get().createProxiedFileSystem(conf);
            fs.exists(new Path("/"));
        } catch (Exception e) {
            LOG.error(MessageCode.ENTI_000012.name(), storageUrl + ", " + e);
            throw new ValidationException(MessageCode.ENTI_000010.name(), e, storageUrl + ", " + e.getMessage());
        }
    }

    private void validateHiveInterface(Cluster entity) throws BeaconException {
        String hsEndPoint = entity.getHsEndpoint();
        LOG.debug(MessageCode.ENIT_000011.name(), hsEndPoint);
        if (StringUtils.isBlank(hsEndPoint)) {
            return;
        }

        HiveDRUtils.initializeDriveClass();
        Properties properties = new Properties();
        properties.put(HiveDRProperties.QUEUE_NAME.getName(), "default");
        String connString = HiveDRUtils.getHS2ConnectionUrl(hsEndPoint);
        Connection connection = null;
        Statement statement = null;
        try {
            connection = HiveDRUtils.getConnection(connString);
            statement = connection.createStatement();
            try (ResultSet rs = statement.executeQuery(SHOW_DATABASES)) {
                if (rs.next()) {
                    String db = rs.getString(1);
                    if (db == null) {
                        throw new SQLException(MessageCode.ENTI_000013.name(), hsEndPoint);
                    }
                }
            }
        } catch (Exception sqe) {
            LOG.error(MessageCode.ENTI_000014.name(), sqe.getMessage());
            throw new ValidationException(MessageCode.ENTI_000014.name(), sqe.getMessage(), sqe);
        } finally {
            HiveDRUtils.cleanup(statement, connection);
        }
    }

    private void validateClusterExists() throws BeaconException {
        try {
            Cluster localCluster = ClusterHelper.getLocalCluster();
            if (localCluster != null) {
                throw new ValidationException(MessageCode.ENTI_000016.name(), localCluster.getName());
            }
        } catch (NoSuchElementException e) {
            //nothing to do.
        }
    }

    private void validateClusterName(Cluster cluster) throws ValidationException {
        boolean localCluster = ClusterHelper.isLocalCluster(cluster.getName());
        if (!localCluster) {
            throw new ValidationException(MessageCode.ENTI_000015.name(),
                    BeaconConfig.getInstance().getEngine().getLocalClusterName());
        }
    }

    private static void validateHAConfig(Properties properties) throws BeaconException {
        LOG.debug(MessageCode.ENTI_000017.name());
        String dfsNameServices = properties.getProperty(BeaconConstants.DFS_NAMESERVICES);
        String haNamenodesPrimaryKey = BeaconConstants.DFS_HA_NAMENODES + BeaconConstants.DOT_SEPARATOR
                + dfsNameServices;
        String  haNameNodesPrimaryValue;
        if (properties.containsKey(haNamenodesPrimaryKey)) {
            haNameNodesPrimaryValue = properties.getProperty(haNamenodesPrimaryKey);
        } else {
            throw new BeaconException(MessageCode.COMM_010002.name(), haNamenodesPrimaryKey);
        }
        String []haNameNodes = haNameNodesPrimaryValue.split(BeaconConstants.COMMA_SEPARATOR);
        String haNameNodeAddressPrefix = BeaconConstants.DFS_NN_RPC_PREFIX + BeaconConstants.DOT_SEPARATOR
                + dfsNameServices;
        for(String haNameNodeName: haNameNodes) {
            String haNameNodeAddress = haNameNodeAddressPrefix + BeaconConstants.DOT_SEPARATOR
                    + haNameNodeName;
            if (!properties.containsKey(haNameNodeAddress)) {
                throw new BeaconException(MessageCode.COMM_010002.name(), haNameNodeAddress);
            }
        }
    }
}
