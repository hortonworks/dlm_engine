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

package com.hortonworks.beacon.entity;

import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.entity.exceptions.EntityAlreadyExistsException;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.ClusterHelper;
import com.hortonworks.beacon.entity.util.hive.HiveClientFactory;
import com.hortonworks.beacon.entity.util.hive.HiveMetadataClient;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.notification.BeaconNotification;
import com.hortonworks.beacon.util.FileSystemClientFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;
import java.util.Properties;

/**
 * Validation helper function to validate Beacon Cluster definition.
 */
public class ClusterValidator extends EntityValidator<Cluster> {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterValidator.class);
    private static final String IPC_MAX_TRIES = "ipc.client.connect.max.retries";

    ClusterValidator() {
        super(EntityType.CLUSTER);
    }

    @Override
    public void validate(Cluster entity) throws BeaconException {
        validateClusterExists(entity.getName());

        if (entity.isLocal()) {
            validateLocalCluster(entity.getName());
            validateCustomSetup(entity);
            boolean knoxProxyEnabled = BeaconConfig.getInstance().getEngine().isKnoxProxyEnabled();

            if (knoxProxyEnabled && StringUtils.isBlank(entity.getKnoxGatewayURL())) {
                LOG.error("Knox proxy URL is empty when knox proxy is enabled in local cluster {}",
                        entity.getName());
            }
        }

        if (ClusterHelper.isHDFSEnabled(entity)) {
            validateFSInterface(entity);
            if (ClusterHelper.isHighlyAvailableHDFS(entity.getCustomProperties())) {
                validateHAConfig(entity.getCustomProperties());
            }
        }
        validateHiveInterface(entity);
    }

    private void validateCustomSetup(Cluster entity) throws ValidationException {
        Configuration defaultConf = new Configuration();
        BeaconNotification notification = new BeaconNotification();
        boolean isHA = StringUtils.isNotBlank(defaultConf.get(BeaconConstants.DFS_NAMESERVICES));
        if (ClusterHelper.isHDFSEnabled(entity)) {
            if (isHA != ClusterHelper.isHighlyAvailableHDFS(entity.getCustomProperties())) {
                notification.addError("NameNode HA setup is not correct");
            }
        }
        if (ClusterHelper.isHiveEnabled(entity.getHsEndpoint())
                && (ClusterHelper.isHighlyAvailableHive(entity.getHsEndpoint())) != isHA) {
            LOG.warn("Hive HA setup is not correct");
        }
        if (UserGroupInformation.isSecurityEnabled() && !ClusterHelper.isKerberized(entity)) {
            notification.addError("Kerberos setup is not correct");
        }
        if (notification.hasErrors()) {
            throw new ValidationException("Issues found while validating cluster information: {}",
                notification.errorMessage());
        }
    }

    public void validateFSInterface(Cluster entity) throws ValidationException {
        String fsEndPoint = entity.getFsEndpoint();
        Configuration conf = ClusterHelper.getHAConfigurationOrDefault(entity);
        if (entity.isLocal()) {
            String defaultStorageUrl = conf.get(BeaconConstants.FS_DEFAULT_NAME_KEY).trim();
            if (!defaultStorageUrl.equals(fsEndPoint)) {
                throw new ValidationException(
                    "FS Endpoint provided {} did not match with cluster default FS endpoint {}", fsEndPoint,
                    defaultStorageUrl);
            }
        }
        validateFileSystem(fsEndPoint, conf);
    }

    private void validateFileSystem(String storageUrl, Configuration conf) throws ValidationException {
        try {
            LOG.debug("Validating File system end point: {}", storageUrl);
            conf.set(BeaconConstants.FS_DEFAULT_NAME_KEY, storageUrl);
            conf.setInt(IPC_MAX_TRIES, 10);
            FileSystem fs = FileSystemClientFactory.get().createFileSystem(conf);
            fs.exists(new Path("/"));
        } catch (Exception e) {
            LOG.error("Invalid Filesystem server or port: {}", storageUrl, e);
            throw new ValidationException(e, "Validating File system end point: {}", storageUrl);
        }
    }

    public void validateHiveInterface(Cluster entity) throws BeaconException {
        LOG.debug("Validating Hive end point - HS2:{}, HMS:{}", entity.getHsEndpoint(), entity.getHmsEndpoint());
        if (!ClusterHelper.isHiveEnabled(entity)) {
            return;
        }

        Properties properties = new Properties();
        properties.put(HiveDRProperties.QUEUE_NAME.getName(), "default");
        HiveMetadataClient hiveMetadataClient = null;
        try {
            hiveMetadataClient = HiveClientFactory.getMetadataClient(entity);
            hiveMetadataClient.listDatabases();
        } catch (Exception sqe) {
            LOG.error("Exception occurred while validating Hive end point: {}", sqe.getMessage());
            throw new ValidationException(sqe, "Exception occurred while validating Hive end point: ");
        } finally {
            HiveClientFactory.close(hiveMetadataClient);
        }
    }

    private void validateLocalCluster(String clusterName) throws BeaconException {
        //validate that local cluster already exists
        try {
            Cluster localCluster = ClusterHelper.getLocalCluster();
            if (localCluster != null) {
                throw new ValidationException("Local cluster: {} already exists.", localCluster);
            }
        } catch (NoSuchElementException e) {
            // nothing to do.
        }

        //validate that cluster name is same as beacon service cluster name
        boolean localCluster = ClusterHelper.isLocalCluster(clusterName);
        if (!localCluster) {
            throw new ValidationException("Submitted cluster name {} does not match with local "
                    + "cluster name: {}", clusterName,
                    BeaconConfig.getInstance().getEngine().getLocalClusterName());
        }
    }

    private void validateClusterExists(String clusterName) throws BeaconException {
        try {
            ClusterHelper.getActiveCluster(clusterName);
            throw new EntityAlreadyExistsException("Cluster entity already exists with name: {}", clusterName);
        } catch (NoSuchElementException e) {
            // nothing to do.
        }
    }

    private static void validateHAConfig(Properties properties) throws BeaconException {
        LOG.debug("Validating HA Config");
        String dfsNameServices = properties.getProperty(BeaconConstants.DFS_NAMESERVICES);
        String haNamenodesPrimaryKey = BeaconConstants.DFS_HA_NAMENODES + BeaconConstants.DOT_SEPARATOR
                + dfsNameServices;
        String  haNameNodesPrimaryValue;
        if (properties.containsKey(haNamenodesPrimaryKey)) {
            haNameNodesPrimaryValue = properties.getProperty(haNamenodesPrimaryKey);
        } else {
            throw new BeaconException("Missing parameter: {}", haNamenodesPrimaryKey);
        }
        String []haNameNodes = haNameNodesPrimaryValue.split(BeaconConstants.COMMA_SEPARATOR);
        String haNameNodeAddressPrefix = BeaconConstants.DFS_NN_RPC_PREFIX + BeaconConstants.DOT_SEPARATOR
                + dfsNameServices;
        for(String haNameNodeName: haNameNodes) {
            String haNameNodeAddress = haNameNodeAddressPrefix + BeaconConstants.DOT_SEPARATOR
                    + haNameNodeName;
            if (!properties.containsKey(haNameNodeAddress)) {
                throw new BeaconException("Missing parameter: {}", haNameNodeAddress);
            }
        }
    }
}
