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

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.result.DBListResult;
import com.hortonworks.beacon.client.result.FileListResult;
import com.hortonworks.beacon.client.result.FileListResult.FileList;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.BeaconCluster;
import com.hortonworks.beacon.entity.util.hive.HiveClientFactory;
import com.hortonworks.beacon.entity.util.hive.HiveMetadataClient;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.fs.SnapshotListing;
import com.hortonworks.dlmengine.fs.hdfs.EncryptionZoneListing;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for Listing stored data.
 */

final class DatasetListing {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetListing.class);

    FileListResult listFiles(BeaconCluster cluster, String path, String filter) throws BeaconException {
        if (filter == null) {
            filter = StringUtils.EMPTY;
        }
        FileListResult fileListResult = new FileListResult(APIResult.Status.SUCCEEDED, "Empty");

        try {
            FileSystem fs = cluster.getFileSystem();
            if (fs == null) {
                return fileListResult;
            }

            RemoteIterator<FileStatus> iterator = fs.listStatusIterator(new Path(path));
            List<FileStatus> fileStatusList = new ArrayList<>();
            long totalResults = 0;
            long maxFilePerPage = BeaconConfig.getInstance().getEngine().getMaxFileListPerPage();
            if (maxFilePerPage == -1) {
                maxFilePerPage = Long.MAX_VALUE;
            }
            while (iterator.hasNext()) {
                FileStatus fileStatus = iterator.next();
                if (fileStatusList.size() < maxFilePerPage && fileStatus.getPath().getName().contains(filter)) {
                    fileStatusList.add(fileStatus);
                }
                totalResults++;
            }

            if (fileStatusList.size() > 0) {
                fileListResult = new FileListResult(APIResult.Status.SUCCEEDED, "Success");
            }

            EncryptionZoneListing encryptionZoneListing = EncryptionZoneListing.get();
            SnapshotListing snapshotListing = SnapshotListing.get();

            String baseEncryptedPath = encryptionZoneListing.getBaseEncryptedPath(cluster, path);
            boolean parentEncrypted = encryptionZoneListing.isEncrypted(baseEncryptedPath);
            String parentEncryptionKey = encryptionZoneListing.getEncryptionKeyName(cluster.getName(),
                    baseEncryptedPath);

            FileListResult.FileList[] fileLists = new FileListResult.FileList[fileStatusList.size()];
            int index = 0;
            for (FileStatus status : fileStatusList) {
                FileList fileList = createFileList(status);
                if (!parentEncrypted) {
                    String encryptedPath = encryptionZoneListing.getBaseEncryptedPath(cluster,
                            status.getPath().toString());
                    fileList.isEncrypted = encryptionZoneListing.isEncrypted(encryptedPath);
                    fileList.encryptionKeyName = encryptionZoneListing.getEncryptionKeyName(cluster.getName(),
                            encryptedPath);
                } else {
                    fileList.isEncrypted = true;
                    fileList.encryptionKeyName = parentEncryptionKey;
                }
                fileList.snapshottable =
                        snapshotListing.isSnapshottable(cluster.getName(), cluster.getFsEndpoint(),
                                status.getPath().toString());
                fileLists[index++] = fileList;
            }
            fileListResult.setCollection(fileLists);
            fileListResult.setTotalResults(totalResults);
        } catch (IOException ioe) {
            LOG.error("Exception occurred while accessing file status : {}", ioe);
            throw new BeaconException("Exception occurred while accessing file status : ", ioe);
        }
        return fileListResult;
    }

    FileListResult listCloudFiles(CloudCred.Provider provider, Configuration conf, String path) {
        try {
            Path cloudPath = new org.apache.hadoop.fs.Path(path);
            FileSystem fileSystem = FileSystem.get(cloudPath.toUri(), conf);
            RemoteIterator<FileStatus> iterator = fileSystem.listStatusIterator(cloudPath);
            ArrayList<FileList> fileLists = new ArrayList<>();
            boolean expThrown = false;
            while (iterator.hasNext()) {
                FileStatus status = iterator.next();
                FileList fileList = createFileList(status);
                if (CloudCred.Provider.AWS.equals(provider) && !status.isDirectory() && !expThrown) {
                    try {
                        String objectPath;
                        String pathSeperator = Path.SEPARATOR;
                        if (path.endsWith(pathSeperator)) {
                            objectPath = path + fileList.pathSuffix;
                        } else {
                            objectPath = path + pathSeperator + fileList.pathSuffix;
                        }
                        fileList.isEncrypted = isObjectEncypted(fileSystem, objectPath);
                    } catch (BeaconException e) {
                        expThrown = true;
                        LOG.warn("Exception while retrieving encryption algo of AWS S3 object {}, will skip for rest",
                                fileList.pathSuffix, e);
                    }
                } else {
                    fileList.isEncrypted = status.isEncrypted();
                }
                fileLists.add(fileList);
            }
            FileListResult listResult = new FileListResult(APIResult.Status.SUCCEEDED, "Success");
            listResult.setCollection(fileLists.toArray(new FileList[fileLists.size()]));
            return listResult;
        } catch (IOException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    private static boolean isObjectEncypted(FileSystem fs, String pathSuffix) throws BeaconException {
        try {
            String encAlgo = ((S3AFileSystem)fs).getObjectMetadata(new Path(pathSuffix)).getSSEAlgorithm();
            return StringUtils.isNotBlank(encAlgo);
        } catch (Exception ex) {
            throw new BeaconException(ex);
        }
    }

    private FileList createFileList(FileStatus status) {
        FileList fileList = new FileList();
        fileList.accessTime = status.getAccessTime();
        fileList.blockSize = status.getBlockSize();
        fileList.group = status.getGroup();
        fileList.length = status.getLen();
        fileList.modificationTime = status.getModificationTime();
        fileList.owner = status.getOwner();
        fileList.pathSuffix = status.getPath().getName();
        fileList.permission = status.getPermission().toString();
        fileList.replication = status.getReplication();
        fileList.type = ((status.isDirectory()) ? "DIRECTORY" : "FILE");
        return fileList;
    }

    DBListResult listHiveDBDetails(BeaconCluster cluster, String dbName) throws BeaconException {
        HiveMetadataClient hiveClient = null;
        try {
            hiveClient = HiveClientFactory.getMetadataClient(cluster);

            DBListResult dbListResult;
            EncryptionZoneListing encryptionZoneListing = EncryptionZoneListing.get();
            List<String> databases = hiveClient.listDatabases();
            if (databases.size()==0) {
                dbListResult = new DBListResult(APIResult.Status.SUCCEEDED, "Empty");
            } else {
                dbListResult = new DBListResult(APIResult.Status.SUCCEEDED, "Success");
            }
            if (StringUtils.isBlank(dbName)) {
                DBListResult.DBList[] dbLists = new DBListResult.DBList[databases.size()];
                int index = 0;
                for (String db : databases) {
                    DBListResult.DBList dbList = new DBListResult.DBList();
                    Path dbLocation = hiveClient.getDatabaseLocation(db);
                    String baseEncryptedPath = encryptionZoneListing.getBaseEncryptedPath(cluster,
                            dbLocation.toString());
                    dbList.isEncrypted = StringUtils.isNotEmpty(baseEncryptedPath);
                    if (dbList.isEncrypted) {
                        dbList.encryptionKeyName = encryptionZoneListing.getEncryptionKeyName(cluster.getName(),
                                baseEncryptedPath);
                    }
                    dbList.database = db;
                    dbLists[index++] = dbList;
                }

                dbListResult.setCollection(dbLists);
            } else {
                DBListResult.DBList[] dbList = new DBListResult.DBList[1];
                dbList[0] = new DBListResult.DBList();
                dbList[0].database = dbName;
                dbList[0].table = hiveClient.getTables(dbName);
                dbListResult.setCollection(dbList);
            }
            return dbListResult;
        } finally {
            HiveClientFactory.close(hiveClient);
        }
    }
}
