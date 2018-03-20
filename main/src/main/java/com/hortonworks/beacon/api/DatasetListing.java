/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.api;

import com.google.common.base.Preconditions;
import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.client.result.DBListResult;
import com.hortonworks.beacon.client.result.FileListResult;
import com.hortonworks.beacon.client.result.FileListResult.FileList;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.entity.util.hive.HiveMetadataClient;
import com.hortonworks.beacon.entity.util.hive.HiveMetadataClientFactory;
import com.hortonworks.beacon.replication.fs.SnapshotListing;
import com.hortonworks.beacon.util.FSUtils;
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

    FileListResult listFiles(Cluster cluster, String path) throws BeaconException {
        Preconditions.checkArgument(StringUtils.isNotEmpty(cluster.getFsEndpoint()), "Namenode Endpoint");
        String dataset = FSUtils.getStagingUri(cluster.getFsEndpoint(), path);
        FileListResult fileListResult;

        try {
            FileSystem fs = FSUtils.getFileSystem(cluster.getFsEndpoint(), new Configuration(), false);
            FileStatus []fileStatuses = fs.listStatus(new Path(dataset));
            if (fileStatuses.length==0) {
                fileListResult = new FileListResult(APIResult.Status.SUCCEEDED, "Empty");
            } else {
                fileListResult = new FileListResult(APIResult.Status.SUCCEEDED, "Success");
            }

            EncryptionZoneListing encryptionZoneListing = EncryptionZoneListing.get();
            SnapshotListing snapshotListing = SnapshotListing.get();

            String baseEncryptedPath = encryptionZoneListing.getBaseEncryptedPath(cluster.getName(),
                    cluster.getFsEndpoint(), dataset);
            boolean parentEncrypted = encryptionZoneListing.isEncrypted(baseEncryptedPath);
            String parentEncryptionKey = encryptionZoneListing.getEncryptionKeyName(cluster.getName(),
                    baseEncryptedPath);

            FileListResult.FileList[] fileLists = new FileListResult.FileList[fileStatuses.length];
            int index = 0;
            for (FileStatus status : fileStatuses) {
                FileList fileList = createFileList(status);
                if (!parentEncrypted) {
                    String encryptedPath = encryptionZoneListing.getBaseEncryptedPath(cluster.getName(),
                            cluster.getFsEndpoint(), status.getPath().toString());
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

    DBListResult listHiveDBDetails(Cluster cluster, String dbName) throws BeaconException {
        HiveMetadataClient hiveClient = null;
        try {
            hiveClient = HiveMetadataClientFactory.getClient(cluster);

            DBListResult dbListResult;
            EncryptionZoneListing encryptionZoneListing = EncryptionZoneListing.get();
            SnapshotListing snapshotListing = SnapshotListing.get();
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
                    Path dbPath = hiveClient.getDatabaseLocation(db);
                    String baseEncryptedPath = encryptionZoneListing.getBaseEncryptedPath(cluster.getName(),
                            cluster.getFsEndpoint(), dbPath.toString());
                    dbList.isEncrypted = StringUtils.isNotEmpty(baseEncryptedPath);
                    if (dbList.isEncrypted) {
                        dbList.encryptionKeyName = encryptionZoneListing.getEncryptionKeyName(cluster.getName(),
                                baseEncryptedPath);
                    }
                    dbList.snapshottable = snapshotListing.isSnapshottable(cluster.getName(), cluster.getFsEndpoint(),
                                    dbPath.toString());
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
            HiveMetadataClientFactory.close(hiveClient);
        }
    }
}
