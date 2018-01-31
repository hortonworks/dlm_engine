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

import com.hortonworks.beacon.api.exception.BeaconWebException;
import com.hortonworks.beacon.client.result.DBListResult;
import com.hortonworks.beacon.client.result.FileListResult;
import com.hortonworks.beacon.client.result.FileListResult.FileList;
import com.hortonworks.beacon.api.util.ValidationUtil;
import com.hortonworks.beacon.client.entity.Cluster;
import com.hortonworks.beacon.client.resource.APIResult;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.entity.util.HiveDRUtils;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.fs.SnapshotListing;
import com.hortonworks.beacon.util.FSUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for Listing stored data.
 */

final class DatasetListing {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetListing.class);
    private static final String SHOW_DATABASES = "SHOW DATABASES";
    private static final String SHOW_TABLES = "SHOW TABLES";
    private static final String USE = "USE ";

    FileListResult listFiles(Cluster cluster, String path) throws BeaconException {
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

    FileListResult listCloudFiles(Configuration conf, String path) {
        try {
            Path cloudPath = new org.apache.hadoop.fs.Path(path);
            FileSystem fileSystem = FileSystem.get(cloudPath.toUri(), conf);
            RemoteIterator<FileStatus> iterator = fileSystem.listStatusIterator(cloudPath);
            ArrayList<FileList> fileLists = new ArrayList<>();
            while (iterator.hasNext()) {
                FileStatus status = iterator.next();
                FileList fileList = createFileList(status);
                fileList.isEncrypted = status.isEncrypted();
                fileLists.add(fileList);
            }
            FileListResult listResult = new FileListResult(APIResult.Status.SUCCEEDED, "Success");
            listResult.setCollection(fileLists.toArray(new FileList[fileLists.size()]));
            return listResult;
        } catch (IOException e) {
            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
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
        String hsEndPoint = cluster.getHsEndpoint();
        if (StringUtils.isBlank(hsEndPoint)) {
            throw new BeaconException("Hive Server end point is not specified in cluster entity");
        }
        HiveDRUtils.initializeDriveClass();
        String connString = HiveDRUtils.getHS2ConnectionUrl(hsEndPoint);
        Connection connection = null;
        Statement statement = null;
        DBListResult dbListResult;
        EncryptionZoneListing encryptionZoneListing = EncryptionZoneListing.get();
        SnapshotListing snapshotListing = SnapshotListing.get();
        try {
            connection = HiveDRUtils.getConnection(connString);
            statement = connection.createStatement();
            List<String> databases = showDatabases(statement);
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
                    String dbPath = getDatabasePath(statement, db, cluster.getName());
                    String baseEncryptedPath = encryptionZoneListing.getBaseEncryptedPath(cluster.getName(),
                            cluster.getFsEndpoint(), dbPath);
                    dbList.isEncrypted = StringUtils.isNotEmpty(baseEncryptedPath);
                    if (dbList.isEncrypted) {
                        dbList.encryptionKeyName = encryptionZoneListing.getEncryptionKeyName(cluster.getName(),
                                baseEncryptedPath);
                    }
                    dbList.snapshottable = snapshotListing.isSnapshottable(cluster.getName(), cluster.getFsEndpoint(),
                                    dbPath);
                    dbList.database = db;
                    dbLists[index++] = dbList;
                }

                dbListResult.setCollection(dbLists);
            } else {
                DBListResult.DBList[] dbList = new DBListResult.DBList[1];
                dbList[0] = new DBListResult.DBList();
                statement.execute(USE + dbName);
                dbList[0].database = dbName;
                try (ResultSet res = statement.executeQuery(SHOW_TABLES)) {
                    dbList[0].table = new ArrayList<>();
                    while (res.next()) {
                        String tableName = res.getString(1);
                        dbList[0].table.add(tableName);
                    }
                }

                dbListResult.setCollection(dbList);
            }
        } catch (SQLException sqe) {
            LOG.error("Exception occurred while validating Hive end point: {}", sqe);
            throw new ValidationException(sqe, "Exception occurred while validating Hive end point: ");
        } finally {
            HiveDRUtils.cleanup(statement, connection);
        }
        return dbListResult;
    }

    private List<String> showDatabases(final Statement statement) throws SQLException {
        List<String> dbList = new ArrayList<>();
        try (ResultSet res = statement.executeQuery(SHOW_DATABASES)) {
            while (res.next()) {
                String dbName = res.getString(1);
                dbList.add(dbName);
            }
        }
        return dbList;
    }

    private String getDatabasePath(final Statement statement, String dbName, String clusterName) throws SQLException,
            ValidationException {
        return ValidationUtil.getDatabasePath(statement, dbName, clusterName);
    }
}
