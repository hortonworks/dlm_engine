/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.hive.util;

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.hive.exception.HiveReplicationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hive.hcatalog.api.repl.Command;
import org.apache.hive.hcatalog.api.repl.ReplicationUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to handle Hive events for data-mirroring.
 */
public class EventUtils {
    private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static final int TIMEOUT_IN_SECS = 300;
    private static final String JDBC_PREFIX = "jdbc:";
    private static final int RETRY_ATTEMPTS = 3;

    private Configuration conf = null;
    private String sourceHiveServer2Uri = null;
    private String sourceDatabase = null;
    private String sourceNN = null;
    private String targetHiveServer2Uri = null;
    private String targetStagingPath = null;
    private String targetNN = null;
    private String targetStagingUri = null;
    private List<Path> sourceCleanUpList = null;
    private List<Path> targetCleanUpList = null;
    private static final Logger LOG = LoggerFactory.getLogger(EventUtils.class);

    private FileSystem sourceFileSystem = null;
    private FileSystem targetFileSystem = null;
    private Connection sourceConnection = null;
    private Connection targetConnection = null;
    private Statement sourceStatement = null;
    private Statement targetStatement = null;

    private List<ReplicationStatus> listReplicationStatus;

    public EventUtils(Configuration conf) {
        this.conf = conf;
        sourceHiveServer2Uri = conf.get("sourceHiveServer2Uri");
        sourceDatabase = conf.get("sourceDatabase");
        sourceNN = conf.get("sourceNN");

        targetHiveServer2Uri = conf.get("targetHiveServer2Uri");
        targetStagingPath = conf.get("targetStagingPath");
        targetNN = conf.get("targetNN");
        sourceCleanUpList = new ArrayList<Path>();
        targetCleanUpList = new ArrayList<Path>();
    }

    public void setupConnection() throws Exception {
        Class.forName(DRIVER_NAME);
        DriverManager.setLoginTimeout(TIMEOUT_IN_SECS);

        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        String user = "";
        if (currentUser != null) {
            user = currentUser.getShortUserName();
        }
        conf.setBoolean("HADOOP_USER_CLASSPATH_FIRST", true);
        sourceConnection = DriverManager.getConnection(JDBC_PREFIX + sourceHiveServer2Uri + "/" + sourceDatabase,
                user, "");
        targetConnection = DriverManager.getConnection(JDBC_PREFIX + targetHiveServer2Uri + "/" + sourceDatabase,
                user, "");
        sourceStatement = sourceConnection.createStatement();
        targetStatement = targetConnection.createStatement();
    }

    public void initializeFS() throws IOException {
        LOG.info("Initializing staging directory");
        targetStagingUri = targetNN + targetStagingPath;
        sourceFileSystem = FileSystem.get(FileUtils.getConfiguration(sourceNN));
        targetFileSystem = FileSystem.get(FileUtils.getConfiguration(targetNN));
    }

    public void processEvents(String event) throws Exception {
        LOG.debug("EventUtils processEvents event to process: {}", event);
        listReplicationStatus = new ArrayList<ReplicationStatus>();
        String[] eventSplit = event.split(DelimiterUtils.FIELD_DELIM);
        String dbName = eventSplit[0];
        String tableName = eventSplit[1];
        String exportEventStr = eventSplit[2];
        String importEventStr = eventSplit[3];
        if (StringUtils.isNotEmpty(exportEventStr)) {
            LOG.info("Process the export statements for db {} table {}", dbName, tableName);
            processCommands(exportEventStr, dbName, tableName, sourceStatement, sourceCleanUpList, false);
            if (!sourceCleanUpList.isEmpty()) {
                invokeCopy(sourceCleanUpList);
            }
        }

        if (StringUtils.isNotEmpty(importEventStr)) {
            LOG.info("Process the import statements for db {} table {}", dbName, tableName);
            processCommands(importEventStr, dbName, tableName, targetStatement, targetCleanUpList, true);
        }
    }

    public List<ReplicationStatus> getListReplicationStatus() {
        return listReplicationStatus;
    }

    private void processCommands(String eventStr, String dbName, String tableName, Statement sqlStmt,
                                 List<Path> cleanUpList, boolean isImportStatements)
        throws SQLException, HiveReplicationException, IOException {
        String[] commandList = eventStr.split(DelimiterUtils.STMT_DELIM);
        for (String command : commandList) {
            LOG.debug(" Hive DR Deserialize : {} :", command);
            try {
                Command cmd = ReplicationUtils.deserializeCommand(command);
                List<String> cleanupLocations = cmd.cleanupLocationsAfterEvent();
                cleanUpList.addAll(getCleanUpPaths(cleanupLocations));
                LOG.debug("Executing command : {} : {} ", cmd.getEventId(), cmd.toString());
                executeCommand(cmd, dbName, tableName, sqlStmt, isImportStatements, 0);
            } catch (Exception e) {
                // clean up locations before failing.
                cleanupEventLocations(sourceCleanUpList, sourceFileSystem);
                cleanupEventLocations(targetCleanUpList, targetFileSystem);
                throw new HiveReplicationException("Could not process replication command for "
                        + " DB Name:" + dbName + ", Table Name:" + tableName, e);
            }
        }
    }

    private void executeCommand(Command cmd, String dbName, String tableName,
                                Statement sqlStmt, boolean isImportStatements, int attempt)
        throws HiveReplicationException, SQLException, IOException {
        for (final String stmt : cmd.get()) {
            executeSqlStatement(cmd, dbName, tableName, sqlStmt, stmt, isImportStatements, attempt);
        }
        if (isImportStatements) {
            addReplicationStatus(ReplicationStatus.Status.SUCCESS, dbName, tableName, cmd.getEventId());
        }
    }

    private void executeSqlStatement(Command cmd, String dbName, String tableName,
                                     Statement sqlStmt, String stmt, boolean isImportStatements, int attempt)
        throws HiveReplicationException, SQLException, IOException {
        try {
            sqlStmt.execute(stmt);
        } catch (SQLException sqeOuter) {
            // Retry if command is retriable.
            if (attempt < RETRY_ATTEMPTS && cmd.isRetriable()) {
                if (isImportStatements) {
                    try {
                        cleanupEventLocations(getCleanUpPaths(cmd.cleanupLocationsPerRetry()), targetFileSystem);
                    } catch (IOException ioe) {
                        // Clean up failed before retry on target. Update failure status and return
                        addReplicationStatus(ReplicationStatus.Status.FAILURE, dbName,
                                tableName, cmd.getEventId());
                        throw ioe;
                    }
                } else {
                    cleanupEventLocations(getCleanUpPaths(cmd.cleanupLocationsPerRetry()), sourceFileSystem);
                }
                executeCommand(cmd, dbName, tableName, sqlStmt, isImportStatements, ++attempt);
                return; // Retry succeeded, return without throwing an exception.
            }
            // If we reached here, retries have failed.
            LOG.error("SQL Exception: {}", sqeOuter);
            undoCommand(cmd, dbName, tableName, sqlStmt, isImportStatements);
            if (isImportStatements) {
                addReplicationStatus(ReplicationStatus.Status.FAILURE, dbName, tableName, cmd.getEventId());
            }
            throw sqeOuter;
        }
    }

    private static List<Path> getCleanUpPaths(List<String> cleanupLocations) {
        List<Path> cleanupLocationPaths = new ArrayList<Path>();
        for (String cleanupLocation : cleanupLocations) {
            cleanupLocationPaths.add(new Path(cleanupLocation));
        }
        return cleanupLocationPaths;
    }

    private void undoCommand(Command cmd, String dbName,
                             String tableName, Statement sqlStmt, boolean isImportStatements)
        throws SQLException, HiveReplicationException {
        if (cmd.isUndoable()) {
            try {
                List<String> undoCommands = cmd.getUndo();
                LOG.debug("Undo command: {}", StringUtils.join(undoCommands.toArray()));
                if (undoCommands.size() != 0) {
                    for (final String undoStmt : undoCommands) {
                        sqlStmt.execute(undoStmt);
                    }
                }
            } catch (SQLException sqeInner) {
                if (isImportStatements) {
                    addReplicationStatus(ReplicationStatus.Status.FAILURE, dbName,
                            tableName, cmd.getEventId());
                }
                LOG.error("SQL Exception: {}", sqeInner);
                throw sqeInner;
            }
        }
    }

    private void addReplicationStatus(ReplicationStatus.Status status, String dbName, String tableName, long eventId)
        throws HiveReplicationException {
        try {
            String drJobName = conf.get("drJobName");
            ReplicationStatus rs = new ReplicationStatus(conf.get("sourceCluster"), conf.get("targetCluster"),
                    drJobName, dbName, tableName, status, eventId);
            listReplicationStatus.add(rs);
        } catch (HiveReplicationException hre) {
            throw new HiveReplicationException("Could not update replication status store for "
                    + " EventId:" + eventId
                    + " DB Name:" + dbName
                    + " Table Name:" + tableName
                    + hre.toString());
        }
    }

    public void invokeCopy(List<Path> srcStagingPaths) throws Exception {
        DistCpOptions options = getDistCpOptions(srcStagingPaths);
        DistCp distCp = new DistCp(conf, options);
        LOG.info("Started DistCp with source Path: {} \ttarget path: ", StringUtils.join(srcStagingPaths.toArray()),
                targetStagingUri);
        Job distcpJob = distCp.execute();
        LOG.info("Distp Hadoop job: {}", distcpJob.getJobID().toString());
        LOG.info("Completed DistCp");
    }

    public DistCpOptions getDistCpOptions(List<Path> srcStagingPaths) {
        srcStagingPaths.toArray(new Path[srcStagingPaths.size()]);

        DistCpOptions distcpOptions = new DistCpOptions(srcStagingPaths, new Path(targetStagingUri));
        /* setSyncFolder to false to retain dir structure as in source at the target. If set to true all files will be
        copied to the same staging sir at target resulting in DuplicateFileException in DistCp.
        */

        distcpOptions.setSyncFolder(false);
        distcpOptions.setBlocking(true);
        distcpOptions.setMaxMaps(Integer.valueOf(conf.get("distcpMaxMaps")));
        distcpOptions.setMapBandwidth(Integer.valueOf(conf.get("distcpMapBandwidth")));
        return distcpOptions;
    }

    public void cleanEventsDirectory() throws IOException {
        LOG.info("Cleaning staging directory");
        cleanupEventLocations(sourceCleanUpList, sourceFileSystem);
        cleanupEventLocations(targetCleanUpList, targetFileSystem);
    }

    private void cleanupEventLocations(List<Path> cleanupList, FileSystem fileSystem)
        throws IOException {
        for (Path cleanUpPath : cleanupList) {
            try {
                fileSystem.delete(cleanUpPath, true);
            } catch (IOException ioe) {
                LOG.error("Cleaning up of staging directory {} failed {}", cleanUpPath, ioe.toString());
                throw ioe;
            }
        }

    }

    public void closeConnection() throws SQLException {
        if (sourceStatement != null) {
            sourceStatement.close();
        }

        if (targetStatement != null) {
            targetStatement.close();
        }

        if (sourceConnection != null) {
            sourceConnection.close();
        }
        if (targetConnection != null) {
            targetConnection.close();
        }
    }
}
