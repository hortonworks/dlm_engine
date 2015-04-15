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

package org.apache.falcon.hive;


import com.google.common.collect.Lists;
import org.apache.falcon.hive.exception.HiveReplicationException;
import org.apache.falcon.hive.util.DRStatusStore;
import org.apache.falcon.hive.util.HiveDRUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.api.repl.Command;
import org.apache.hive.hcatalog.api.repl.ReplicationTask;
import org.apache.hive.hcatalog.api.repl.ReplicationUtils;
import org.apache.hive.hcatalog.api.repl.StagingDirectoryProvider;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hive.hcatalog.common.HCatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * Sources meta store change events from Hive.
 */
public class MetaStoreEventSourcer implements EventSourcer {

    private static final Logger LOG = LoggerFactory.getLogger(MetaStoreEventSourcer.class);
    private final HCatClient sourceMetastoreClient;
    private final HCatClient targetMetastoreClient;
    private final Partitioner partitioner;
    private final DRStatusStore drStore;

    /* TODO handle cases when no events. files will be empty and lists will be empty */

    public MetaStoreEventSourcer(String sourceMetastoreUri, String targetMetastoreUri,
                                 Partitioner defaultPartitioner, DRStatusStore drStore) throws Exception {
        sourceMetastoreClient = initializeHiveMetaStoreClient(sourceMetastoreUri);
        targetMetastoreClient = initializeHiveMetaStoreClient(targetMetastoreUri);
        partitioner = defaultPartitioner;
        this.drStore = drStore;
    }

    public HCatClient initializeHiveMetaStoreClient(String metastoreUri) throws Exception {
        try {
            HiveConf hcatConf = createHiveConf(new Configuration(false), metastoreUri);
            return HCatClient.create(hcatConf);
        } catch (HCatException e) {
            throw new Exception("Exception creating HCatClient: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new Exception("Exception creating HCatClient: " + e.getMessage(), e);
        }
    }

    private static HiveConf createHiveConf(Configuration conf,
                                           String metastoreUrl) throws IOException {
        HiveConf hcatConf = new HiveConf(conf, HiveConf.class);

        hcatConf.set("hive.metastore.local", "false");
        hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUrl);
        hcatConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        hcatConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
                HCatSemanticAnalyzer.class.getName());
        hcatConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

        hcatConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hcatConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        return hcatConf;
    }

    @Override
    public ListIterator<ReplicationEvents> sourceEvents(HiveDROptions inputOptions) throws Exception {
        LOG.info("Enter sourceEvents");
        List<ReplicationEvents> replicationEvents = Lists.newArrayList();

        HiveDRUtils.ReplicationType replicationType = HiveDRUtils.getReplicationType(inputOptions.getSourceTables());
        LOG.info("replicationType : {}", replicationType);
        if (replicationType == HiveDRUtils.ReplicationType.DB) {
            List<String> dbNames = inputOptions.getSourceDatabases();
            for(String db : dbNames) {
                List<ReplicationEvents> events = sourceEventsForDb(inputOptions, db);
                if (events != null && !events.isEmpty()) {
                    replicationEvents.addAll(events);
                }
            }
        } else {
            List<String> tableNames = inputOptions.getSourceTables();
            String db = inputOptions.getSourceDatabases().get(0);
            for(String tableName : tableNames) {
                List<ReplicationEvents> events = sourceEventsForTable(inputOptions, db, tableName);
                if (events != null && !events.isEmpty()) {
                    replicationEvents.addAll(events);
                }
            }
        }

        if (replicationEvents.isEmpty()) {
            LOG.info("No events for tables for the request db: {} , Tables : {}", inputOptions.getSourceDatabases(),
                    inputOptions.getSourceTables());
        }
        return replicationEvents.listIterator();
    }

    private List<ReplicationEvents> sourceEventsForDb(HiveDROptions inputOptions, String dbName) throws Exception {
        HiveDRUtils.ReplicationType type = HiveDRUtils.getReplicationType(inputOptions.getSourceTables());
        String jobName = inputOptions.getJobName();
        String sourceMetastoreUri = inputOptions.getSourceMetastoreUri();
        String targetMetastoreUri = inputOptions.getTargetMetastoreUri();
        Iterator<ReplicationTask> replicationTaskIter = sourceReplicationEvents(getLastSavedEventId(type,
                sourceMetastoreUri, targetMetastoreUri, jobName, dbName, null),
                inputOptions.getMaxEvents(), dbName, null);
        if (replicationTaskIter == null || !replicationTaskIter.hasNext()) {
            LOG.info("No events for db: {}", dbName);
            return null;
        }
        return processEvents(dbName, null, inputOptions, replicationTaskIter);
    }

    private List<ReplicationEvents> sourceEventsForTable(HiveDROptions inputOptions, String dbName, String tableName)
        throws Exception {
        HiveDRUtils.ReplicationType type = HiveDRUtils.getReplicationType(inputOptions.getSourceTables());
        String jobName = inputOptions.getJobName();
        String sourceMetastoreUri = inputOptions.getSourceMetastoreUri();
        String targetMetastoreUri = inputOptions.getTargetMetastoreUri();
        Iterator<ReplicationTask> replicationTaskIter = sourceReplicationEvents(getLastSavedEventId(type,
                sourceMetastoreUri, targetMetastoreUri, jobName, dbName, tableName),
                inputOptions.getMaxEvents(), dbName, tableName);
        if (replicationTaskIter == null || !replicationTaskIter.hasNext()) {
            LOG.info("No events for db.table: {}.{}", dbName, tableName);
            return null;
        }
        return processEvents(dbName, tableName, inputOptions, replicationTaskIter);
    }

    private List<ReplicationEvents> processEvents(String dbName, String tableName, HiveDROptions inputOptions,
                               Iterator<ReplicationTask> replicationTaskIter) throws Exception {
        LOG.info("In processEvents");
        List<ReplicationEvents> replicationEvents;

        if (partitioner.isPartitioningRequired(inputOptions)) {
            replicationEvents = partitioner.partition(inputOptions, dbName, replicationTaskIter);

            if (replicationEvents.isEmpty()) {
                LOG.info("Nothing to replicate");
            }
        } else {
            replicationEvents = processTableReplicationEvents(replicationTaskIter, dbName, tableName,
                    inputOptions.getSourceStagingPath(), inputOptions.getTargetStagingPath());
        }

        return replicationEvents;
    }

    private long getLastSavedEventId(final HiveDRUtils.ReplicationType replicationType,
                                     final String sourceMetastoreUri, final String targetMetastoreUri,
                                     final String jobName, final String dbName,
                                     final String tableName) throws Exception {
        long eventId = 0;
        if (HiveDRUtils.ReplicationType.DB == replicationType) {
            eventId = drStore.getReplicationStatus(sourceMetastoreUri, targetMetastoreUri,
                    jobName, dbName).getEventId();
        } else if (HiveDRUtils.ReplicationType.TABLE == replicationType) {
            eventId = drStore.getReplicationStatus(sourceMetastoreUri, targetMetastoreUri,
                    jobName, dbName, tableName).getEventId();
        }

        if (eventId == -1) {
            if (HiveDRUtils.ReplicationType.DB == replicationType) {
                /*
                 * API to get last repl ID for a DB is very expensive, so Hive does not want to make it public.
                 * HiveDrTool finds last repl id for DB by finding min last repl id of all tables.
                 */
                // eventId = ReplicationUtils.getLastReplicationId(database);

                eventId  = getLastReplicationIdForDatabase(dbName);
            } else {
                HCatTable table = targetMetastoreClient.getTable(dbName, tableName);
                eventId = ReplicationUtils.getLastReplicationId(table);
            }
        }
        LOG.info("getLastSavedEventId eventId : {}", eventId);
        return eventId;
    }

    private long getLastReplicationIdForDatabase(String databaseName) throws HiveReplicationException {
        /*
         * This is a very expensive method and should only be called during first dbReplication instance.
         */
        long eventId = Long.MAX_VALUE;
        try {
            List<String> tableList = targetMetastoreClient.listTableNamesByPattern(databaseName, "*");
            for (String tableName : tableList) {
                long temp = ReplicationUtils.getLastReplicationId(
                        targetMetastoreClient.getTable(databaseName, tableName));
                if (temp < eventId) {
                    eventId = temp;
                }
            }
            return (eventId == Long.MAX_VALUE) ?  0 : eventId;
        } catch (HCatException e) {
            throw new HiveReplicationException("Unable to find last replication id for database "
                + databaseName, e);
        }
    }

    private Iterator<ReplicationTask> sourceReplicationEvents(long lastEventId, int maxEvents, String dbName,
                                                              String tableName) throws Exception {
        try {
            return sourceMetastoreClient.getReplicationTasks(lastEventId, maxEvents, dbName, tableName);
        } catch (HCatException e) {
            throw new Exception("Exception getting replication events " + e.getMessage(), e);
        }
    }


    private List<ReplicationEvents> processTableReplicationEvents(Iterator<ReplicationTask> taskIter, String dbName,
                                                                  String tableName, String srcStagingDirProvider,
                                                                  String dstStagingDirProvider) throws Exception {
        List<Command> srcReplicationEventList = Lists.newArrayList();
        List<Command> trgReplicationEventList = Lists.newArrayList();

        while (taskIter.hasNext()) {
            ReplicationTask task = taskIter.next();
            if (task.needsStagingDirs()) {
                task.withSrcStagingDirProvider(new StagingDirectoryProvider.TrivialImpl(srcStagingDirProvider,
                        HiveDRUtils.SEPARATOR));
                task.withDstStagingDirProvider(new StagingDirectoryProvider.TrivialImpl(dstStagingDirProvider,
                        HiveDRUtils.SEPARATOR));
            }

            if (task.isActionable()) {
                Iterable<? extends org.apache.hive.hcatalog.api.repl.Command> srcCmds = task.getSrcWhCommands();
                for(Command cmd : srcCmds) {
                    srcReplicationEventList.add(cmd);
                }

                Iterable<? extends org.apache.hive.hcatalog.api.repl.Command> dstCmds = task.getDstWhCommands();
                for(Command cmd : dstCmds) {
                    trgReplicationEventList.add(cmd);
                }
            } else {
                LOG.error("Task is not actionable with event Id : {}", task.getEvent().getEventId());
            }
        }

        List<ReplicationEvents> replicationEvents = Lists.newArrayList();
        ReplicationEvents events = null;

        if (!srcReplicationEventList.isEmpty() || !trgReplicationEventList.isEmpty()) {
            LOG.info("processTableReplicationEvents add src and dst events");
            events = new ReplicationEvents(dbName.toLowerCase(), tableName.toLowerCase(), srcReplicationEventList,
                    trgReplicationEventList);
        }
        if (events != null) {
            replicationEvents.add(events);
        }

        return replicationEvents;
    }

    public void cleanUp() throws Exception {
        if (sourceMetastoreClient != null) {
            sourceMetastoreClient.close();
        }

        if (targetMetastoreClient != null) {
            targetMetastoreClient.close();
        }
    }
}
