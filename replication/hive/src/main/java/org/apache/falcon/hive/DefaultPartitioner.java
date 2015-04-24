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
import org.apache.commons.lang.StringUtils;
import org.apache.falcon.hive.util.DRStatusStore;
import org.apache.falcon.hive.util.HiveDRUtils;
import org.apache.falcon.hive.util.ReplicationStatus;
import org.apache.hive.hcatalog.api.repl.Command;
import org.apache.hive.hcatalog.api.repl.ReplicationTask;
import org.apache.hive.hcatalog.api.repl.StagingDirectoryProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hive.hcatalog.api.HCatNotificationEvent.Scope;

/**
 *  Partitioner for partitioning events for a given DB.
 */
public class DefaultPartitioner implements Partitioner {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultPartitioner.class);
    private EventFilter eventFilter;
    private DRStatusStore drStore;

    public DefaultPartitioner(DRStatusStore drStore) {
        this.drStore = drStore;
    }

    private class EventFilter {
        private final Map<String, Long> eventFilterMap;

        public EventFilter(String sourceMetastoreUri, String targetMetastoreUri, String jobName,
                           String database) throws Exception {
            eventFilterMap = new HashMap<String, Long>();
            Iterator<ReplicationStatus> replStatusIter = drStore.getTableReplicationStatusesInDb(sourceMetastoreUri,
                    targetMetastoreUri, jobName, database);
            while(replStatusIter.hasNext()) {
                ReplicationStatus replStatus = replStatusIter.next();
                eventFilterMap.put(replStatus.getTable(), replStatus.getEventId());
            }
        }
    }

    public List<ReplicationEvents> partition(final HiveDROptions drOptions, final String databaseName,
                                             final Iterator<ReplicationTask> taskIter) throws Exception {
        String dbName = databaseName.toLowerCase();
        // init filtering before partitioning
        this.eventFilter = new EventFilter(drOptions.getSourceMetastoreUri(), drOptions.getTargetMetastoreUri(),
                drOptions.getJobName(), dbName);
        String srcStagingDirProvider = drOptions.getSourceStagingPath();
        String dstStagingDirProvider = drOptions.getTargetStagingPath();

        List<Command> dbSrcEventList = Lists.newArrayList();
        List<Command> dbTrgEventList = Lists.newArrayList();

        Map<String, List<Command>> srcReplicationEventMap =
                new HashMap<String, List<Command>>();
        Map<String, List<Command>> trgReplicationEventMap =
                new HashMap<String, List<Command>>();

        while (taskIter.hasNext()) {
            ReplicationTask task = taskIter.next();
            if (task.needsStagingDirs()) {
                task.withSrcStagingDirProvider(new StagingDirectoryProvider.TrivialImpl(srcStagingDirProvider,
                        HiveDRUtils.SEPARATOR));
                task.withDstStagingDirProvider(new StagingDirectoryProvider.TrivialImpl(dstStagingDirProvider,
                        HiveDRUtils.SEPARATOR));
            }

            if (task.isActionable()) {
                Scope eventScope = task.getEvent().getEventScope();
                String tableName = task.getEvent().getTableName();
                if (StringUtils.isNotEmpty(tableName)) {
                    tableName = tableName.toLowerCase();
                }

                Iterable<? extends org.apache.hive.hcatalog.api.repl.Command> srcCmds = task.getSrcWhCommands();
                for(Command cmd : srcCmds) {
                    addEvent(eventScope, tableName, cmd, dbSrcEventList, srcReplicationEventMap);
                }

                Iterable<? extends org.apache.hive.hcatalog.api.repl.Command> dstCmds = task.getDstWhCommands();
                for(Command cmd : dstCmds) {
                    addEvent(eventScope, tableName, cmd, dbTrgEventList, trgReplicationEventMap);
                }
            } else {
                LOG.error("Task is not actionable with event Id : {}", task.getEvent().getEventId());
            }
        }

        List<ReplicationEvents> replicationEvents = Lists.newArrayList();

        /* Loop through src events as there can't be import if there is no export */
        for (Map.Entry<String, List<Command>> entry : srcReplicationEventMap.entrySet()) {
            String tableName = entry.getKey();
            List<Command> srcReplicationEventList = entry.getValue();
            List<Command> trgReplicationEventList = trgReplicationEventMap.get(tableName);
            ReplicationEvents events = null;
            if (!srcReplicationEventList.isEmpty() || !trgReplicationEventList.isEmpty()) {
                events = new ReplicationEvents(dbName, tableName, srcReplicationEventList,
                        trgReplicationEventList);
            }
            if (events != null) {
                replicationEvents.add(events);
            }
        }

        // Only DB events
        if (replicationEvents.isEmpty()) {
            ReplicationEvents events = null;
            if (!dbSrcEventList.isEmpty() || !dbTrgEventList.isEmpty()) {
                events = new ReplicationEvents(dbName, null, dbSrcEventList, dbTrgEventList);
            }
            if (events != null) {
                replicationEvents.add(events);
            }
        }

        return replicationEvents;
    }

    private void addEvent(final Scope eventScope, final String tableName, final Command cmd,
                          final List<Command> dbEventList,
                          final Map<String, List<Command>> replicationEventMap) throws Exception {
        /* TODO : How to handle only DB events */
        if (eventScope == Scope.DB) {
            dbEventList.add(cmd);
            /* add DB event to all tables */
            if (!replicationEventMap.isEmpty()) {
                addDbEventToAllTablesEventList(cmd, replicationEventMap);
            }
        } else if (eventScope == Scope.TABLE) {
            List<Command> tableEventList = replicationEventMap.get(tableName);
            if (tableEventList == null) {
                tableEventList = Lists.newArrayList();
                replicationEventMap.put(tableName, tableEventList);
                // Before adding this event, add all the DB events
                addDbEventsToTableEventList(tableName, dbEventList, tableEventList);
            }
            addTableEvent(tableName, cmd, tableEventList);
        } else {
            throw new Exception("Event scope is not DB or Table");
        }
    }

    private void addDbEventToAllTablesEventList(final Command cmd,
                                                final Map<String, List<Command>> tableEventMap) {
        for (Map.Entry<String, List<Command>> entry : tableEventMap.entrySet()) {
            String tableName = entry.getKey();
            List<Command> tableEventList = entry.getValue();
            addTableEvent(tableName, cmd, tableEventList);
        }
    }

    private void addDbEventsToTableEventList(final String tableName, final List<Command> dbEventList,
                                             final List<Command> tableEventList) {
        for (Command cmd : dbEventList) {
            addTableEvent(tableName, cmd, tableEventList);
        }
    }

    private void addTableEvent(final String tableName, final Command cmd, final List<Command> tableEventList) {
        Long eventId = eventFilter.eventFilterMap.get(tableName);
        /* If not already processed, add it */
        if (eventId == null || cmd.getEventId() > eventId) {
            tableEventList.add(cmd);
        }
    }

    public boolean isPartitioningRequired(final HiveDROptions options) {
        return (HiveDRUtils.getReplicationType(options.getSourceTables()) == HiveDRUtils.ReplicationType.DB);
    }
}
