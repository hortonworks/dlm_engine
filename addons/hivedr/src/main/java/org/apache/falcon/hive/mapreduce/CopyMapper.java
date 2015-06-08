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

package org.apache.falcon.hive.mapreduce;

import org.apache.falcon.hive.util.EventUtils;
import org.apache.falcon.hive.util.ReplicationStatus;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * Map class for Hive DR.
 */
public class CopyMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(CopyMapper.class);
    private EventUtils eventUtils;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        eventUtils = new EventUtils(context.getConfiguration());
        eventUtils.initializeFS();
        try {
            eventUtils.setupConnection();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {
        LOG.debug("Processing Event value: {}", value.toString());

        try {
            eventUtils.processEvents(value.toString());
        } catch (Exception e) {
            LOG.error("Exception in processing events:", e);
        } finally {
            cleanup(context);
        }
        List<ReplicationStatus> replicationStatusList = eventUtils.getListReplicationStatus();
        if (replicationStatusList != null && !replicationStatusList.isEmpty()) {
            for (ReplicationStatus rs : replicationStatusList) {
                context.write(new Text(rs.getJobName()), new Text(rs.toString()));
            }
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        LOG.info("Invoking cleanup process");
        super.cleanup(context);
        try {
            eventUtils.cleanEventsDirectory();
        } catch (IOException e) {
            LOG.error("Cleaning up of events directories failed", e);
        } finally {
            try {
                eventUtils.closeConnection();
            } catch (SQLException e) {
                LOG.error("Closing the connections failed", e);
            }
        }
    }
}
