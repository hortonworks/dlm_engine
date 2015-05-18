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

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.falcon.hive.mapreduce.CopyMapper;
import org.apache.falcon.hive.mapreduce.CopyReducer;
import org.apache.falcon.hive.util.DRStatusStore;
import org.apache.falcon.hive.util.DelimiterUtils;
import org.apache.falcon.hive.util.FileUtils;
import org.apache.falcon.hive.util.HiveDRStatusStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.api.repl.Command;
import org.apache.hive.hcatalog.api.repl.ReplicationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ListIterator;

/**
 * DR Tool Driver.
 */
public class HiveDRTool extends Configured implements Tool {
    private FileSystem jobFS;
    private FileSystem targetClusterFs;

    private HiveDROptions inputOptions;
    private DRStatusStore drStore;
    private String eventsInputFile;

    private static final String DEFAULT_EVENT_STORE_PATH = DRStatusStore.BASE_DEFAULT_STORE_PATH
            + File.separator + "Events";
    public static final FsPermission STAGING_DIR_PERMISSION =
            new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);

    private static final FsPermission FS_PERMISSION =
            new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);
    private static final Logger LOG = LoggerFactory.getLogger(HiveDRTool.class);

    public HiveDRTool() {
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            usage();
            return -1;
        }

        try {
            init(args);
        } catch (Throwable e) {
            LOG.error("Invalid arguments: ", e);
            System.err.println("Invalid arguments: " + e.getMessage());
            usage();
            return -1;
        }

        try {
            execute();
        } catch (Exception e) {
            System.err.println("Exception encountered " + e.getMessage());
            e.printStackTrace();
            LOG.error("Exception encountered ", e);
            return -1;
        } finally {
            cleanup();
        }

        return 0;
    }

    private void init(String[] args) throws Exception {
        LOG.info("Enter init");
        inputOptions = parseOptions(args);
        LOG.info("Input Options: {}", inputOptions);

        targetClusterFs = FileSystem.get(FileUtils.getConfiguration(inputOptions.getTargetWriteEP()));
        jobFS = FileSystem.get(FileUtils.getConfiguration(inputOptions.getJobClusterWriteEP()));

        // init DR status store
        drStore = new HiveDRStatusStore(targetClusterFs);

        // Create base dir to store events on cluster where job is running
        Path dir = new Path(DEFAULT_EVENT_STORE_PATH);
        // Validate base path
        FileUtils.validatePath(jobFS, new Path(DRStatusStore.BASE_DEFAULT_STORE_PATH));

        if (!jobFS.exists(dir)) {
            if (!jobFS.mkdirs(dir)) {
                throw new Exception("Creating directory failed: " + dir);
            }
        }
        LOG.info("Exit init");
    }

    private HiveDROptions parseOptions(String[] args) throws ParseException {
        return HiveDROptions.create(args);
    }

    public Job execute() throws Exception {
        assert inputOptions != null;
        assert getConf() != null;

        String jobIdentifier = getJobIdentifier(inputOptions.getJobName());
        setStagingDirectory(jobIdentifier);
        ListIterator<ReplicationEvents> events = sourceEvents();
        if (events == null || !events.hasNext()) {
            LOG.info("No events to process");
            return null;
        }

        Job job;
        synchronized (this) {
            eventsInputFile = persistReplicationEvents(DEFAULT_EVENT_STORE_PATH, jobIdentifier, events);
            job = createJob();
        }

        createStagingDirectory();
        job.submit();

        String jobID = job.getJobID().toString();
        job.getConfiguration().set("HIVEDR_JOB_ID", jobID);

        LOG.info("HiveDR job-id: {}", jobID);
        if (inputOptions.shouldBlock() && !job.waitForCompletion(true)) {
            throw new IOException("HiveDR failure: Job " + jobID + " has failed: "
                    + job.getStatus().getFailureInfo());
        }

        return job;
    }

    private Job createJob() throws Exception {
        String jobName = "hive-dr";
        String userChosenName = getConf().get(JobContext.JOB_NAME);
        if (userChosenName != null) {
            jobName += ": " + userChosenName;
        }
        Job job = Job.getInstance(getConf());
        job.setJobName(jobName);

        job.setJarByClass(CopyMapper.class);
        job.setMapperClass(CopyMapper.class);
        job.setReducerClass(CopyReducer.class);

        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.NLineInputFormat.class);

        job.setOutputFormatClass(NullOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.getConfiguration().set(JobContext.MAP_SPECULATIVE, "false");
        job.getConfiguration().set(JobContext.NUM_MAPS,
                String.valueOf(inputOptions.getReplicationMaxMaps()));

        for (HiveDRArgs args : HiveDRArgs.values()) {
            if (inputOptions.getValue(args) != null) {
                job.getConfiguration().set(args.getName(), inputOptions.getValue(args));
            } else {
                job.getConfiguration().set(args.getName(), "null");
            }
        }

        job.getConfiguration().set(FileInputFormat.INPUT_DIR, this.eventsInputFile);

        return job;
    }

    private void setStagingDirectory(String jobIdentifier) throws Exception {
        String sourceStagingPath = inputOptions.getSourceStagingPath();
        String targetStagingPath = inputOptions.getTargetStagingPath();
        if (StringUtils.isNotEmpty(sourceStagingPath) && StringUtils.isNotEmpty(targetStagingPath)) {
            sourceStagingPath += File.separator + jobIdentifier;
            targetStagingPath += File.separator + jobIdentifier;
            inputOptions.setSourceStagingDir(sourceStagingPath);
            inputOptions.setTargetStagingDir(targetStagingPath);
        } else {
            throw new Exception("Staging paths cannot be null");
        }
    }

    private void createStagingDirectory() throws IOException {
        Path sourceStagingPath = new Path(inputOptions.getSourceStagingPath());
        Path targetStagingPath = new Path(inputOptions.getTargetStagingPath());
        LOG.info("Source staging path: {}", sourceStagingPath);
        if (!FileSystem.mkdirs(jobFS, sourceStagingPath, STAGING_DIR_PERMISSION)) {
            throw new IOException("mkdir failed for " + sourceStagingPath);
        }

        LOG.info("Target staging path: {}", targetStagingPath);
        if (!FileSystem.mkdirs(targetClusterFs, targetStagingPath, STAGING_DIR_PERMISSION)) {
            throw new IOException("mkdir failed for " + targetStagingPath);
        }
    }

    private void cleanStagingDirectory() {
        LOG.info("Cleaning staging directories");
        Path sourceStagingPath = new Path(inputOptions.getSourceStagingPath());
        Path targetStagingPath = new Path(inputOptions.getTargetStagingPath());
        try {
            if (jobFS.exists(sourceStagingPath)) {
                jobFS.delete(sourceStagingPath, false);
            }

            if (targetClusterFs.exists(targetStagingPath)) {
                targetClusterFs.delete(targetStagingPath, false);
            }
        } catch (IOException e) {
            LOG.error("Unable to cleanup staging dir:", e);
        }
    }

    private ListIterator<ReplicationEvents> sourceEvents() throws Exception {
        MetaStoreEventSourcer defaultSourcer = null;
        ListIterator<ReplicationEvents> replicationEventsIter = null;
        try {
            defaultSourcer = new MetaStoreEventSourcer(inputOptions.getSourceMetastoreUri(),
                    inputOptions.getTargetMetastoreUri(), new DefaultPartitioner(drStore), drStore);
            replicationEventsIter = defaultSourcer.sourceEvents(inputOptions);
        } finally {
            if (defaultSourcer != null) {
                defaultSourcer.cleanUp();
            }
        }
        LOG.info("Return sourceEvents");
        return replicationEventsIter;
    }

    public static void main(String[] args) {
        int exitCode;
        try {
            HiveDRTool hiveDRTool = new HiveDRTool();
            exitCode = ToolRunner.run(getDefaultConf(), hiveDRTool, args);
        } catch (Exception e) {
            LOG.error("Couldn't complete HiveDR operation: ", e);
            exitCode = -1;
        }

        System.exit(exitCode);
    }

    private static Configuration getDefaultConf() {
        Configuration conf = new Configuration();
        conf.addResource(new Path("file:///", System.getProperty("oozie.action.conf.xml")));
        return conf;
    }

    private void cleanInputFile() {
        if (!inputOptions.shouldKeepHistory()) {
            try {
                if (StringUtils.isEmpty(eventsInputFile)) {
                    return;
                }
                jobFS.delete(new Path(eventsInputFile), false);
                eventsInputFile = null;
            } catch (IOException e) {
                LOG.error("Unable to cleanup: {}", eventsInputFile, e);
            }
        }
    }

    private synchronized void cleanup() {
        cleanStagingDirectory();
        cleanInputFile();
        try {
            if (jobFS != null) {
                jobFS.close();
            }
            if (targetClusterFs != null) {
                targetClusterFs.close();
            }
        } catch (IOException e) {
            LOG.error("Closing FS failed", e);
        }
    }

    private String persistReplicationEvents(String dir, String filename,
                                            ListIterator<ReplicationEvents> eventsList) throws Exception {
        OutputStream out = null;
        Path filePath = new Path(getFilename(dir, filename));

        try {
            out = FileSystem.create(jobFS, filePath, FS_PERMISSION);
            while (eventsList.hasNext()) {
                ReplicationEvents events = eventsList.next();
                String dbName = events.getDbName();
                String tableName = events.getTableName();
                ListIterator<Command> exportCmds = events.getExportCommands();
                ListIterator<Command> importCmds = events.getImportCommands();

                if (dbName != null) {
                    out.write(dbName.getBytes());
                }
                out.write(DelimiterUtils.FIELD_DELIM.getBytes());
                if (tableName != null) {
                    out.write(tableName.getBytes());
                }
                out.write(DelimiterUtils.FIELD_DELIM.getBytes());
                writeCommandsToFile(exportCmds, out);
                out.write(DelimiterUtils.FIELD_DELIM.getBytes());
                writeCommandsToFile(importCmds, out);

                if (eventsList.hasNext()) {
                    out.write(DelimiterUtils.NEWLINE_DELIM.getBytes());
                }
            }
            out.flush();
        } finally {
            IOUtils.closeQuietly(out);
        }
        return jobFS.getFileStatus(filePath).getPath().toString();
    }

    private static void writeCommandsToFile(ListIterator<Command> cmds, OutputStream out) throws IOException {
        while (cmds.hasNext()) {
            String cmd = ReplicationUtils.serializeCommand(cmds.next());
            out.write(cmd.getBytes());
            LOG.debug("HiveDR Serialized Repl Command : {}", cmd);
            if (cmds.hasNext()) {
                out.write(DelimiterUtils.STMT_DELIM.getBytes());
            }
        }
    }

    private static String getFilename(String dir, String filename) throws Exception {
        return dir + File.separator + filename + ".txt";
    }

    private static String getJobIdentifier(String identifier) {
        return identifier + "_" + System.currentTimeMillis();
    }

/*
    private boolean isSubmitted() {
        return submitted;
    }
*/

    public static void usage() {
        System.out.println("Usage: hivedrtool -option value ....");
    }
}
