/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.beacon.api.plugin;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Used by the plugin to report status.  This should be updated frequently to give end users an
 * idea of how the work is going.
 *
 * Since a given job may consist of many tasks, this has the notion of a subsidiary reporter.
 * This can be passed to a task or sub-task, along with an estimate of its weight for the total
 * task.  This way when a subtask that represents 10% of the overall job says it is 50% done the
 * controlling task knows to only add 5% to the pctDone figure that is reported.  Subsidiary
 * reporters can of course themselves have subsidiary reporters.  It is up to the creator of a
 * reporter to assure that his own use of pctDone plus the weights of his subsidiaries adds up to
 * 1.0.
 */
public class StatusReporter {
    private final static int NUM_STATUS_MSGS_KEPT = 10;

    private final float weight;
    private float pctDone;
    private List<StatusReporter> subsidiaries;
    private Deque<String> statusMsgs;

    /**
     * Only Beacon should be creating new StatusReporters.  If you're calling this inside a
     * plugin something is wrong.  You should instead be calling
     * {@link #getSubsidiaryReporter(float)} and passing the result to any tasks you have.
     */
    public StatusReporter() {
        this(0);
    }

    // Used to create subsidiary reporters.
    private StatusReporter(float weight) {
        this.weight = weight;
        pctDone = 0;
    }

    /**
     * Get a subsidiary status reporter.
     * @param weight estimated percentage of your overall task that is represented by this task.
     *               It is your problem to assure that your pctDone plus the sum of the weights
     *               you pass to subsidiaries sums to 100%.
     * @return a subsidiary reporter
     */
    public StatusReporter getSubsidiaryReporter(float weight) {
        if (subsidiaries == null) subsidiaries = new ArrayList<>();
        StatusReporter subsidiary = new StatusReporter(weight);
        subsidiaries.add(subsidiary);
        return subsidiary;
    }

    /**
     * Get the percentage this task is done.  This includes the task itself plus any subsidiary
     * tasks it has spawned.
     * @return percentage completed, with 1.0 being 100%.
     */
    public float getPercentDone() {
        float total = 0;
        if (subsidiaries != null) {
            for (StatusReporter subsidiary : subsidiaries) {
                total += subsidiary.getPercentDone() * subsidiary.weight;
            }
        }
        return pctDone + total;
    }

    /**
     * Add to the tally of the percentage of work done.  This will be added to the existing
     * percent work done.  Note that this is only for work specific to this task.  Any percentage
     * done accounted for by subtasks will be added in from there.  That is, once you finish a
     * subtask you should not call this.
     * @param amountDone incremental additional percentage of work completed, with 1.0 beings 100%.
     */
    public void addToPercentDone(float amountDone) {
        pctDone += amountDone;
    }

    /**
     * Add a new status message.  The number of status messages kept are bounded, so if you do
     * not read the status messages for a long time you may miss some.
     * @param status message to send to end user
     */
    public void addStatusMessage(String status) {
        if (statusMsgs == null) statusMsgs = new ArrayDeque<>(NUM_STATUS_MSGS_KEPT);
        if (statusMsgs.size() >= NUM_STATUS_MSGS_KEPT) statusMsgs.removeLast();
        statusMsgs.addFirst(status);
    }

    /**
     * Read all status messages.  This is a destructive read.  All status messages will be erased
     * after this is read.
     * @return all status messages
     */
    public List<String> getStatusMessages() {
        List<String> msgs = new ArrayList<>();
        getStatusMessages(msgs, true);
        return msgs;
    }

    /**
     * Read all of the status messages without clearing them.
     * @return all status messages
     */
    public List<String> peekAtStatusMessages() {
        List<String> msgs = new ArrayList<>();
        getStatusMessages(msgs, false);
        return msgs;
    }

    private void getStatusMessages(List<String> msgs, boolean destructive) {
        if (statusMsgs != null && statusMsgs.size() > 0) {
            msgs.addAll(statusMsgs);
            if (destructive) statusMsgs.clear();
        }
        if (subsidiaries != null) {
            for (StatusReporter subsidiary : subsidiaries) {
                subsidiary.getStatusMessages(msgs, destructive);
            }
        }
    }

}
