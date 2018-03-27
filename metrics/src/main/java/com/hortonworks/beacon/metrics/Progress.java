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

package com.hortonworks.beacon.metrics;

/**
 * Replication Progress.
 */
public class Progress {
    private long total;
    private long completed;
    private long failed;
    private long killed;
    private long filesCopied;
    private long dirCopied;
    private long bytesCopied;
    private long timeTaken;
    private String unit;
    private long jobProgress;

    public Progress() {
    }

    Progress(long total, long completed, long jobProgress, String unit) {
        this.total = total;
        this.completed = completed;
        this.jobProgress = jobProgress;
        this.unit = unit;
    }

    long getTotal() {
        return total;
    }

    long getCompleted() {
        return completed;
    }

    public long getFailed() {
        return failed;
    }

    public long getKilled() {
        return killed;
    }

    public String getUnit() {
        return unit;
    }

    long getFilesCopied() {
        return filesCopied;
    }

    long getBytesCopied() {
        return bytesCopied;
    }

    long getTimeTaken() {
        return timeTaken;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    private long getValue(Long value) {
        return value != null ? value : 0;
    }

    public void setCompleted(Long completed) {
        this.completed = getValue(completed);
    }

    public void setFailed(Long failed) {
        this.failed = getValue(failed);
    }

    public void setKilled(Long killed) {
        this.killed = getValue(killed);
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public void setFilesCopied(Long filesCopied) {
        this.filesCopied = getValue(filesCopied);
    }

    public void setBytesCopied(Long bytesCopied) {
        this.bytesCopied = getValue(bytesCopied);
    }

    public void setTimeTaken(Long timeTaken) {
        this.timeTaken = getValue(timeTaken);
    }

    public void setDirectoriesCopied(Long directoriesCopied) {
        this.dirCopied = getValue(directoriesCopied);
    }

    public long getDirectoriesCopied() {
        return dirCopied;
    }

    public long getJobProgress() {
        return jobProgress;
    }

    public void setJobProgress(long jobProgress) {
        this.jobProgress = jobProgress;
    }
}
