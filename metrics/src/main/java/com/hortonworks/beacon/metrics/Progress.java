/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
    private long bytesCopied;
    private long timeTaken;
    private String unit;

    public Progress() {
    }

    Progress(long total, long completed, String unit) {
        this.total = total;
        this.completed = completed;
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

    public void setCompleted(long completed) {
        this.completed = completed;
    }

    public void setFailed(long failed) {
        this.failed = failed;
    }

    public void setKilled(long killed) {
        this.killed = killed;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public void setFilesCopied(long filesCopied) {
        this.filesCopied = filesCopied;
    }

    public void setBytesCopied(long bytesCopied) {
        this.bytesCopied = bytesCopied;
    }

    public void setTimeTaken(long timeTaken) {
        this.timeTaken = timeTaken;
    }
}
