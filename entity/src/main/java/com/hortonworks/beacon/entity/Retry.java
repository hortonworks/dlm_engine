package com.hortonworks.beacon.entity;

public class Retry {
    private int attempts;
    private long delay;
    public static final int RETRY_ATTEMPTS = 3;
    // 30 minutes in sec
    public static final long RETRY_DELAY = 1800;

    public Retry() {
    }

    public Retry(int attempts, long delay) {
        this.attempts = attempts;
        this.delay = delay;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public long getDelay() {
        return delay;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    @Override
    public String toString() {
        return "Retry{" +
                "attempts=" + attempts +
                ", delay=" + delay +
                '}';
    }

}
