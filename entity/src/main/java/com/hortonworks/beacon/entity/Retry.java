package com.hortonworks.beacon.entity;

public class Retry {
    private int attempts;
    private long delay;

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
