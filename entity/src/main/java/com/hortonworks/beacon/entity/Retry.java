package com.hortonworks.beacon.entity;

/**
 * Created by sramesh on 9/30/16.
 */
public class Retry {
    private int attempts;
    private String delay; // What abt policy, timeout?

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public String getDelay() {
        return delay;
    }

    public void setDelay(String delay) {
        this.delay = delay;
    }

    @Override
    public String toString() {
        return "Retry{" +
                "attempts=" + attempts +
                ", delay='" + delay + '\'' +
                '}';
    }
}
