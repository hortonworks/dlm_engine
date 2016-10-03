package com.hortonworks.beacon.entity;

/**
 * Created by sramesh on 9/30/16.
 */
public class Retry {
    private int attempts;
    private int delay; // What abt policy, timeout?

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public int getDelay() {
        return delay;
    }

    public void setDelay(int delay) {
        this.delay = delay;
    }
}
