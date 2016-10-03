package com.hortonworks.beacon.entity;

/**
 * Created by sramesh on 9/30/16.
 */
public class Notification {
    private String type;
    private String to;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }
}
