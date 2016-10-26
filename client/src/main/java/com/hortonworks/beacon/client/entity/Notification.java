package com.hortonworks.beacon.client.entity;

public class Notification {
    private String type;
    private String to;

    public Notification() {
    }

    public Notification(String type, String to) {
        this.type = type;
        this.to = to;
    }

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

    @Override
    public String toString() {
        return "Notification{" +
                "type='" + type + '\'' +
                ", to='" + to + '\'' +
                '}';
    }
}
