package com.hortonworks.beacon.entity;

public class Acl {
    private String owner;
    private String group;
    private String permission;

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getPermission() {
        return permission;
    }

    public void setPermission(String permission) {
        this.permission = permission;
    }

    @Override
    public String toString() {
        return "Acl {" +
                "owner='" + owner + '\'' +
                ", group='" + group + '\'' +
                ", permission='" + permission + '\'' +
                '}';
    }
}
