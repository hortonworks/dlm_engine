package com.hortonworks.beacon.replication.test;

public enum TestDRProperties {

    JOB_NAME("jobName", "unique job name", true),
    JOB_FREQUENCY("jobFrequency","job frequency schedule", true),
    JOB_TYPE("type", "type of job"),
    START_TIME("startTime", "job start time", false),
    END_TIME("endTime", "job end time", false),
    SLEEP_TIME("sleepTime", "Sleep time property", false);

    private final String name;
    private final String description;
    private final boolean isRequired;

    TestDRProperties(String name, String description) {
        this(name, description, true);
    }

    TestDRProperties(String name, String description, boolean isRequired) {
        this.name = name;
        this.description = description;
        this.isRequired = isRequired;
    }

    public String getName() {
        return this.name;
    }

    public String getDescription() {
        return description;
    }

    public boolean isRequired() {
        return isRequired;
    }

    @Override
    public String toString() {
        return getName();
    }
}